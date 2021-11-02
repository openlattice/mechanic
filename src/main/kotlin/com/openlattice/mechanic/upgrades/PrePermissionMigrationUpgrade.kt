package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.hazelcast.query.Predicates
import com.openlattice.ApiHelpers
import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.Ace
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Action
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Permission
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.organizations.mapstores.TABLE_ID_INDEX
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.external.Schemas
import com.openlattice.postgres.mapstores.EntitySetMapstore
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.Statement
import java.util.EnumSet
import java.util.UUID
import java.util.concurrent.TimeUnit

class PrePermissionMigrationUpgrade(
    toolbox: Toolbox,
    private val exDbPermMan: ExternalDatabasePermissioningService,
    private val exConnMan: ExternalDatabaseConnectionManager,
    private val dbCreds: DbCredentialService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(PrePermissionMigrationUpgrade::class.java)

    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(toolbox.hazelcast)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(toolbox.hazelcast)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val permissions = HazelcastMap.PERMISSIONS.getMap(toolbox.hazelcast)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)
    private val externalRoleNames = HazelcastMap.EXTERNAL_PERMISSION_ROLES.getMap(toolbox.hazelcast)

    private val olToPostgres = mapOf<Permission, Set<PostgresPrivileges>>(
        Permission.READ to EnumSet.of(PostgresPrivileges.SELECT),
        Permission.WRITE to EnumSet.of(PostgresPrivileges.INSERT, PostgresPrivileges.UPDATE),
        Permission.OWNER to EnumSet.of(PostgresPrivileges.ALL)
    )
    private val READ_WRITE_OWNER_PERMISSIONS = setOf(Permission.READ, Permission.WRITE, Permission.OWNER)

    override fun upgrade(): Boolean {

        logger.info("starting pre-migration")

        try {
            val timer = Stopwatch.createStarted()
            val targetOrgId: UUID? = null

            val filtered = permissions.entrySet(
                Predicates.`in`(
                    PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX,
                    SecurableObjectType.PropertyTypeInEntitySet,
                    SecurableObjectType.OrganizationExternalDatabaseColumn
                )
            ).filter { filterPermissions(it) }
            logger.info("filtering permissions took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.reset().start()

            val grouped = filtered.groupBy({ it.key.aclKey }, { (aceKey, aceVal) ->
                Ace(aceKey.principal, aceVal.permissions, aceVal.expirationDate)
            })
            logger.info("grouping by acl key took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.reset().start()

            val aclsByOrg = grouped
                .map { (aclKey, aces) -> Acl(aclKey, aces) }
                .groupBy { externalTables[it.aclKey[0]]?.organizationId }
                .toList()
                .sortedBy { it.second.size }
                .toMap()
            logger.info("grouping by org took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.stop()

            val targetOrgIds = mutableListOf<UUID?>()
            aclsByOrg.forEach {
                logger.info("org ${it.key} acls ${it.value.size}")
                targetOrgIds.add(it.key)
            }

            timer.reset().start()
            logger.info("starting part 1")
            revokeFromOldPermissionRoles(targetOrgIds, aclsByOrg)
            logger.info("part 1 took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.reset().start()

            logger.info("starting part 2")
            dropOldPermissionRoles()
            logger.info("part 2 took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
        } catch (e: Exception) {
            logger.error("something went wrong with the migration", e)
            return false
        }

        return true
    }

    private fun filterPermissions(entry: MutableMap.MutableEntry<AceKey, AceValue>): Boolean {
        try {
            val columnId = entry.key.aclKey[1]
            if (
                entry.key.aclKey.size > 2
                || (!propertyTypes.containsKey(columnId) && !externalColumns.containsKey(columnId))
            ) {
                logger.warn("ignoring permission entry {}", entry)
                return false
            }
            return when (entry.value.securableObjectType) {
                SecurableObjectType.PropertyTypeInEntitySet -> {
                    val entitySetId = entry.key.aclKey[0]
                    val entitySetFlags = entitySets[entitySetId]?.flags ?: setOf()
                    if (entitySetFlags.contains(EntitySetFlag.TRANSPORTED)) {
                        true
                    }
                    else {
                        logger.warn("ignoring permission entry {}", entry)
                        false
                    }
                }
                SecurableObjectType.OrganizationExternalDatabaseColumn -> {
                    true
                }
                else -> {
                    logger.warn("ignoring permission entry {}", entry)
                    false
                }
            }
        }
        catch (e: Exception) {
            logger.error("something went wrong filtering permission entry {}", entry)
            return false
        }
    }

    private fun revokeFromOldPermissionRoles(targetOrgIds: List<UUID?>, aclsByOrg: Map<UUID?, List<Acl>>) {
        targetOrgIds.forEachIndexed { index, orgId ->
            try {
                logger.info("================================")
                logger.info("================================")
                logger.info("starting to process org $orgId")
                if (aclsByOrg.containsKey(orgId)) {

                    val orgTimer = Stopwatch.createStarted()
                    val acls = aclsByOrg.getValue(orgId)
                    logger.info("org acls {}", acls.size)

                    logger.info("revoking permissions - org $orgId")
                    acls.chunked(128).forEach { aclChunk ->
                        val keysInChunk = aclChunk.map { it.aclKey }
                        logger.info("processing chunk {}", keysInChunk)
                        exDbPermMan.executePrivilegesUpdate(Action.DROP, aclChunk)
                    }
                    logger.info(
                        "revoking permissions took {} ms - org $orgId acls {}",
                        orgTimer.elapsed(TimeUnit.MILLISECONDS),
                        acls.size
                    )
                }
                else {
                    logger.warn("aclsByOrg does not contain org $orgId")
                }
            } catch (e: Exception) {
                logger.error("something went wrong processing org $orgId", e)
            } finally {
                logger.info("progress ${index + 1}/${targetOrgIds.size}")
                logger.info("================================")
                logger.info("================================")
            }
        }
    }

    private fun dropOldPermissionRoles() {
        organizations.values.forEachIndexed { index, it ->
            try {
                logger.info("================================")
                logger.info("================================")
                logger.info("starting to process org ${it.id}")

                val orgTimer = Stopwatch.createStarted()

                // Drop old permission roles
                val admin = dbCreds.getDbUsername(it.adminRoleAclKey)

                logger.info("dropping column/propertyType roles for org {}, with admin {}", it.id, admin)
                exConnMan.connectToOrg(it.id).use { hds ->
                    hds.connection.use { conn ->
                        conn.autoCommit = false
                        conn.createStatement().use { stmt ->
                            val timer = Stopwatch.createStarted()
                            // drop old column permission roles
                            columnsFilterAndProcess(conn, stmt, it.id, admin)
                            logger.info("dropping columns took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
                            timer.reset().start()

                            // drop old property type permission roles
                            propertyTypesFilterAndProcess(conn, stmt, it.id, admin)
                            logger.info("dropping property types took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
                        }
                    }
                }
                logger.info("processing org took {} ms - org ${it.id}", orgTimer.elapsed(TimeUnit.MILLISECONDS))
            }
            catch (e: Exception) {
                logger.error("something went wrong dropping roles for org {}", it.id, e)
            } finally {
                logger.info("progress ${index + 1}/${organizations.size}")
                logger.info("================================")
                logger.info("================================")
            }
        }
    }

    private fun columnsFilterAndProcess(conn: Connection, stmt: Statement, orgId: UUID, admin: String) {
        externalTables.entrySet(
            Predicates.equal<UUID, ExternalTable>(ORGANIZATION_ID_INDEX, orgId)
        ).forEach { table ->
            try {
                val tableName = table.value.name
                val schemaName = table.value.schema
                externalColumns.entrySet(
                    Predicates.equal<UUID, ExternalColumn>(TABLE_ID_INDEX, table.key)
                ).forEach { column ->
                    try {
                        val columnName = column.value.name
                        val aclKey = AclKey(table.key, column.key)
                        logger.info("org {}: dropping column {} of table {} with acl_key {}", orgId, column.key, table.key, aclKey)
                        reassignRevokeDrop(
                            conn,
                            stmt,
                            orgId,
                            admin,
                            listOf(columnName),
                            tableName,
                            schemaName,
                            aclKey
                        )
                    }
                    catch (e: Exception) {
                        logger.error("something went wrong - org {} table {} column {}", orgId, table.key, column.key, e)
                    }
                }
            }
            catch (e: Exception) {
                logger.error("something went wrong - org {} table {}", orgId, table.key, e)
            }
        }
    }

    private fun propertyTypesFilterAndProcess(conn: Connection, stmt: Statement, orgId: UUID, admin: String) {
        entitySets.entrySet(
            Predicates.and(
                Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ORGANIZATION_INDEX, orgId),
                Predicates.`in`<UUID, EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.TRANSPORTED)
            )
        ).forEach { es ->
            try {
                val esName = es.value.name
                propertyTypes.entrySet(
                    Predicates.alwaysTrue()
                ).forEach { pt ->
                    try {
                        val ptName = pt.value.type.toString()
                        val aclKey = AclKey(es.key, pt.key)

                        logger.info("org {}: dropping property type {} of entity set {} with acl_key {}", orgId, pt.key, es.key, aclKey)
                        reassignRevokeDrop(
                            conn,
                            stmt,
                            orgId,
                            admin,
                            listOf(ptName, EdmConstants.ID_FQN.toString()),
                            esName,
                            Schemas.ASSEMBLED_ENTITY_SETS.toString(),
                            aclKey
                        )
                    }
                    catch (e: Exception) {
                        logger.error("something went wrong - org {} entity set {} property {}", orgId, es.key, pt.key, e)
                    }
                }
            }
            catch (e: Exception) {
                logger.error("something went wrong - org {} entity set {}", orgId, es.key, e)
            }
        }
    }

    private fun reassignRevokeDrop(
            conn: Connection,
            stmt: Statement,
            orgId: UUID,
            admin: String,
            columnNameList: List<String>,
            tableName: String,
            schemaName: String,
            aclKey: AclKey
    ) {
        READ_WRITE_OWNER_PERMISSIONS.mapNotNull { permission ->
            externalRoleNames[AccessTarget(aclKey, permission)]?.let {
                permission to it.second
            }
        }.forEach { (permission, roleId) ->
            val role = roleId.toString()
            val sqls = mutableListOf<String>()
            val quotedColumns = columnNameList.joinToString {
                    ApiHelpers.dbQuote(it)
            }

            // first reassign objects to admin
            sqls.add("""
                REASSIGN OWNED BY ${ApiHelpers.dbQuote(role)} TO ${ApiHelpers.dbQuote(admin)}
            """.trimIndent())

            // then revoke all column privileges granted to role
            val privilegeString = olToPostgres.getValue(permission).joinToString { privilege ->
                "$privilege ( ${quotedColumns} )"
            }
            sqls.add("""
                REVOKE $privilegeString 
                ON $schemaName.${ApiHelpers.dbQuote(tableName)}
                FROM ${ApiHelpers.dbQuote(role)}
            """.trimIndent())
            sqls.add("""
                REVOKE USAGE ON SCHEMA $schemaName FROM ${ApiHelpers.dbQuote(role)}
            """.trimIndent())

            // finally drop the role
            sqls.add("""
                DROP ROLE ${ApiHelpers.dbQuote(role)}
            """.trimIndent())

            logger.info(
                "executing sql sequence - org {} table {} column {} role {}",
                orgId,
                tableName,
                columnNameList,
                role
            )
            try {
                sqls.forEach { sql ->
                    stmt.addBatch(sql)
                }

                stmt.executeBatch()
                conn.commit()
            } catch (e: Exception) {
                logger.error(
                    "error dropping role - org {} table {} column {} role {}",
                    orgId,
                    tableName,
                    columnNameList,
                    role,
                    e
                )
                conn.rollback()
            }
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
