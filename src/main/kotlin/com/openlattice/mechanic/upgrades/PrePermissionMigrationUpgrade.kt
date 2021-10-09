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
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organizations.Organization
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

            val filteredPermissions = permissions.entrySet(
                Predicates.`in`(
                    PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX,
                    SecurableObjectType.PropertyTypeInEntitySet,
                    SecurableObjectType.OrganizationExternalDatabaseColumn
                )
            ).filter { entry ->
                entry.key.aclKey.size <= 2
            }.filter { entry ->
                val columnId = entry.key.aclKey[1]
                if (!propertyTypes.containsKey(columnId) && !externalColumns.containsKey(columnId)) {
                    logger.warn("ignoring permission entry because neither properties nor columns contain the property/column id {}", entry)
                    return@filter false
                }
                return@filter true
            }.filter { entry ->
                orgIdPredicate(entry, targetOrgId)
            }

            val acls = filteredPermissions
                .groupBy({ it.key.aclKey }, { (aceKey, aceVal) ->
                    Ace(aceKey.principal, aceVal.permissions, aceVal.expirationDate)
                })
                .map { (aclKey, aces) -> Acl(aclKey, aces) }

            logger.info("target acls {} {}", acls.size, acls)

            // revoke membership from column permission roles
            val revokeSuccess = revokeFromOldPermissionRoles(acls)
            logger.info(
                "revoking role membership took {} ms - {}",
                timer.elapsed(TimeUnit.MILLISECONDS),
                acls
            )

            // reassign & revoke all objs owned by column permission roles, then drop
            val dropSuccess = dropOldPermissionRoles(targetOrgId)
            logger.info(
                "dropping permission roles took {} ms",
                timer.elapsed(TimeUnit.MILLISECONDS)
            )

            if (revokeSuccess && dropSuccess) {
                return true
            }

            logger.error("pre-migration failed - revoke {} drop {}", revokeSuccess, dropSuccess)
            return false
        } catch (e: Exception) {
            logger.error("something went wrong with the pre-migration", e)
        }

        return true
    }

    private fun orgIdPredicate(entry: MutableMap.MutableEntry<AceKey, AceValue>, orgId: UUID?): Boolean {
        try {
            val tableId = entry.key.aclKey[0]
            val securableObjectType = entry.value.securableObjectType
            return when (securableObjectType) {
                SecurableObjectType.OrganizationExternalDatabaseColumn -> {
                    orgId == null || externalTables[tableId]!!.organizationId == orgId
                }
                SecurableObjectType.PropertyTypeInEntitySet -> {
                    (orgId == null || entitySets[tableId]!!.organizationId == orgId) && entitySets[tableId]!!.flags.contains(EntitySetFlag.TRANSPORTED)
                }
                else -> {
                    logger.error("SecurableObjectType {} is unexpected, filtering out {}", securableObjectType, entry)
                    false
                }
            }
        }
        catch (e: Exception) {
            logger.error("something went wrong filtering permissions for {}", entry, e)
            return false
        }
    }

    private fun revokeFromOldPermissionRoles(acls: List<Acl>): Boolean {
        logger.info("Revoking membership from old perms roles")
        exDbPermMan.executePrivilegesUpdate(Action.DROP, acls)
        return true
    }

    private fun dropOldPermissionRoles(orgId : UUID?): Boolean {
        try {
            logger.info("Dropping old perms roles")

            val filteringPredicate = if (orgId != null) {
                Predicates.equal<UUID, Organization>("__key", orgId)
            } else {
                Predicates.alwaysTrue()
            }

            organizations.entrySet(
                filteringPredicate
            ).forEach {
                logger.info("================================")
                logger.info("================================")
                logger.info("starting to process org ${it.key}")

                val timer = Stopwatch.createStarted()

                // Drop old permission roles
                val admin = dbCreds.getDbUsername(it.value.adminRoleAclKey)

                logger.info("dropping column/propertyType roles for org {}, with admin {}", it.key, admin)
                exConnMan.connectToOrg(it.key).use { hds ->
                    hds.connection.use { conn ->
                        conn.autoCommit = false
                        conn.createStatement().use { stmt ->
                            // drop old column permission roles
                            columnsFilterAndProcess(conn, stmt, it.key, admin)
                            logger.info(
                                "dropping columns took {} ms",
                                timer.elapsed(TimeUnit.MILLISECONDS)
                            )

                            // drop old property type permission roles
                            propertyTypesFilterAndProcess(conn, stmt, it.key, admin)
                            logger.info(
                                "dropping property types took {} ms",
                                timer.elapsed(TimeUnit.MILLISECONDS)
                            )
                        }
                    }
                }

                logger.info(
                    "processing org took {} ms - org ${it.key}",
                    timer.elapsed(TimeUnit.MILLISECONDS),
                )
                logger.info("================================")
                logger.info("================================")
            }
        } catch (e: Exception) {
            logger.error("something went wrong with the role dropping", e)
        }

        return true
    }

    private fun columnsFilterAndProcess(conn: Connection, stmt: Statement, orgId: UUID, admin: String) {
        externalTables.entrySet(
            Predicates.equal<UUID, ExternalTable>(ORGANIZATION_ID_INDEX, orgId)
        ).forEach { table ->
            val tableName = table.value.name
            val schemaName = table.value.schema
            externalColumns.entrySet(
                Predicates.equal<UUID, ExternalColumn>(TABLE_ID_INDEX, table.key)
            ).forEach { column ->
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
        }
    }

    private fun propertyTypesFilterAndProcess(conn: Connection, stmt: Statement, orgId: UUID, admin: String) {
        entitySets.entrySet(
            Predicates.and(
                Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ORGANIZATION_INDEX, orgId),
                Predicates.`in`<UUID, EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.TRANSPORTED)
            )

        ).forEach { es ->
            val esName = es.value.name
            propertyTypes.entrySet(
                Predicates.alwaysTrue()
            ).forEach { pt ->
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
