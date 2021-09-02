package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.hazelcast.query.Predicates
import com.openlattice.ApiHelpers
import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Permission
import com.openlattice.edm.EntitySet
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
import com.openlattice.postgres.external.Schemas
import com.openlattice.postgres.mapstores.EntitySetMapstore
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.Statement
import java.util.*
import java.util.concurrent.TimeUnit

class PostPermissionMigrationUpgrade(
    toolbox: Toolbox,
    private val exConnMan: ExternalDatabaseConnectionManager,
    private val dbCreds: DbCredentialService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(PostPermissionMigrationUpgrade::class.java)

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(toolbox.hazelcast)
    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(toolbox.hazelcast)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val externalRoleNames = HazelcastMap.EXTERNAL_PERMISSION_ROLES.getMap(toolbox.hazelcast)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

    private val olToPostgres = mapOf<Permission, Set<PostgresPrivileges>>(
        Permission.READ to EnumSet.of(PostgresPrivileges.SELECT),
        Permission.WRITE to EnumSet.of(PostgresPrivileges.INSERT, PostgresPrivileges.UPDATE),
        Permission.OWNER to EnumSet.of(PostgresPrivileges.ALL)
    )
    private val READ_WRITE_OWNER_PERMISSIONS = setOf(Permission.READ, Permission.WRITE, Permission.OWNER)

    override fun upgrade(): Boolean {

        logger.info("starting post-migration")

        try {
            val targetOrgId: UUID? = null

            val filteringPredicate = if (targetOrgId != null) {
                Predicates.equal<UUID, Organization>("__key", targetOrgId)
            } else {
                Predicates.alwaysTrue()
            }

            // filter out org and apply old permission role dropping
            logger.info("Filtering out organizations")
            organizations.entrySet(
                filteringPredicate
            ).forEach {
                if (targetOrgId != null && it.key != targetOrgId) return@forEach

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
            logger.error("something went wrong with the post-migration", e)
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
                reassignRevokeDrop(conn, stmt, orgId, admin, columnName, tableName, schemaName, aclKey)
            }
        }
    }

    private fun propertyTypesFilterAndProcess(conn: Connection, stmt: Statement, orgId: UUID, admin: String) {
        entitySets.entrySet(
            Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ORGANIZATION_INDEX, orgId)
        ).forEach { es ->
            val esName = es.value.name
            propertyTypes.entrySet(
                Predicates.alwaysTrue()
            ).forEach { pt ->
                val ptName = pt.value.type.toString()
                val aclKey = AclKey(es.key, pt.key)

                logger.info("org {}: dropping property type {} of entity set {} with acl_key {}", orgId, pt.key, es.key, aclKey)
                reassignRevokeDrop(conn, stmt, orgId, admin, ptName, esName, Schemas.ASSEMBLED_ENTITY_SETS.toString(), aclKey)
            }
        }
    }

    private fun reassignRevokeDrop(
            conn: Connection, 
            stmt: Statement, 
            orgId: UUID, 
            admin: String, 
            columnName: String, 
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

            // first reassign objects to admin
            sqls.add("""
                REASSIGN OWNED BY $role TO $admin
            """.trimIndent())

            // then revoke all column privileges granted to role
            val privilegeString = olToPostgres.getValue(permission).joinToString { privilege ->
                "$privilege ( ${ApiHelpers.dbQuote(columnName)} )"
            }
            sqls.add("""
                REVOKE $privilegeString 
                ON $schemaName.${ApiHelpers.dbQuote(tableName)}
                FROM $role
            """.trimIndent())
            sqls.add("""
                REVOKE USAGE ON SCHEMA $schemaName FROM $role
            """.trimIndent())

            // finally drop the role
            sqls.add("""
                DROP ROLE $role
            """.trimIndent())

            logger.info(
                "executing sql sequence - org {} table {} column {} role {}",
                orgId,
                tableName,
                columnName,
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
                    columnName,
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