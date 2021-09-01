package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.ApiHelpers
import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.SQLException
import java.util.EnumSet
import java.util.UUID
import java.util.regex.Pattern

class PostPermissionMigrationUpgrade(
        toolbox: Toolbox,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val dbCreds: DbCredentialService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(PostPermissionMigrationUpgrade::class.java)

    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(toolbox.hazelcast)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val externalRoleNames = HazelcastMap.EXTERNAL_PERMISSION_ROLES.getMap(toolbox.hazelcast)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

    private val olToPostgres = mapOf<Permission, Set<PostgresPrivileges>>(
        Permission.READ to EnumSet.of(PostgresPrivileges.SELECT),
        Permission.WRITE to EnumSet.of(PostgresPrivileges.INSERT, PostgresPrivileges.UPDATE),
        Permission.OWNER to EnumSet.of(PostgresPrivileges.ALL)
    )
    private val allTablePermissions = setOf(Permission.READ, Permission.WRITE, Permission.OWNER)

    override fun upgrade(): Boolean {
        val uuidRegex= Pattern.compile(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            Pattern.CASE_INSENSITIVE
        )

        // filtering for a specific org
        println("Organization to filter: ")
        val input = readLine()
        val filterFlag = uuidRegex.matcher(input).matches()
        val filteringPredicate = if (filterFlag) {
            Predicates.equal<UUID, ExternalColumn>(ORGANIZATION_ID_INDEX, UUID.fromString(input!!))
        } else {
            Predicates.alwaysTrue()
        }

        if (filterFlag) {
            logger.info("Filtering for org {}", input)
        } else {
            logger.info("No filtering")
        }

        val adminUsernameRegex = Pattern.compile(
            "ol-internal\\|role\\|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            Pattern.CASE_INSENSITIVE
        )

        // Drop old permission roles
        externalColumns.entrySet(
            filteringPredicate
        ).groupBy {
            it.value.organizationId
        }.forEach { (orgID, orgColumns) ->
            logger.info("Searching for admin of org {}", orgID)
            val org = organizations[orgID]

            val admin = dbCreds.getDbUsername(org!!.adminRoleAclKey)
            if (!adminUsernameRegex.matcher(admin).matches()) {
                logger.warn("Unexpected formatting of admin for org {}", orgID)
                logger.warn("Admin found in db_creds: {}", admin)
                logger.warn("Skipping org {}")
                return@forEach
            }

            logger.info("dropping column roles for org {}, with admin {}", orgID, admin)
            exConnMan.connectToOrg(orgID).use { hds ->
                hds.connection.use { conn ->
                    conn.autoCommit = false
                    conn.createStatement().use { stmt ->
                        orgColumns.forEach { col ->
                            val colId = col.key
                            val colName = col.value.name
                            val tableId = col.value.tableId
                            val tableName = externalTables.getValue(tableId)?.name ?: String()
                            val schemaName = externalTables.getValue(tableId)?.schema ?: String()
                            val aclKey = AclKey(tableId, colId)

                            logger.info("org {}: dropping column {} of table {} with acl_key {}", orgID, colId, tableId, aclKey)
                            allTablePermissions.mapNotNull { permission ->
                                externalRoleNames[AccessTarget(aclKey, permission)]?.let { 
                                    permission to it.second
                                }
                            }.forEach { (permission, roleUUID) ->
                                val roleName = roleUUID.toString()
                                val sqls = mutableListOf<String>()

                                // first reassign objects to admin
                                sqls.add("""
                                    REASSIGN OWNED BY $roleName TO $admin
                                """.trimIndent())

                                // then revoke all column privileges granted to role
                                val privilegeString = olToPostgres.getValue(permission).joinToString { privilege ->
                                    "$privilege ( ${ApiHelpers.dbQuote(colName)} )"
                                }
                                sqls.add("""
                                    REVOKE $privilegeString 
                                    ON $schemaName.${ApiHelpers.dbQuote(tableName)}
                                    FROM $roleName
                                """.trimIndent())
                                sqls.add("""
                                    REVOKE USAGE ON SCHEMA $schemaName FROM $roleName
                                """.trimIndent())

                                // finally drop the role
                                sqls.add("""
                                    DROP ROLE $roleName
                                """.trimIndent())

                                logger.info("org {}, aclkey {}: dropping {}", orgID, aclKey, roleName)
                                try {
                                    sqls.forEach { sql ->
                                        stmt.addBatch(sql)
                                    }

                                    stmt.executeBatch()
                                    conn.commit()
                                }
                                catch (ex: SQLException) {
                                    logger.error("SQL error occurred while dropping role {} (column {}, table {}, acl_key {}) of org {}", roleName, colId, tableId, aclKey, orgID, ex)
                                    conn.rollback()
                                    return false
                                }
                            }
                        }
                    }
                }
            }
        }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}