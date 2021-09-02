package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.hazelcast.query.Predicates
import com.openlattice.ApiHelpers
import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.ExternalColumn
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit
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
    private val READ_WRITE_OWNER_PERMISSIONS = setOf(Permission.READ, Permission.WRITE, Permission.OWNER)

    private val ADMIN_USERNAME_REGEX = Pattern.compile(
        "ol-internal\\|role\\|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        Pattern.CASE_INSENSITIVE
    )

    override fun upgrade(): Boolean {

        logger.info("starting migration")

        try {
            val targetOrgId: UUID? = null

            val filteringPredicate = if (targetOrgId != null) {
                Predicates.equal<UUID, ExternalColumn>(ORGANIZATION_ID_INDEX, targetOrgId)
            } else {
                Predicates.alwaysTrue()
            }

            // Drop old permission roles
            externalColumns.entrySet(
                filteringPredicate
            ).groupBy {
                it.value.organizationId
            }.forEach { (orgId, orgColumns) ->

                logger.info("================================")
                logger.info("================================")
                logger.info("starting to process org $orgId")

                val timer = Stopwatch.createStarted()

                val admin = getOrgAdminUsername(orgId)
                if (admin == null) {
                    logger.warn("skipping org {}", orgId)
                    return@forEach
                }

                exConnMan.connectToOrg(orgId).use { hds ->
                    hds.connection.use { conn ->
                        conn.autoCommit = false
                        conn.createStatement().use { stmt ->
                            orgColumns.forEachIndexed { index, col ->
                                val colId = col.key
                                val colName = col.value.name
                                val tableId = col.value.tableId
                                val tableName = externalTables.getValue(tableId)?.name ?: String()
                                val schemaName = externalTables.getValue(tableId)?.schema ?: String()
                                val aclKey = AclKey(tableId, colId)

                                READ_WRITE_OWNER_PERMISSIONS.mapNotNull { permission ->
                                    externalRoleNames[AccessTarget(aclKey, permission)]?.let {
                                        permission to it
                                    }
                                }.forEach { (permission, roleId) ->
                                    val role = roleId.toString()
                                    val sqls = mutableListOf<String>()
                                    try {
                                        // first reassign objects to admin
                                        sqls.add("""
                                            REASSIGN OWNED BY $role TO $admin
                                        """.trimIndent())

                                        // then revoke all column privileges granted to role
                                        val privilegeString = olToPostgres
                                            .getValue(permission)
                                            .joinToString { privilege ->
                                                "$privilege ( ${ApiHelpers.dbQuote(colName)} )"
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
                                            tableId,
                                            colId,
                                            role
                                        )

                                        sqls.forEach { sql -> stmt.addBatch(sql) }
                                        stmt.executeBatch()
                                        conn.commit()
                                    }
                                    catch (e: Exception) {
                                        logger.error(
                                            "error dropping role - org {} table {} column {} role {}",
                                            orgId,
                                            tableId,
                                            colId,
                                            role,
                                            e
                                        )
                                        conn.rollback()
                                        return false
                                    }
                                }
                                logger.info("progress ${index + 1}/${orgColumns.size}")
                            }
                        }
                    }
                }

                logger.info(
                    "processing org took {} ms - org $orgId",
                    timer.elapsed(TimeUnit.MILLISECONDS),
                )
                logger.info("================================")
                logger.info("================================")
            }
        } catch (e: Exception) {
            logger.error("something went wrong with the migration", e)
            return false
        }

        return true
    }

    private fun getOrgAdminUsername(orgId: UUID) :String? {

        val org = organizations[orgId]
        if (org == null) {
            logger.error("org {} does not exist in mapstore", orgId)
            return null
        }

        val admin = dbCreds.getDbUsername(org.adminRoleAclKey)
        if (!ADMIN_USERNAME_REGEX.matcher(admin).matches()) {
            logger.error("invalid org admin username - org {} username {}", orgId, admin)
            return null
        }

        return admin
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
