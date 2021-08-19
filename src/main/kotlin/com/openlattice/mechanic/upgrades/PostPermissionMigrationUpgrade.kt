package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.AccessTarget
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.ExternalColumn
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.SQLException

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class PostPermissionMigrationUpgrade(
        toolbox: Toolbox,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val dbCreds: DbCredentialService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(SyncOrgPermissionsUpgrade::class.java)

    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(toolbox.hazelcast)
    private val externalRoleNames = HazelcastMap.EXTERNAL_PERMISSION_ROLES.getMap(toolbox.hazelcast)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

    private val allTablePermissions = setOf(Permission.READ, Permission.WRITE, Permission.OWNER)

    override fun upgrade(): Boolean {
        // Drop old permission roles
        externalColumns.groupBy {
            it.value.organizationId
        }.forEach { (orgID, orgColumns) ->
            val org = organizations[orgID]
            // should be of the form "${org.securablePrincipal.id}|${org.securablePrincipal.name} - ADMIN"
            val admin = dbCreds.getDbUsername(org!!.adminRoleAclKey)

            logger.info("dropping column roles for org {}, with admin {}", orgID, admin)
            exConnMan.connectToOrg(orgID).use { hds ->
                hds.connection.use { conn ->
                    conn.autoCommit = false
                    conn.createStatement().use { stmt ->
                        orgColumns.forEach { col ->
                            val colId = col.key
                            val tableId = col.value.tableId
                            val aclKey = AclKey(tableId, colId)

                            logger.info("org {}: dropping column {} of table {} with acl_key {}", orgID, colId, tableId, aclKey)
                            allTablePermissions.mapNotNull { permission ->
                                externalRoleNames[AccessTarget(aclKey, permission)]
                            }.forEach { roleUUID ->
                                val roleName = roleUUID.toString()

                                logger.info("org {}, aclkey {}: dropping {}", orgID, aclKey, roleName)
                                try {
                                    // first reassign objects to admin
                                    stmt.addBatch("""
                                        REASSIGN OWNED BY $roleName TO $admin
                                    """.trimIndent())
                                    // then drop everything owned by rolename (needed or DROP ROLE will not go through)
                                    stmt.addBatch("""
                                        DROP OWNED BY $roleName
                                    """.trimIndent())
                                    // finally drop the role
                                    stmt.addBatch("""
                                        DROP ROLE $roleName
                                    """.trimIndent())

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