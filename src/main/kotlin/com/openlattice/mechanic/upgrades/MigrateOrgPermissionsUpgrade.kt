package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.authorization.Ace
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.Acl
import com.openlattice.authorization.Action
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.SQLException

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class MigrateOrgPermissionsUpgrade(
        toolbox: Toolbox,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val exDbPermMan: ExternalDatabasePermissioningService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(SyncOrgPermissionsUpgrade::class.java)

    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)
    private val permissions = HazelcastMap.PERMISSIONS.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {
        // pre-migration population of external_permission_roles table
        try {
            logger.info("Populating external_permission_roles")
            populateExternalPermissionRolesTable()
        }
        catch (e: Exception) {
            logger.error("Error occurred while populating external_permission_roles", e)
            // don't continue
            return false
        }

        val filteredPermissionEntries = permissions.entrySet(
                Predicates.`in`<AceKey, AceValue>(PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX,
                        SecurableObjectType.PropertyTypeInEntitySet,
                        SecurableObjectType.OrganizationExternalDatabaseColumn
                )
        )
        val acls = filteredPermissionEntries.groupBy({
            it.key.aclKey
        }, { (aceKey, aceVal) ->
            val prin = aceKey.principal
            val perms = aceVal.permissions
            val exp = aceVal.expirationDate
            Ace(prin, perms, exp)
        }).map { (aclkey, aces) ->
            Acl(aclkey, aces)
        }

        // actual permission migration
        val dropOldPerms = dropOldPermissionRoles(acls)
        val assignPerms = assignAllPermissions(acls)
        if (dropOldPerms && assignPerms) {
            return true
        }
        logger.error("Permissions migration upgrade failed, final status:\n" +
                "dropOldRolePermissions: {}\n" +
                "assignAllPermissions: {}\n", dropOldPerms, assignPerms)
        return false
    }

    private fun populateExternalPermissionRolesTable() {
        organizations.values.forEach { org ->
            val orgID = org.id
            try {
                logger.info("populating external_permission_roles for org {}", orgID)
                exConnMan.connectToOrg(orgID).use { hds ->
                    hds.connection.use { conn ->
                        conn.createStatement().use { stmt ->
                            stmt.addBatch("""
                                UPDATE external_permission_roles epr
                                SET epr.column_name = oedc.name
                                FROM organization_external_database_columns oedc
                                WHERE epr.aclkey = ARRAY[oedc.table_id, oedc.id]
                            """.trimIndent())
                            stmt.executeBatch()
                        }
                    }
                }
            }
            catch (ex: SQLException) {
                logger.error("SQL error occurred while populating external_permission_roles for org {}", orgID, ex)
                throw ex
            }
        }
    }

    private fun dropOldPermissionRoles(acls: List<Acl>): Boolean {
        logger.info("Revoking membership from old perms roles")
        exDbPermMan.executePrivilegesUpdate(Action.DROP, acls)
        return true
    }

    private fun assignAllPermissions(acls: List<Acl>): Boolean {
        logger.info("Granting direct permissions")
        exDbPermMan.executePrivilegesUpdate(Action.SET, acls)
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}