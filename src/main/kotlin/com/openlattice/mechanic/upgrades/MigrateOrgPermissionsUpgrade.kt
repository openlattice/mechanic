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
import java.util.UUID
import java.util.regex.Pattern

class MigrateOrgPermissionsUpgrade(
        toolbox: Toolbox,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val exDbPermMan: ExternalDatabasePermissioningService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(MigrateOrgPermissionsUpgrade::class.java)

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val permissions = HazelcastMap.PERMISSIONS.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {
        val uuidRegex= Pattern.compile(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            Pattern.CASE_INSENSITIVE
        )

        // filtering for a specific org
        println("Organization to filter: ")
        val input = readLine()
        val filterFlag = uuidRegex.matcher(input).matches()
        val filteringOrgID = if (filterFlag) {
            UUID.fromString(input!!)
        } else {
            UUID(0, 0)
        }

        if (filterFlag) {
            logger.info("Filtering for org {}", filteringOrgID)
        } else {
            logger.info("No filtering")
        }

        val filteredPermissionEntries = permissions.entrySet(
                Predicates.`in`<AceKey, AceValue>(PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX,
                        SecurableObjectType.PropertyTypeInEntitySet,
                        SecurableObjectType.OrganizationExternalDatabaseColumn
                )
        ).filter { entry ->
            !filterFlag || orgIdPredicate(entry, filteringOrgID)
        }

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
        val revokeOldPerms = revokeFromOldPermissionRoles(acls)
        val assignPerms = assignAllPermissions(acls)
        if (revokeOldPerms && assignPerms) {
            return true
        }
        logger.error("Permissions migration upgrade failed, final status:\n" +
                "revokeFromOldPermissionRoles: {}\n" +
                "assignAllPermissions: {}\n", revokeOldPerms, assignPerms)
        return false
    }

    private fun revokeFromOldPermissionRoles(acls: List<Acl>): Boolean {
        logger.info("Number of acls to be processed: {}", acls.size)
        logger.info("Revoking membership from old perms roles")
        exDbPermMan.executePrivilegesUpdate(Action.DROP, acls)
        return true
    }

    private fun assignAllPermissions(acls: List<Acl>): Boolean {
        logger.info("Granting direct permissions")
        exDbPermMan.executePrivilegesUpdate(Action.SET, acls)
        return true
    }

    private fun orgIdPredicate(entry: MutableMap.MutableEntry<AceKey, AceValue>, orgId: UUID): Boolean {
        val tableId = entry.key.aclKey[0]
        val securableObjectType = entry.value.securableObjectType
        try {
            when (securableObjectType) {
                SecurableObjectType.OrganizationExternalDatabaseColumn -> {
                    return externalTables[tableId]!!.organizationId == orgId
                }
                SecurableObjectType.PropertyTypeInEntitySet -> {
                    return entitySets[tableId]!!.organizationId == orgId
                }
                else -> {
                    logger.error("SecurableObjectType {} is unexpected, filtering out {}", securableObjectType, entry)
                    return false
                }
            }
        }
        catch (e: Exception) {
            logger.error("something went wrong filtering permissions for {}", entry, e)
            return false
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
