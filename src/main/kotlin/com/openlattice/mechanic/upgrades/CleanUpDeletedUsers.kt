package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import org.slf4j.LoggerFactory

class CleanUpDeletedUsers(private val toolbox: Toolbox): Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CleanUpDeletedUsers::class.java)
    }

    override fun upgrade(): Boolean {
        val userIds = HazelcastMap.USERS.getMap(toolbox.hazelcast).keys.toSet()
        val principals = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast)

        val permissions = HazelcastMap.PERMISSIONS.getMap(toolbox.hazelcast)
        val principalTrees = HazelcastMap.PRINCIPAL_TREES.getMap(toolbox.hazelcast)
        val dbCreds = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast)
        val aclKeys = HazelcastMap.ACL_KEYS.getMap(toolbox.hazelcast)
        val names = HazelcastMap.NAMES.getMap(toolbox.hazelcast)
        val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(toolbox.hazelcast)

        val deletedPrincipals = principals.values(Predicates.equal(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.USER))
                .toList()
                .filter { !userIds.contains(it.principal.id) }

        logger.info("Going to clean up ${deletedPrincipals.size} deleted principals...")

        deletedPrincipals.forEach { sp ->
            logger.info("Cleaning up principal ${sp.title} (${sp.principal.id}) [${sp.id}]")

            permissions.removeAll(Predicates.equal(PermissionMapstore.ACL_KEY_INDEX, sp.aclKey))
            permissions.removeAll(Predicates.equal(PermissionMapstore.PRINCIPAL_INDEX, sp.principal))
            principalTrees.remove(sp.aclKey)
            dbCreds.remove(sp.aclKey)
            principals.remove(sp.aclKey)
            aclKeys.remove(sp.principal.id)
            names.remove(sp.id)
            securableObjectTypes.remove(sp.aclKey)
        }

        logger.info("Finished cleaning up deleted principals")

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }
}