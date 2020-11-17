package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.Ace
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.slf4j.LoggerFactory
import java.util.*

class CleanOutOrgMembersAndRoles(
        private val toolbox: Toolbox,
        private val principalsManager: SecurePrincipalsManager,
        private val authorizationManager: AuthorizationManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CleanOutOrgMembersAndRoles::class.java)
        private val MEMBER_PERMISSIONS = EnumSet.of(Permission.READ)
    }

    override fun upgrade(): Boolean {
        val organizationsMapstore = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

        val organizations = organizationsMapstore.toMap()
        val principals = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).values.associate { it.principal to it.aclKey }

        organizations.values.forEach {
            logger.info("About to clean out members for org {} [{}]", it.title, it.id)

            val memberAclKeys = it.members.mapNotNull { p -> principals[p] }.toSet()
            principalsManager.addPrincipalToPrincipals(it.getAclKey(), memberAclKeys)
            authorizationManager.addPermissions(listOf(
                    Acl(
                            it.getAclKey(),
                            it.members.map { p -> Ace(p, MEMBER_PERMISSIONS) }
                    )
            ))

            organizationsMapstore.executeOnKey(it.id) { entry ->
                val org = entry.value
                org.members.clear()
                org.roles.clear()
                entry.setValue(org)
            }

            logger.info("Finished cleaning out {} members for org {} [{}]", memberAclKeys.size, it.title, it.id)
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }
}