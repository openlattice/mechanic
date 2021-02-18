package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import java.util.*

class GrantReadToOrgOnMetadataEntitySets(
        private val toolbox: Toolbox,
        private val authorizationManager: AuthorizationManager
) : Upgrade {

    companion object {
        private val READ_PERMISSIONS = EnumSet.of(Permission.READ)
    }

    override fun upgrade(): Boolean {
        val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

        val grants = mutableListOf<Acl>()

        organizations.values.forEach { org ->
            val principal = org.principal
            val mesids = org.organizationMetadataEntitySetIds

            grants.addAll(getGrantsForEs(mesids.columns, principal))
            grants.addAll(getGrantsForEs(mesids.datasets, principal))
            grants.addAll(getGrantsForEs(mesids.organization, principal))
        }

        authorizationManager.addPermissions(grants)

        return true
    }

    private fun getGrantsForEs(entitySetId: UUID, principal: Principal): List<Acl> {
        val entitySet = toolbox.entitySets[entitySetId] ?: return listOf()

        val aclKeys = mutableListOf(AclKey(entitySetId))
        toolbox.entityTypes.getValue(entitySet.entityTypeId).properties.forEach {
            aclKeys.add(AclKey(entitySetId, it))
        }
        return aclKeys.map { Acl(it, listOf(Ace(principal, READ_PERMISSIONS))) }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_02_14.value
    }
}