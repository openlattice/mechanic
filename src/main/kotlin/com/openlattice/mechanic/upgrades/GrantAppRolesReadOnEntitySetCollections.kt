package com.openlattice.mechanic.upgrades

import com.google.common.eventbus.EventBus
import com.hazelcast.query.Predicates
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import java.util.*

class GrantAppRolesReadOnEntitySetCollections(
    private val toolbox: Toolbox,
    private val eventBus: EventBus
) : Upgrade {

    override fun upgrade(): Boolean {
//        val authorizations = HazelcastAuthorizationService(toolbox.hazelcast, eventBus)
        val permissionsToAdd = mutableListOf<Acl>()

        val rolePrincipals = getRolePrincipalsByAclKey()
        val adminRoles = getAdminRolesByOrgId(rolePrincipals)

        (HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast) as Map<AppConfigKey, AppTypeSetting>)
            .forEach { (appConfigKey, appTypeSetting) ->
                val orgId = appConfigKey.organizationId
                val aclKey = listOf(appTypeSetting.entitySetCollectionId)

                val aces = mutableListOf<Ace>()

                adminRoles[orgId]?.let {
                    aces.add(Ace(it, EnumSet.allOf(Permission::class.java)))
                }

                appTypeSetting.roles.values.forEach { roleAclKey ->
                    rolePrincipals[roleAclKey]?.let {
                        aces.add(Ace(it.principal, EnumSet.of(Permission.READ)))
                    }
                }

                permissionsToAdd.add(Acl(aclKey, aces))
            }

//        authorizations.addPermissions(permissionsToAdd)
        return true
    }

    private fun getRolePrincipalsByAclKey(): Map<AclKey, SecurablePrincipal> {
        return HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).values(
            Predicates.equal(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.ROLE)
        ).associateBy { it.aclKey }
    }

    private fun getAdminRolesByOrgId(rolesByAclKey: Map<AclKey, SecurablePrincipal>): Map<UUID, Principal> {
        val orgPrincipalNames = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).values.map {
            "${it.securablePrincipal.id}|${it.securablePrincipal.name} - ADMIN"
        }.toSet()

        val adminRoles = mutableMapOf<UUID, Principal>()
        rolesByAclKey.values.forEach {
            if (orgPrincipalNames.contains(it.principal.id)) {
                adminRoles[it.aclKey[0]] = it.principal
            }
        }

        return adminRoles
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_09_15.value
    }
}