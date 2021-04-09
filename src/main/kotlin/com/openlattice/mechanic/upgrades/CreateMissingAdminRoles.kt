package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.Organization
import com.openlattice.organizations.roles.SecurePrincipalsManager
import java.util.*

class CreateMissingAdminRoles(
        private val toolbox: Toolbox,
        private val principalsManager: SecurePrincipalsManager,
        private val authorizationManager: AuthorizationManager
) : Upgrade {

    override fun upgrade(): Boolean {
        val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).values.toList()
        val principalAclKeys = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).keys.toSet()

        organizations.filter { !principalAclKeys.contains(it.adminRoleAclKey) }.forEach {
            createAdminRoleForOrganization(it)
        }

        return true
    }

    private fun createAdminRoleForOrganization(organization: Organization): Role {
        //Create the admin role for the organization and give it ownership of organization.
        val adminRoleAclKey = organization.adminRoleAclKey
        val adminRole = createOrganizationAdminRole(organization.securablePrincipal, adminRoleAclKey)

        val userOwnerPrincipals = authorizationManager.getOwnersForSecurableObjects(listOf(organization.getAclKey()))
                .get(organization.getAclKey())
                .filter { it.type == PrincipalType.USER }

        val userOwnerAclKeys = userOwnerPrincipals.map { principalsManager.lookup(it) }.toSet()

        principalsManager.createSecurablePrincipalIfNotExists(userOwnerPrincipals.first(), adminRole)
        principalsManager.addPrincipalToPrincipals(adminRoleAclKey, userOwnerAclKeys)

        authorizationManager.addPermission(adminRoleAclKey, organization.principal, EnumSet.of(Permission.READ))
        authorizationManager.addPermission(organization.getAclKey(), adminRole.principal, EnumSet.allOf(Permission::class.java))

        return adminRole
    }

    private fun createOrganizationAdminRole(organization: SecurablePrincipal, adminRoleAclKey: AclKey): Role {
        val roleId = adminRoleAclKey[1]
        val principalTitle = constructOrganizationAdminRolePrincipalTitle(organization)
        val principalId = constructOrganizationAdminRolePrincipalId(organization)
        val rolePrincipal = Principal(PrincipalType.ROLE, principalId)
        return Role(
                Optional.of(roleId),
                organization.id,
                rolePrincipal,
                principalTitle,
                Optional.of("Administrators of this organization")
        )
    }

    private fun constructOrganizationAdminRolePrincipalTitle(organization: SecurablePrincipal): String {
        return organization.name + " - ADMIN"
    }

    private fun constructOrganizationAdminRolePrincipalId(organization: SecurablePrincipal): String {
        return organization.id.toString() + "|" + constructOrganizationAdminRolePrincipalTitle(organization)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_03_10.value
    }
}