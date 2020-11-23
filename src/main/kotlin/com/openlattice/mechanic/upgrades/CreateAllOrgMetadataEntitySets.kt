package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.Organization
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.slf4j.LoggerFactory
import java.util.*

class CreateAllOrgMetadataEntitySets(
        private val toolbox: Toolbox,
        private val metadataEntitySetsService: OrganizationMetadataEntitySetsService,
        private val principalsManager: SecurePrincipalsManager,
        private val authorizationManager: AuthorizationManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CreateAllOrgMetadataEntitySets::class.java)
    }

    override fun upgrade(): Boolean {
        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()

        val adminRoles = getAdminRolesByOrgId(orgs)

        orgs.values.forEach {
            logger.info("About to initialize metadata entity sets for organization ${it.title} [${it.id}]")
            metadataEntitySetsService.initializeOrganizationMetadataEntitySets(adminRoles.getValue(it.id))
            logger.info("Finished initializing metadata entity sets for organization ${it.title} [${it.id}]")
        }

        return true
    }

    private fun getAdminRolesByOrgId(organizations: Map<UUID, Organization>): Map<UUID, Role> {
        val rolesByAclKey = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).values(
                Predicates.equal(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.ROLE)
        ).associateBy { it.aclKey }

        val orgPrincipalNames = organizations.values.map {
            "${it.securablePrincipal.id}|${it.securablePrincipal.name} - ADMIN"
        }.toSet()

        val adminRoles = mutableMapOf<UUID, Role>()
        rolesByAclKey.values.forEach {
            if (orgPrincipalNames.contains(it.principal.id)) {
                adminRoles[it.aclKey[0]] = it as Role
            }
        }

        return organizations.values.associate {
            val aclKey = adminRoles[it.id] ?: createAdminRoleForOrganization(it)
            it.id to aclKey
        }
    }

    private fun createAdminRoleForOrganization(organization: Organization): Role {
        //Create the admin role for the organization and give it ownership of organization.
        val adminRoleAclKey = AclKey(organization.id, UUID.randomUUID())
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
        return Version.V2020_10_14.value
    }


}