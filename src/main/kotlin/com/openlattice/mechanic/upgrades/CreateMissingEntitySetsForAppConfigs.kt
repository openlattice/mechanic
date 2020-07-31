package com.openlattice.mechanic.upgrades

import com.openlattice.apps.App
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppType
import com.openlattice.apps.AppTypeSetting
import com.openlattice.authorization.*
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.Organization
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(CreateMissingEntitySetsForAppConfigs::class.java)

class CreateMissingEntitySetsForAppConfigs(
        val toolbox: Toolbox,
        val spm: SecurePrincipalsManager,
        val authManager: AuthorizationManager,
        val entitySetManager: EntitySetManager,
        val reservations: HazelcastAclKeyReservationService
) : Upgrade {

    override fun getSupportedVersion(): Long {
        return Version.V2020_06_11.value
    }

    override fun upgrade(): Boolean {
        val appConfigsMapstore = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)
        val appConfigs = appConfigsMapstore.toMap()
        val apps = HazelcastMap.APPS.getMap(toolbox.hazelcast).toMap()
        val appTypes = HazelcastMap.APP_TYPES.getMap(toolbox.hazelcast).toMap()
        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()

        val orgsToUserOwners = getUserOwnersOfOrgs(orgs.keys)
        val orgsToAdminRoles = orgs.values.associate {
            it.id to getOrCreateAdminRole(it, orgsToUserOwners.getValue(it.id))
        }

        val newAppConfigEntries = mutableMapOf<AppConfigKey, AppTypeSetting>()
        val aclsToGrant = mutableListOf<Acl>()

        orgs.values.forEach { org ->

            val appConfigKeysToCreate = mutableSetOf<AppConfigKey>()

            val userOwnerPrincipal = orgsToUserOwners.getValue(org.id)
            val adminRole = orgsToAdminRoles.getValue(org.id)
            val rolesByPrincipalId = getOrgRolesByPrincipalId(org.id)
            val adminAceKeys = listOf(Ace(adminRole, EnumSet.allOf(Permission::class.java)))
            val roleAcesByApp = mutableMapOf<UUID, Set<Ace>>()

            org.apps.forEach { appId ->

                val app = apps.getValue(appId)
                val orgAppRoles = getRoleAcesForApp(rolesByPrincipalId, org.id, app, userOwnerPrincipal)
                roleAcesByApp[appId] = orgAppRoles
                getOrCreateAppPrincipal(app, org.id, adminAceKeys.first().principal)
                val appPrincipalAces = listOf(getAppPrincipalAce(appId, org.id))

                app.appTypeIds.forEach { appTypeId ->

                    val appConfigKey = AppConfigKey(appId, org.id, appTypeId)
                    if (!appConfigs.containsKey(appConfigKey)) {
                        appConfigKeysToCreate.add(appConfigKey)
                    } else {
                        val entitySetId = appConfigs.getValue(appConfigKey).entitySetId
                        aclsToGrant.add(Acl(AclKey(entitySetId), appPrincipalAces))
                        toolbox.entityTypes.getValue(appTypes.getValue(appTypeId).entityTypeId).properties.forEach { ptId ->
                            aclsToGrant.add(Acl(AclKey(entitySetId, ptId), appPrincipalAces))
                        }
                    }

                }
            }

            if (userOwnerPrincipal == null) {
                logger.error("ERROR: Unable to create missing entity sets for org ${org.id} because no user owner was found. AppConfigKeys missing: $appConfigKeysToCreate")
                return@forEach
            }

            if (adminRole == null) {
                logger.info("Admin role not present for org ${org.id} so skipping additional permission grants")
            }

            logger.info("About to create ${appConfigKeysToCreate.size} entity sets for org ${org.id}")

            appConfigKeysToCreate.forEach { ack ->
                val entitySet = generateEntitySet(org, apps.getValue(ack.appId), appTypes.getValue(ack.appTypeId))
                val entitySetId = entitySetManager.createEntitySet(userOwnerPrincipal, entitySet)
                val aceKeysToGrant = adminAceKeys + roleAcesByApp.getOrDefault(ack.appId, setOf()) + getAppPrincipalAce(ack.appId, ack.organizationId)

                newAppConfigEntries[ack] = AppTypeSetting(entitySetId, EnumSet.of(Permission.READ, Permission.WRITE))

                aclsToGrant.add(Acl(AclKey(entitySetId), aceKeysToGrant))
                toolbox.entityTypes.getValue(appTypes.getValue(ack.appTypeId).entityTypeId).properties.forEach { ptId ->
                    aclsToGrant.add(Acl(AclKey(entitySetId, ptId), aceKeysToGrant))
                }

            }
        }

        logger.info("Writing new app configs to the app configs mapstore")
        appConfigsMapstore.putAll(newAppConfigEntries)

        logger.info("Granting all org admin roles permissions on their new entity sets")
        authManager.addPermissions(aclsToGrant)

        return true
    }

    private fun getOrgRolesByPrincipalId(orgId: UUID): Map<String, SecurablePrincipal> {
        return spm.getAllRolesInOrganization(orgId).associateBy { it.principal.id }
    }

    private fun getRoleAcesForApp(
            rolesByPrincipalId: Map<String, SecurablePrincipal>,
            orgId: UUID,
            app: App,
            userOwnerPrincipal: Principal
    ): Set<Ace> {
        val aces = mutableSetOf<Ace>()
        EnumSet.of(Permission.READ, Permission.WRITE, Permission.OWNER).forEach { permission ->
            val title = "${app.title} - ${permission.name}"
            val principalId = "$orgId|$title"

            var principal: Principal? = null

            if (rolesByPrincipalId.containsKey(principalId)) {
                principal = rolesByPrincipalId.getValue(principalId).principal
            } else {
                rolesByPrincipalId.values.firstOrNull { it.title == title }?.let {
                    principal = it.principal
                }
            }

            if (principal == null) {
                logger.info("Could not find $permission principal for app ${app.name} in org $orgId")
                val description = "${permission.name} permission for the ${app.title} app"
                createAppRole(userOwnerPrincipal, orgId, title, principalId, description )
            }

            aces.add(Ace(principal, EnumSet.of(permission)))
        }

        return aces
    }

    private fun createAppRole(
            userOwnerPrincipal: Principal,
            orgId: UUID,
            title: String,
            name: String,
            description: String
    ): Principal {
        val rolePrincipal = Principal(PrincipalType.ROLE, name)
        val role = Role( Optional.empty(),
                orgId,
                rolePrincipal,
                title,
                Optional.of( description ) )

        spm.createSecurablePrincipalIfNotExists(userOwnerPrincipal, role)

        return rolePrincipal
    }

    private fun getOrCreateAdminRole(org: Organization, userOwnerPrincipal: Principal): Principal {
        val principalId = "${org.securablePrincipal.id}|${org.securablePrincipal.name} - ADMIN"
        val adminRolePrincipal = Principal(PrincipalType.ROLE, principalId)

        try {
            if (spm.lookup(adminRolePrincipal) != null) {
                return adminRolePrincipal
            }
        } catch (e: Exception) {
            logger.info("No AclKey found for admin role of org ${org.title} (${org.id})")
        }


        logger.info("About to create admin role for org ${org.title} (${org.id})")
        val adminRole = Role(
                Optional.empty(),
                org.id,
                adminRolePrincipal,
                "${org.securablePrincipal.name} - ADMIN",
                Optional.of("Administrators of this organization")
        )

        // create admin role
        spm.createSecurablePrincipalIfNotExists(userOwnerPrincipal, adminRole)

        // grant admin role permissions on org+roles
        val adminRoleAce = listOf(Ace(adminRolePrincipal, EnumSet.allOf(Permission::class.java)))
        val acls = org.roles.map { Acl(it.aclKey, adminRoleAce) } + Acl(AclKey(org.id), adminRoleAce)
        authManager.addPermissions(acls)

        // grant admin role to owner
        val roleAclKey = AclKey(org.id, adminRole.id)
        val userAclKey = spm.lookup(userOwnerPrincipal)
        spm.addPrincipalToPrincipal(roleAclKey, userAclKey)

        return adminRolePrincipal
    }


    private fun getUserOwnersOfOrgs(orgIds: Set<UUID>): Map<UUID, Principal> {
        val orgAclKeys = orgIds.map { AclKey(it) }
        return authManager.getOwnersForSecurableObjects(orgAclKeys).asMap()
                .mapKeys { it.key.first() }
                .mapValues { it.value.first { p -> p.type == PrincipalType.USER } }
    }

    private fun getNextAvailableName(name: String): String {
        var nameAttempt = name
        var counter = 1
        while (reservations.isReserved(nameAttempt)) {
            nameAttempt = "${name}_$counter"
            counter++
        }
        return nameAttempt
    }

    private fun formatEntitySetName(prefix: String, appTypeFqn: FullQualifiedName): String {
        val name = "${prefix}_${appTypeFqn.namespace}_${appTypeFqn.name}"
                .toLowerCase()
                .replace(regex = "[^a-z0-9_]".toRegex(), replacement = "")
        return getNextAvailableName(name)
    }

    private fun generateEntitySet(org: Organization, app: App, appType: AppType): EntitySet {
        val name = formatEntitySetName(org.title, appType.type)
        val title = "${appType.title} (${org.title})"
        val description = "Auto-generated for organization ${org.id}\n\n${appType.description}"
        val entitySet = EntitySet(
                UUID.randomUUID(),
                appType.entityTypeId,
                name,
                title,
                description,
                mutableSetOf(),
                mutableSetOf(),
                org.id,
                EnumSet.noneOf(EntitySetFlag::class.java))
        entitySet.setPartitions(org.partitions)
        return entitySet
    }

    private fun getOrCreateAppPrincipal(app: App, organizationId: UUID, adminPrincipal: Principal): Principal {
        val principal = Principal(PrincipalType.APP, "${app.id}|$organizationId")
        try {
            spm.lookup(principal)
        } catch (e: Exception) {
            logger.info("App principal did not exist for app ${app.id} in org $organizationId. Creating it now.")
            spm.createSecurablePrincipalIfNotExists(adminPrincipal, SecurablePrincipal(
                    AclKey(app.id, UUID.randomUUID()),
                    principal,
                    "${app.title} ($organizationId)",
                    Optional.of("${app.description}\nInstalled for organization $organizationId")
            ))
        }

        return principal
    }

    private fun getAppPrincipalAce(appId: UUID, organizationId: UUID): Ace {
        val principalId = "$appId|$organizationId"
        return Ace(Principal(PrincipalType.APP, principalId), EnumSet.of(Permission.READ, Permission.WRITE))
    }

}