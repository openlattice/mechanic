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

        val newAppConfigEntries = mutableMapOf<AppConfigKey, AppTypeSetting>()
        val aclsToGrant = mutableListOf<Acl>()

        orgs.values.forEach { org ->

            val appConfigKeysToCreate = mutableSetOf<AppConfigKey>()

            val adminRole = tryGetOrgAdminRole(org)
            val rolesByPrincipalId = getOrgRolesByPrincipalId(org.id)
            val adminAceKeys = adminRole?.let { listOf(Ace(it, EnumSet.allOf(Permission::class.java))) } ?: listOf()
            val roleAcesByApp = mutableMapOf<UUID, Set<Ace>>()

            org.apps.forEach { appId ->

                val app = apps.getValue(appId)
                roleAcesByApp[appId] = getRoleAcesForApp(rolesByPrincipalId, org.id, app)

                app.appTypeIds.forEach { appTypeId ->

                    val appConfigKey = AppConfigKey(appId, org.id, appTypeId)
                    if (!appConfigs.containsKey(appConfigKey)) {
                        appConfigKeysToCreate.add(appConfigKey)
                    }

                }
            }

            val userOwnerPrincipal = orgsToUserOwners[org.id]

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
                val aceKeysToGrant = adminAceKeys + roleAcesByApp.getOrDefault(ack.appId, setOf())

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

    private fun getRoleAcesForApp(rolesByPrincipalId: Map<String, SecurablePrincipal>, orgId: UUID, app: App): Set<Ace> {
        val aces = mutableSetOf<Ace>()
        EnumSet.of(Permission.READ, Permission.WRITE, Permission.OWNER).forEach { permission ->
            val title = "${app.title} - ${permission.name}"
            val principalId = "$orgId-$title"

            var principal: Principal? = null

            if (rolesByPrincipalId.containsKey(principalId)) {
                principal = rolesByPrincipalId.getValue(principalId).principal
            } else {
                rolesByPrincipalId.values.firstOrNull { it.title == title }?.let {
                    principal = it.principal
                }
            }

            if (principal != null) {
                aces.add(Ace(principal, EnumSet.of(permission)))
            } else  {
                logger.info("Could not find $permission principal for app ${app.name} in org $orgId")
            }
        }

        return aces
    }

    private fun tryGetOrgAdminRole(org: Organization): Principal? {
        val principalId = "${org.securablePrincipal.id}|${org.securablePrincipal.name} - ADMIN"
        val principal = Principal(PrincipalType.ROLE, principalId)

        try {
            if (spm.lookup(principal) != null) {
                return principal
            }
        } catch (e: Exception) {
            logger.info("No AclKey found for admin role of org ${org.title} (${org.id})")
        }
        return null
    }

    private fun getUserOwnersOfOrgs(orgIds: Set<UUID>): Map<UUID, Principal?> {
        val orgAclKeys = orgIds.map { AclKey(it) }
        return authManager.getOwnersForSecurableObjects(orgAclKeys).asMap()
                .mapKeys { it.key.first() }
                .mapValues { it.value.firstOrNull { p -> p.type == PrincipalType.USER } }
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

}