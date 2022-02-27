package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Permission
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 *
 * // grant chronicle superuser READ permissions on all legacy study participant entity sets and associated property types
 */
class GrantChronicleSuperUserPermissionsOnLegacyEntitySets(
    toolbox: Toolbox,
    private val authorizationManager: AuthorizationManager,
    private val entitySetService: EntitySetManager,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val principalsManager: SecurePrincipalsManager
) : Upgrade {
    private val entitySetIdByName: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).associate { it.value.name to it.key }
    private val entitySetNameById: Map<UUID, String> = entitySetIdByName.entries.associate { it.value to it.key }

    companion object {
        private val logger = LoggerFactory.getLogger(GrantChronicleSuperUserPermissionsOnLegacyEntitySets::class.java)

        private const val studiesEntitySetName = "chronicle_study"
        private val GENERAL_STRING_FQN = FullQualifiedName("general.stringid")
    }

    override fun upgrade(): Boolean {
        val studiesEntitySetId = entitySetIdByName.getValue(studiesEntitySetName)

        val studyIds = getStudyIds(studiesEntitySetId)
        val participantEntitySetIds = getParticipantEntitySetIds(studyIds)
        logger.info("Participant entity sets found: $participantEntitySetIds")

        val propertyTypes = entitySetService.getPropertyTypesForEntitySet(participantEntitySetIds.first()).keys

        val securablePrincipal = principalsManager.getSecurablePrincipal("auth0|5ae9026c04eb0b243f1d2bb6")
        val principals = principalsManager.getAllPrincipals(securablePrincipal).map { it.principal }.toSet()
        logger.info("principals: $principals")

        val requiredPermissions = EnumSet.of(Permission.READ, Permission.OWNER, Permission.WRITE)

        val unauthorizedAclKeys = participantEntitySetIds.map { entitySetId -> propertyTypes.map { propertyType -> AclKey(entitySetId, propertyType) } }
            .flatten().filter { ackKey -> !authorizationManager.checkIfHasPermissions(ackKey, principals, requiredPermissions) }

        unauthorizedAclKeys.forEach { aclKey ->
            authorizationManager.addPermission(aclKey, principals.first(), requiredPermissions)
        }

        val uniqueEntitySetIdsResolved = unauthorizedAclKeys.map { it.first() }.toSet()
        val associatedStudies = uniqueEntitySetIdsResolved.map { entitySetNameById.getValue(it) }

        logger.info("Granted chronicle super user $requiredPermissions on studies: $associatedStudies")

        return true
    }

    private fun getParticipantEntitySetIds(studyIds: Set<UUID>): Set<UUID> {
        return studyIds.mapNotNull { entitySetIdByName["chronicle_participants_$it"] }.toSet()
    }

    private fun getStudyIds(entitySetId: UUID): Set<UUID> {
        return dataQueryService
            .getEntitiesWithPropertyTypeFqns(
                mapOf(entitySetId to Optional.empty()),
                entitySetService.getPropertyTypesOfEntitySets(setOf(entitySetId)),
                mapOf(),
                setOf(),
                Optional.empty(),
                false
            ).values.mapNotNull {  getFirstUUIDOrNull(it, GENERAL_STRING_FQN) }.toSet()
    }

    private fun getFirstUUIDOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): UUID? {
        return when (val string = getFirstValueOrNull(entity, fqn)) {
            null -> null
            else -> UUID.fromString(string)
        }
    }

    private fun getFirstValueOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): String? {
        entity[fqn]?.iterator()?.let {
            if (it.hasNext()) return it.next().toString()
        }
        return null
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}