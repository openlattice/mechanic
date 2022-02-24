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
 */
class GrantChronicleSuperUserMissingReadPermissions(
    toolbox: Toolbox,
    private val authorizationManager: AuthorizationManager,
    private val entitySetService: EntitySetManager,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val principalsManager: SecurePrincipalsManager
) : Upgrade {
    val entitySetIds: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).associate { it.value.name to it.key }


    companion object {
        private val logger = LoggerFactory.getLogger(GrantChronicleSuperUserMissingReadPermissions::class.java)

        const val studiesEntitySetName = "chronicle_study"
        val generalStringFqn = FullQualifiedName("general.stringid")
    }

    override fun upgrade(): Boolean {
        val studiesEntitySetId = entitySetIds.getValue(studiesEntitySetName)

        val studyIds = getStudyIds(studiesEntitySetId)
        val participantEntitySetIds = getParticipantEntitySetIds(studyIds)
        val propertyTypes = entitySetService.getPropertyTypesForEntitySet(participantEntitySetIds.first()).keys

        val aclKeys = participantEntitySetIds.map { entitySetId -> propertyTypes.map { propertyType -> AclKey(entitySetId, propertyType) } }
            .flatten()

        val securablePrincipal = principalsManager.getSecurablePrincipal("")
        val principals = principalsManager.getAllPrincipals(securablePrincipal).map { it.principal }

        aclKeys.forEach {
            authorizationManager.addPermission(it, principals.first(), EnumSet.of(Permission.READ))
        }
        logger.info("Granted chronicle super user read on participant entity sets and properties")
        logger.info("entity set ids: $participantEntitySetIds")
        return true
    }

    private fun getParticipantEntitySetIds(studyIds: Set<UUID>): Set<UUID> {
        return studyIds.mapNotNull { entitySetIds["chronicle_participants_$it"] }.toSet()
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
            ).values.map { it[generalStringFqn]?.iterator()?.next().toString() }.map { UUID.fromString(it) }.toSet()
    }


    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}