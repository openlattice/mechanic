package com.openlattice.mechanic.upgrades

import com.openlattice.data.DataGraphManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.Organization
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

class SyncOrgEntitySetsMissingInMeta(
    private val toolbox: Toolbox,
    private val entitySetsService: EntitySetManager,
    private val metadataEntitySetsService: OrganizationMetadataEntitySetsService,
    private val edmService: EdmManager,
    private val dataService: DataGraphManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(SyncOrgEntitySetsMissingInMeta::class.java)
        private val ID_FQN = FullQualifiedName("ol.id")
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_03_10.value
    }

    override fun upgrade(): Boolean {

        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()
        val entitySetsByOrg = toolbox.entitySets.values.groupBy { it.organizationId }

        orgs.values.forEachIndexed { index, org ->

            logger.info("================================")
            logger.info("================================")
            logger.info("starting processing org ${org.id}")

            val orgEntitySetsIds = entitySetsByOrg.getOrDefault(org.id, listOf()).mapNotNull { it.id }.toSet()
            if (orgEntitySetsIds.isEmpty()) {
                logger.warn("org does not have entity sets")
                return@forEachIndexed
            }

            val idPT = edmService.getPropertyType(ID_FQN)
            if (idPT == null) {
                logger.info("ol.id PropertyType is null")
                return@forEachIndexed
            }

            val dataSetsESID = org.organizationMetadataEntitySetIds.datasets
            val dsEntitySet = entitySetsService.getEntitySet(dataSetsESID)
            if (dsEntitySet == null) {
                logger.info("\"datasets\" meta entity set is null")
                return@forEachIndexed
            }

            val data = dataService.getEntitySetData(
                mapOf(dataSetsESID to Optional.empty()),
                linkedSetOf(idPT.type.toString()),
                mapOf(dataSetsESID to mapOf(idPT.id to idPT)),
                dsEntitySet.isLinking
            )

            if (data.entities == null) {
                logger.info("getEntitySetData returned null entities")
                return@forEachIndexed
            }

            val ids = data.entities
                .filterNotNull()
                .mapNotNull { entity ->
                    val id = entity[ID_FQN]?.first()
                    try {
                        UUID.fromString(id.toString())
                    } catch (e: Exception) {
                        logger.error("invalid data set id $id", e)
                        return@mapNotNull null
                    }
                }
                .toSet()

            val targetIds = orgEntitySetsIds.subtract(ids)
            logger.info("found ${targetIds.size} entity sets to sync - $targetIds")

            if (targetIds.isNotEmpty()) {
                logger.info("starting syncing org entity sets")
                syncOrgEntitySets(org, targetIds)
                logger.info("finished syncing org entity sets")
            }

            logger.info("finished processing org ${org.id}")
            logger.info("progress ${((index + 1) / orgs.size) * 100}%")
            logger.info("================================")
            logger.info("================================")
        }

        return true
    }

    private fun syncOrgEntitySets(org: Organization, ids: Set<UUID>) {
        try {
            logger.info("getting entity sets")
            val entitySets = toolbox.entitySets.filter { ids.contains(it.key) }.values
            if (entitySets.size != ids.size) {
                throw IllegalStateException("entitySets.size does not match ids.size")
            }
            val propertyTypesByEntitySet = mutableMapOf<UUID, Collection<PropertyType>>()
            logger.info("getting property types")
            entitySets.forEach { entitySet ->
                val entityType = toolbox.entityTypes[entitySet.entityTypeId]
                if (entityType == null) {
                    logger.warn("encountered null entity type for entity set ${entitySet.id}")
                    return@forEach
                }
                val propertyTypes = toolbox.propertyTypes.filter { entityType.properties.contains(it.key) }
                propertyTypesByEntitySet[entitySet.id] = propertyTypes.values
            }
            metadataEntitySetsService.addDatasetsAndColumns(entitySets, propertyTypesByEntitySet)
        } catch (e: Exception) {
            logger.error("caught exception while syncing org ${org.id} entity sets", e)
        }
    }
}
