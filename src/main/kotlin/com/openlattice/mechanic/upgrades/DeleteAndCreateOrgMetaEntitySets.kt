package com.openlattice.mechanic.upgrades

import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.*
import org.slf4j.LoggerFactory
import java.util.*

class DeleteAndCreateOrgMetaEntitySets(
    private val toolbox: Toolbox,
    private val orgsService: HazelcastOrganizationService,
    private val entitySetsService: EntitySetManager,
    private val metadataEntitySetsService: OrganizationMetadataEntitySetsService,
    private val externalDatabaseManagementService: ExternalDatabaseManagementService
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(DeleteAndCreateOrgMetaEntitySets::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_02_14.value
    }

    override fun upgrade(): Boolean {

        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()
        orgs.values.forEach {

            deleteOrgMetaEntitySet(it, it.organizationMetadataEntitySetIds.organization)
            deleteOrgMetaEntitySet(it, it.organizationMetadataEntitySetIds.columns)
            deleteOrgMetaEntitySet(it, it.organizationMetadataEntitySetIds.datasets)

            logger.info("setting org metadata entity set ids to UNINITIALIZED - ${it.title} (${it.id})")
            orgsService.setOrganizationMetadataEntitySetIds(it.id, OrganizationMetadataEntitySetIds())

            logger.info("initializing org metadata entity sets - ${it.title} (${it.id})")
            metadataEntitySetsService.initializeOrganizationMetadataEntitySets(it.id)
            logger.info("finished initializing org metadata entity sets - ${it.title} (${it.id})")

            logger.info("syncing org external database tables and columns - ${it.title} (${it.id})")
            syncOrgExternalDatabaseObjects(it)
            logger.info("finished syncing org external database tables and columns - ${it.title} (${it.id})")
        }

        return true
    }

    private fun deleteOrgMetaEntitySet(org: Organization, entitySetId: UUID) {
        if (entitySetId != UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            logger.info("getting org metadata entity set (${entitySetId}) - ${org.title} (${org.id})")
            val entitySet = entitySetsService.getEntitySet(entitySetId)
            if (entitySet != null) {
                logger.info("deleting org metadata entity set (${entitySetId}) - ${org.title} (${org.id})")
                entitySetsService.deleteEntitySet(entitySet)
            }
            else {
                logger.info("org metadata entity set (${entitySetId}) is null - ${org.title} (${org.id})")
            }
        }
        else {
            logger.info("org metadata entity set (${entitySetId}) is UNINITIALIZED - ${org.title} (${org.id})")
        }
    }

    private fun syncOrgExternalDatabaseObjects(org: Organization) {

        logger.info("getting org external database tables - ${org.title} (${org.id})")
        val tables = externalDatabaseManagementService.getExternalDatabaseTables(org.id)
        logger.info("table ids - ${tables.keys}")

        logger.info("getting org external database columns - ${org.title} (${org.id})")
        val columns = tables
            .keys
            .map { id -> externalDatabaseManagementService.getExternalDatabaseTableWithColumns(id) }
            .associateBy(
                { pair -> pair.table.id },
                { pair -> pair.columns }
            )
        val columnIds = columns.mapValues { it.value.map { c -> c.id }.toSet() }
        logger.info("column ids by table id - $columnIds")

        metadataEntitySetsService.addDatasetsAndColumns(org.id, tables.values, columns)
    }
}
