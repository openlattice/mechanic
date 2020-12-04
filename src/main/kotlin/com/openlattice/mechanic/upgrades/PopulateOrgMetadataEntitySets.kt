package com.openlattice.mechanic.upgrades

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.OrganizationMetadataEntitySetsService

class PopulateOrgMetadataEntitySets(
        private val toolbox: Toolbox,
        private val metadataEntitySetService: OrganizationMetadataEntitySetsService
): Upgrade {

    override fun upgrade(): Boolean {
        val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).values.toList()

        val entityTypesToPropertyTypes = toolbox.entityTypes.mapValues { it.value.properties.map { id -> toolbox.propertyTypes.getValue(id) } }
        val entitySetsToPropertyTypes = toolbox.entitySets.mapValues { entityTypesToPropertyTypes.getValue(it.value.entityTypeId) }

        val entitySetsByOrg = toolbox.entitySets.values.groupBy { it.organizationId }
        val externalTablesByOrg = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(toolbox.hazelcast).values.groupBy { it.organizationId }
        val externalColumnsByTable = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(toolbox.hazelcast).values.groupBy { it.tableId }

        organizations.forEach { organization ->
            val orgId = organization.id

            entitySetsByOrg[orgId]?.let { entitySets ->
                entitySets.forEach {
                    metadataEntitySetService.addDataset(it)
                    metadataEntitySetService.addDatasetColumns(it, entitySetsToPropertyTypes.getValue(it.id))
                }
            }

            externalTablesByOrg[orgId]?.let { tables ->
                tables.forEach {
                    metadataEntitySetService.addDataset(orgId, it)
                    metadataEntitySetService.addDatasetColumns(orgId, it, externalColumnsByTable.getValue(it.id))

                }
            }

        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }
}