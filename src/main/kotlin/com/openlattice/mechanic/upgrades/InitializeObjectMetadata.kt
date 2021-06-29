package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.AclKey
import com.openlattice.datasets.DataSetService
import com.openlattice.datasets.SecurableObjectMetadata
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox

class InitializeObjectMetadata(
        private val toolbox: Toolbox,
        private val datasetService: DataSetService
) : Upgrade {

    override fun upgrade(): Boolean {

        toolbox.entitySets.values.forEach { entitySet ->
            datasetService.initializeMetadata(AclKey(entitySet.id), SecurableObjectMetadata.fromEntitySet(entitySet))

            val entityType = toolbox.entityTypes.getValue(entitySet.entityTypeId)

            entityType.properties.map { toolbox.propertyTypes.getValue(it) }.forEach {
                val flags = entityType.propertyTags[it.id] ?: mutableSetOf<String>()
                datasetService.initializeMetadata(AclKey(entitySet.id, it.id), SecurableObjectMetadata.fromPropertyType(it, flags))
            }
        }

        val tableToColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(toolbox.hazelcast).values.toList()
                .groupBy { it.tableId }.mapValues { it.value }

        HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast).values.toList().forEach { table ->
            datasetService.initializeMetadata(AclKey(table.id), SecurableObjectMetadata.fromExternalTable(table))

            tableToColumns[table.id]?.forEach {
                datasetService.initializeMetadata(it.getAclKey(), SecurableObjectMetadata.fromExternalColumn(it))
            }
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_03_10.value
    }
}
