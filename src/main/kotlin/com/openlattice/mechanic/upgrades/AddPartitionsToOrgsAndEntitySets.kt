package com.openlattice.mechanic.upgrades

import com.hazelcast.map.IMap
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.Organization
import org.slf4j.LoggerFactory
import java.util.*

class AddPartitionsToOrgsAndEntitySets(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddPartitionsToOrgsAndEntitySets::class.java)
    }

    override fun upgrade(): Boolean {

        val organizations = HazelcastMap.ORGANIZATIONS.getMap( toolbox.hazelcast )
        val entitySets = HazelcastMap.ENTITY_SETS.getMap( toolbox.hazelcast )
        val partitionManager = PartitionManager(toolbox.hazelcast, toolbox.hds)

        addMissingPartitionsToOrganizations(partitionManager, organizations)
        addMissingPartitionsToEntitySets(partitionManager, entitySets)
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_12_12.value
    }

    private fun addMissingPartitionsToOrganizations(
            partitionManager: PartitionManager,
            organizations: IMap<UUID, Organization>
    ) {

        logger.info("About to add missing partitions to organizations.")

        organizations.values.filter { it.partitions.isEmpty() }.forEach {

            logger.info("Allocating partitions for organization {} [{}]", it.title, it.id)
            val partitions = partitionManager.allocateDefaultOrganizationPartitions(it.id)
            logger.info("Organization {} assigned partitions {}", it.id, partitions)
        }

        logger.info("Done adding missing partitions to organizations.")

    }

    private fun addMissingPartitionsToEntitySets(partitionManager: PartitionManager, entitySets: IMap<UUID, EntitySet>) {

        logger.info("About to add missing partitions to entity sets.")

        toolbox.entitySets.values.filter { it.partitions.isEmpty() }.forEach {

            logger.info("Allocating partitions for entity set {} [{}]", it.name, it.id)
            val entitySetWithAllocatedPartitions = partitionManager.allocateEntitySetPartitions(it)
            entitySets[entitySetWithAllocatedPartitions.id] = entitySetWithAllocatedPartitions
            logger.info("Entity set {} assigned partitions {}", it.id, entitySetWithAllocatedPartitions.partitions)
        }

        logger.info("Done adding missing partitions to entity sets.")

    }

}