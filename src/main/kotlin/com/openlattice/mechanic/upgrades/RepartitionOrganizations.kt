package com.openlattice.mechanic.upgrades

import com.google.common.collect.Maps
import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask
import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask.Companion.ENTITY_SET_SIZES_VIEW
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ORGANIZATIONS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import java.util.*

val GLOBAL_ORG_ID = UUID.fromString("00000000-0000-0001-0000-000000000000")

val NCRIC_ORG_IDS = listOf(
        UUID.fromString("72bd145f-d1c9-460c-8e7a-d16100eb8527"), // intellisite
        UUID.fromString("1cfb5870-9368-4dcf-9bea-13954d6e52ae"), // genetec
        UUID.fromString("47b646d7-a01a-4232-b25b-15c880ea4046") // NCRIC
)

const val GET_ENTITY_SET_COUNTS_SQL = "SELECT * FROM $ENTITY_SET_SIZES_VIEW WHERE $COUNT > 0"

val ALL_PARTITIONS = (0..256).toList()

class RepartitionOrganizations(val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {

        val assignedPartitions = chooseNewPartitionsForOrganizations()

        updateOrgPartitions(assignedPartitions)

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }

    /* Plan the repartitioning */

    private fun getEntityCountByOrg(): Map<UUID, Long> {
        val entitySetCounts = BasePostgresIterable(StatementHolderSupplier(toolbox.hds, GET_ENTITY_SET_COUNTS_SQL)) {
            ResultSetAdapters.entitySetId(it) to ResultSetAdapters.count(it)
        }.toMap()

        val orgCounts = mutableMapOf<UUID, Long>()

        entitySetCounts.forEach { (entitySetId, count) ->
            toolbox.entitySets[entitySetId]?.organizationId?.let { id ->
                orgCounts[id] = orgCounts.getOrDefault(id, 0) + count
            }
        }

        return orgCounts
    }

    private fun getNumPartitionsForOrg(id: UUID): Int {
        return when (id) {
            GLOBAL_ORG_ID -> ALL_PARTITIONS.size
            in NCRIC_ORG_IDS -> 15
            else -> 2
        }
    }

    private fun chooseNewPartitionsForOrganizations(): Map<UUID, List<Int>> {

        // entitiesPerAssignedPartition does not actually track the number of current entities in the system per
        // assigned partition as the data is currently living on random partitions and won't be moved by this migration,
        // but it will partition organizations proportionally to how much data they have, and any future or repartitioned
        // entity sets will respect the new partitions
        val entitiesPerAssignedPartition = ALL_PARTITIONS.associateWith { 0L }.toMutableMap()
        val assignedOrganizationPartitions = mutableMapOf<UUID, List<Int>>()

        getEntityCountByOrg().entries.sortedByDescending { it.value }.forEach { (id, orgTotalCount) ->

            val numPartitions = getNumPartitionsForOrg(id)
            val orgPartitionCount = orgTotalCount / numPartitions

            val orgPartitions = entitiesPerAssignedPartition.entries
                    .sortedBy { it.value } // NOTE: this is not ideal but since we only have 150 orgs at the time of writing this, should be fine
                    .take(numPartitions)
                    .map { it.key }
                    .sorted()

            assignedOrganizationPartitions[id] = orgPartitions

            orgPartitions.forEach {
                entitiesPerAssignedPartition[it] = entitiesPerAssignedPartition.getValue(it) + orgPartitionCount
            }
        }

        return assignedOrganizationPartitions
    }

    /* Do the repartitioning */

    private fun getUpdateOrgPartitionSql(orgId: UUID, partitions: List<Int>): String {
        return """
            UPDATE ${ORGANIZATIONS.name}              
            SET ${ORGANIZATION.name} = jsonb_set( 
              ${ORGANIZATION.name}, 
              '{${PARTITIONS.name}}', 
              '[${partitions.joinToString(",")}]'::jsonb 
            ) 
            WHERE ${ID.name} = '$orgId'
        """.trimIndent()
    }

    private fun updateOrgPartitions(orgPartitions: Map<UUID, List<Int>>) {
        toolbox.hds.connection.use { conn ->
            val stmt = conn.createStatement()

            orgPartitions.forEach { (orgId, partitions) ->
                stmt.addBatch(getUpdateOrgPartitionSql(orgId, partitions))
            }

            stmt.executeBatch()
        }

        HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).loadAll(true)
    }
}