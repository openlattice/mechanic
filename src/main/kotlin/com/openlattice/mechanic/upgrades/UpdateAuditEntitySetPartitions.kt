package com.openlattice.mechanic.upgrades

import com.openlattice.auditing.AuditRecordEntitySetConfiguration
import com.openlattice.authorization.AclKey
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.PARTITIONS
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.slf4j.LoggerFactory

class UpdateAuditEntitySetPartitions(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(UpdateAuditEntitySetPartitions::class.java)
    }


    override fun upgrade(): Boolean {
        val entitySetPartitions = toolbox.entitySets.values
                .filter { !it.flags.contains(EntitySetFlag.AUDIT) }
                .associate { it.id to it.partitions }

        logger.info("Mapped ${entitySetPartitions.size} non-audit entity set ids to their partitions")

        val auditRecordEntitySets = HazelcastMap.AUDIT_RECORD_ENTITY_SETS.getMap( toolbox.hazelcast )

        val sql = "UPDATE ${ENTITY_SETS.name} SET ${PARTITIONS.name} = ? WHERE ${ID.name} = ANY(?)"

        logger.info("About to upgrade audit entity set partitions using sql: $sql")

        val numUpdates = toolbox.hds.connection.use { conn ->
            conn.prepareStatement(sql).use { ps ->

                auditRecordEntitySets.forEach {
                    val entitySetId = it.key.first()

                    if (entitySetPartitions.containsKey(entitySetId)) {
                        val partitionsArr = PostgresArrays.createIntArray(conn, entitySetPartitions.getValue(entitySetId))
                        val auditEntitySetIds = PostgresArrays.createUuidArray(conn, it.value.auditRecordEntitySetIds + it.value.auditEdgeEntitySetIds)

                        ps.setArray(1, partitionsArr)
                        ps.setArray(2, auditEntitySetIds)
                        ps.addBatch()
                    }
                }

                ps.executeBatch().sum()
            }
        }

        logger.info("Successfully upgraded $numUpdates audit entity set partitions.")

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_12_12.value
    }
}