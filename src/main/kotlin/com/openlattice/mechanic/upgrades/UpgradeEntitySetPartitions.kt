package com.openlattice.mechanic.upgrades


import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ENTITY_KEY_IDS
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.slf4j.LoggerFactory
import java.util.*

class UpgradeEntitySetPartitions(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private const val PRIMEY_BOI = 257
        private val PARTITION_RANGE = 1..PRIMEY_BOI
        private val logger = LoggerFactory.getLogger(MigratePropertyValuesToDataTable::class.java)

    }

    private val entitySetSizes = retrieveEntitySetSizes()
    private val THE_BIG_ONE = entitySetSizes.values.maxBy { it.count }?.count ?: throw IllegalStateException(
            "Entity set sizes cannot be null."
    )

    override fun upgrade(): Boolean {
        //Manually ran upgrade to alter entity sets table.
        toolbox.hds.connection.use { conn ->
            entitySetSizes.forEach { (entitySetId, entitySetInfo) ->
                val partitions = getPartitions(entitySetId, entitySetInfo)
                val entitySet = toolbox.entitySets.getValue(entitySetId)
                entitySet.setPartitions(partitions)
                toolbox.esms.store(entitySetId, entitySet)
                logger.info(
                        "Partitiosn for entity set {} ({}) => ({},{})",
                        entitySet.name,
                        entitySet.id,
                        entitySet.partitions,
                        entitySet.partitionsVersion
                )
            }
        }
        return true
    }

    private fun retrieveEntitySetSizes(): Map<UUID, EntitySetInfo> {
        return BasePostgresIterable<Pair<UUID, EntitySetInfo>>(
                StatementHolderSupplier(toolbox.hds, GET_ENTITY_SET_COUNT)
        ) { rs ->
            ResultSetAdapters.entitySetId(rs) to EntitySetInfo(
                    ResultSetAdapters.count(rs),
                    ResultSetAdapters.entitySetFlags(rs)
            )
        }.toMap()
    }

    private fun getPartitions(entitySetId: UUID, entitySetInfo: EntitySetInfo): List<Int> {
        return if (entitySetInfo.flags.contains(EntitySetFlag.AUDIT)) {
            PARTITION_RANGE.toList()
        } else {
            when (entitySetId) {
                //NCRIC EntitySets
                UUID.fromString("65705aee-52af-4199-b013-83d8041af42d"),
                UUID.fromString("892b4c4f-578d-45e7-81e6-acbbf849031c"),
                UUID.fromString("4f4f94f1-5e04-425d-a22a-5f191671a817"),
                UUID.fromString("61939128-b38f-400a-8183-4fb6a0d84bc7"),
                UUID.fromString("2be6cd8c-a66e-4d45-9197-66ff91d99102"),
                UUID.fromString("21a4de85-0faa-4417-b073-270dd72f6384"),
                UUID.fromString("e3f97f98-a36f-470e-aa90-dbbcc02184ca"),
                UUID.fromString("38ee5039-cdf2-4158-9be9-5466c42d0345"),
                UUID.fromString("7416886d-ec88-4d42-b62d-322ad31dfe72"),
                UUID.fromString("12481c74-cfea-4b96-b132-dbbee3abb405"),
                UUID.fromString("81c7e707-f8bf-4685-8761-f81800c5d2f1"),
                UUID.fromString("c8a4d76e-8a5d-43c1-9f36-661b898036ed"),
                UUID.fromString("52ef171a-37fb-445b-9c00-8dd99d0e3649"),
                UUID.fromString("5bfa1659-a03e-4da8-8f34-8565c5f6550e"),
                UUID.fromString("ed316b25-e6de-4532-9043-e3f1c96982e3"),
                UUID.fromString("6bd62b44-4bab-4df1-b8c3-69d9f6f25380"),
                UUID.fromString("77a97eae-3a99-4b81-88a6-8ba2424cc936"),
                UUID.fromString("a223cfd7-0906-48f5-81d5-f03f1ace7c01"),
                UUID.fromString("b47a9f9a-60eb-42c1-9cb2-02957aaa80de") -> PARTITION_RANGE.toList()
                //Chronicle data sets
                UUID.fromString("fda9b1c1-6cea-4130-8d86-057e659bb9ea"),
                UUID.fromString("671ec7c3-99a1-423b-98ff-c7e2b3ea6efc") -> PARTITION_RANGE.toList()
                else -> getPartitionValues(entitySetInfo.count)
            }
        }
    }

    private fun getPartitionValues(esSize: Long): List<Int> {
        val spread = Math.ceil(esSize.toDouble() / THE_BIG_ONE).toInt()
        var shuffled = PARTITION_RANGE.shuffled()
        return shuffled.take(spread)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}

private data class EntitySetInfo(val count: Long, val flags: Set<EntitySetFlag>)

private val GET_ENTITY_SET_COUNT = "SELECT * FROM (SELECT ${ENTITY_SET_ID.name}, count(*)  FROM ${ENTITY_KEY_IDS.name} GROUP BY (${ENTITY_SET_ID.name}) as entity_set_counts INNER JOIN (select id as entity_set_id ,${FLAGS.name} as entity_set_id from entity_sets) USING (entity_set_id) "