package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.graph.IdType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private const val BATCH_SIZE = 16000
private val SOUTH_DAKOTA_ENTITY_SET_IDS = listOf(
        "066aab8f-1703-44e4-8dea-82bf88310d6b",
        "0a48710c-3899-4743-b1f7-28c6f99aa202",
        "0ac30441-caac-4949-b835-f37b9e19a3ca",
        "0f8c334b-b4bb-4073-84d7-4772f8f7748d",
        "12a7f0e2-bd3c-42e1-84e2-c180b981223f",
        "14d5501a-a85a-4c59-bf54-460633159709",
        "183df52c-99ec-4295-bc14-cf718fdae042",
        "1866931f-438f-4921-a0bf-c3d3918ca1e1",
        "1a4d5164-2bc1-4713-8e46-83bed2e13ca3",
        "1b1cd21f-ca69-4fda-981e-230695676710",
        "1f53dd17-035a-4459-902d-a641225662a0",
        "24ac2aaf-3df2-4fa4-9ea6-f40df6f070c2",
        "278dcead-2829-4850-8c0a-60650b0f71d6",
        "2dcd95d7-5f50-491d-9604-83ac0c217102",
        "2e75dd35-7e23-42b8-84b1-3ba3aa343e94",
        "31c3d8e1-e1e1-4911-a00e-f38a534f045a",
        "32e44e1f-5587-4ef8-ad37-e93ef9bdcbb9",
        "33c594b5-ce87-451b-b495-0f1612f7966c",
        "35e34daf-2904-417b-81c3-23e55faabe3a",
        "399f142c-8e1c-4bf9-94e7-af4194db26c3",
        "3b61121a-f752-4877-8524-1c282a92e067",
        "3fa68a17-a8ff-4210-b6e0-6b53872a82a5",
        "40787a1f-9480-44f9-84b5-d00262dbd1ef",
        "48d4d37d-8969-4fe8-bb90-e5d69eef20b1",
        "4e2fbfbf-2f74-4c6f-80cd-28af74727c2b",
        "4f365c13-0640-4da8-bea5-c1c5e3a9f7af",
        "51097efb-1647-476b-80c7-b8c31c168bd7",
        "52ad520d-5a98-4a0d-bcb2-eb12f2e05445",
        "52e9e9b8-5bcf-4015-ba97-3c2256a334f1",
        "5ea29b47-6fde-4156-b8ec-1208e6ee4f50",
        "6010b530-7243-4de6-95f9-8dbc59e34872",
        "626ade80-8bc2-4137-ae1b-c78967a9191c",
        "64cc0ba6-36f5-489d-b1f6-99f53d71a80f",
        "68cd6423-d288-4018-9ec1-a5ae6787b101",
        "6b5dada9-f8f5-4442-abf0-fb8da6d505ba",
        "6bb61e76-c601-47a3-a986-af86c6c1bf83",
        "7215ba23-ca45-4f19-9c29-2bc73be67733",
        "72230e7a-9441-4938-86fa-1db2a80265d0",
        "724981ae-6a43-4fec-9d58-c2b267fbad66",
        "73eec1ed-bf50-41a8-aa05-5866d6bc4e44",
        "77bc0857-8825-4e5c-a56e-2f662febd5a6",
        "7a1bd40d-1a31-4c2a-ade3-fa16eaa9f56b",
        "7a6fb1d7-6b68-48a2-8639-3d2d53f7437c",
        "7c60ed45-95fc-4347-b6e3-2077ff906812",
        "7cbbb1ce-691f-41d7-bfe0-56a898520623",
        "7e9591f0-989f-4b6e-9782-6141f54171b8",
        "804f2943-a2d0-4569-93a1-58ce3e3e69c2",
        "851ffa0b-aa25-43c3-b04e-ddd5b34001e4",
        "8549f48b-8827-4bf3-b093-ab34a25c543c",
        "8615d6ac-a1a1-42a5-a3c5-89b3121e8854",
        "88c3568a-2c2f-4e7a-8987-38c8b36cc164",
        "8a4547e4-48cf-477c-be0a-15415c56fe10",
        "8c2fe1e1-bcde-48c7-ab25-ea7f3a95a87e",
        "8c9237fb-0319-4c08-9ab6-29a7525de1c9",
        "8d493e9f-9e15-4941-9ad0-65380b6f5af3",
        "8f5d484c-605d-49b1-b1d5-a0b7c3b762d0",
        "90aeacac-a6aa-4912-ab7d-381b57631e57",
        "9802bc1a-c024-4fea-88d4-d7906fdf2ca3",
        "9d38aa5a-2c83-4afa-839c-1e03089fec30",
        "9de47145-aa7e-437e-8c55-25066486e889",
        "9e7eb62f-4170-417f-b922-93ad8af8c2d9",
        "a0320fed-fa85-4f34-82f9-588033021ca4",
        "a1648fd0-0274-4fc2-8f36-d4428c4455b6",
        "a79a0fb6-181d-441d-81b3-040741b7ee8b",
        "aa7011b3-53ce-49ef-8168-2b86d5787f60",
        "aadde33e-d17f-45f1-a966-4d16d033a8a0",
        "abed0de0-e358-4539-859e-c5b63e371c6e",
        "adc4db1e-a2ba-465d-a448-19ed1e74aa84",
        "b6015703-278d-4595-ae88-939da0b91816",
        "b66606a0-f8b1-4023-aa22-ead7f571ba3b",
        "bab936f2-4833-4f58-9298-23ba1ae35214",
        "c3a43642-b995-456f-879f-8b38ea2a2fc3",
        "c40aad67-8347-4e42-9485-ead842e7b28f",
        "c674cecb-9977-4c26-bef9-6db481dde3ac",
        "c7811a15-d774-4417-a9cc-52f7e28672b5",
        "c80aae3a-7d21-4a6e-9672-5adf34f65e1e",
        "ce92a664-6b15-438e-9fca-594f1df61227",
        "d09b2570-efb6-4ecd-9fde-dc122c97e6ac",
        "d16ad959-5f37-4d37-9a80-e114a05e690d",
        "d2989930-8c4d-49aa-833f-56612977e839",
        "d57f8ff6-f632-44ca-86cb-66d6007d7cf8",
        "d6cac010-5680-44f9-824b-98c4e2550f2f",
        "d6d6eea1-f2d3-4776-8583-8da7a2cfdb0e",
        "d724d8f2-da4c-46e0-b5d8-5db8c3367b50",
        "d81fa49d-549b-4978-8fe7-1f568a909850",
        "da9bcdf0-4fc8-4cde-9ebc-48133e5e9348",
        "e3da2472-43b4-4fef-9b73-86d038da6921",
        "e4559b24-17bf-4c24-9d6c-9e218a1440ea",
        "e46a4b2c-e609-4531-a7d4-a5274b2e39cd",
        "e5752990-1651-455a-9240-5dd7f739b1df",
        "e62490fc-a53e-4181-a45b-1519fdc2e68d",
        "e9e38764-1f16-4b98-a173-2f0dd6ae9b8c",
        "ed5716db-830b-41b7-9905-24fa82761ace",
        "ee2fc738-38f6-4655-bc79-5e87a6f85501",
        "ee56f37b-72cb-4fd3-be13-b0333dde89c4",
        "f1b446b5-622f-4815-9737-2932e831cafb",
        "f1f84b47-d843-43fe-aeff-fa653b8e3f51",
        "f25af9ae-4e95-45cc-a0d4-99d4de7db293",
        "f5044fcd-5ae5-419c-81c3-3a1c16272302",
        "f64765c9-aab0-4132-ac19-054d79631245",
        "f8f05c3c-d99b-4939-980b-e19df4ec63a5",
        "fa29142b-648c-4c62-a097-343802c3bf5d",
        "fa8dd8b5-1c63-448d-874c-f0b95fd2d34c",
        "fb3ce259-e4ab-4346-93ff-fbb459cda47c",
        "fdbc7dc9-9f5e-4438-8837-bb969cbdf4d0",
        "fdec4c8e-4086-4b21-8c2f-b14ac7269ba7"
).map(UUID::fromString)

@Component
class UpgradeEdgesTable(val toolbox: Toolbox) : Upgrade {
    private val limiter = Semaphore(16)

    companion object {
        private val logger = LoggerFactory.getLogger(UpgradeEdgesTable::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.createTable(E)
        /*
                            PARTITION,
                            ID_VALUE,
                            SRC_ENTITY_SET_ID,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID,
                            EDGE_ENTITY_SET_ID,
                            EDGE_ENTITY_KEY_ID,
                            VERSION,
                            VERSIONS,
                            PARTITIONS_VERSION
         */
        addMigratedVersionColumn()

        val insertCols = E.columns.joinToString(",") { it.name }

//        toolbox.entitySets.values.map { it.id }.stream().parallel().forEach { // TODO use this one for all edges
        toolbox.entitySets.keys.filter { SOUTH_DAKOTA_ENTITY_SET_IDS.contains(it) }.stream().parallel().forEach {

            try {
                limiter.acquire()

                val srcPartitionSql = "${migratedVersionSql(SRC_ENTITY_SET_ID, it)} INSERT INTO ${E.name} ( $insertCols ) " +
                        buildEdgeSelection(SRC_ENTITY_SET_ID)
                val dstPartitionSql = "${migratedVersionSql(DST_ENTITY_SET_ID, it)} INSERT INTO ${E.name} ( $insertCols ) " +
                        buildEdgeSelection(DST_ENTITY_SET_ID)
                val edgePartitionSql = "${migratedVersionSql(EDGE_ENTITY_SET_ID, it)} INSERT INTO ${E.name} ( $insertCols ) " +
                        buildEdgeSelection(EDGE_ENTITY_SET_ID)

                logger.info("Src sql: {}", srcPartitionSql)
                logger.info("Dst sql: {}", dstPartitionSql)
                logger.info("Edge sql: {}", edgePartitionSql)

                toolbox.hds.connection.use { conn ->
                    conn.autoCommit = false
                    var insertCounter = 0
                    var insertCount = 1
                    val swTotal = Stopwatch.createStarted()

                    conn.createStatement().use { stmt ->
                        val sw = Stopwatch.createStarted()
                        while (insertCount > 0) {
                            val srcCount = stmt.executeUpdate(srcPartitionSql)
                            val dstCount = stmt.executeUpdate(dstPartitionSql)
                            val edgeCount = stmt.executeUpdate(edgePartitionSql)
                            logger.info("Inserted {} edges into src partitions.", srcCount)
                            logger.info("Inserted {} edges into dst partitions.", dstCount)
                            logger.info("Inserted {} edges into edge partitions.", edgeCount)
                            insertCount = srcCount + dstCount + edgeCount
                            insertCounter += insertCount

                            conn.commit()
                        }
                        logger.info(
                                "Migrated batch of {} edges into E table in {} ms. Total so far: {} in {} ms",
                                insertCount,
                                sw.elapsed(TimeUnit.MILLISECONDS),
                                insertCounter,
                                swTotal.elapsed(TimeUnit.MILLISECONDS)
                        )
                    }
                }

            } finally {
                limiter.release()
            }
        }

        return true
    }

    fun migratedVersionSql(joinColumn: PostgresColumnDefinition, entitySetId: UUID): String {
        return "WITH for_migration AS ( UPDATE ${EDGES.name} SET migrated_version = abs(version) " +
                "WHERE ${COMPONENT_TYPES.name} = ${IdType.SRC.ordinal} AND ${joinColumn.name} = '$entitySetId' AND " +
                "(id,edge_comp_1,edge_comp_2,component_types) in ( select id,edge_comp_1,edge_comp_2,component_types FROM ${EDGES.name} migrated_version < abs(migrated_version)) " +
                "LIMIT $BATCH_SIZE) ) RETURNING *) "
    }

    fun addMigratedVersionColumn() {

        logger.info("About to add migrated_version to edges table")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                        "ALTER TABLE ${EDGES.name} ADD COLUMN if not exists migrated_version bigint NOT NULL DEFAULT 0"
                )
            }
        }
        logger.info("Added migrated_version to edges table")
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }


    private fun buildEdgeSelection(joinColumn: PostgresColumnDefinition): String {
        val selectCols = listOf(
                "partitions[ 1 + (('x'||right(id::text,8))::bit(32)::int % array_length(partitions,1))] as partition",
                SRC_ENTITY_SET_ID.name,
                "${ID_VALUE.name} as ${SRC_ENTITY_KEY_ID.name}",
                DST_ENTITY_SET_ID.name,
                "${EDGE_COMP_1.name} as ${DST_ENTITY_KEY_ID.name}",
                EDGE_ENTITY_SET_ID.name,
                "${EDGE_COMP_2.name} as ${EDGE_ENTITY_KEY_ID.name}",
                VERSION.name,
                VERSIONS.name,
                PARTITIONS_VERSION.name
        ).joinToString(",")
        return "SELECT $selectCols FROM ${EDGES.name} INNER JOIN (select id as ${joinColumn.name}, partitions, partitions_version from ${ENTITY_SETS.name} as entity_set_partitions USING(${joinColumn.name}) "
    }

}