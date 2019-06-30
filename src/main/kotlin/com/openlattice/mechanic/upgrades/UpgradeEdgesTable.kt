package com.openlattice.mechanic.upgrades

import com.openlattice.graph.IdType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class UpgradeEdgesTable(val toolbox: Toolbox) : Upgrade {
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

        val migratedVersionSql = "WITH ( UPDATE ${E.name} SET migrated_version = abs(version) WHERE ( migrated_version < abs(migrated_version) ) RETURNING *) "

        val srcPartitionSql = "$migratedVersionSql INSERT INTO ${E.name} ( $insertCols ) " + buildEdgeSelection(SRC_ENTITY_SET_ID)
        val dstPartitionSql = "$migratedVersionSql INSERT INTO ${E.name} ( $insertCols ) " + buildEdgeSelection(DST_ENTITY_SET_ID)
        val edgePartitionSql = "$migratedVersionSql INSERT INTO ${E.name} ( $insertCols ) " + buildEdgeSelection(EDGE_ENTITY_SET_ID)

        logger.info("Src sql: {}", srcPartitionSql)
        logger.info("Dst sql: {}", dstPartitionSql)
        logger.info("Edge sql: {}", edgePartitionSql)

        toolbox.hds.connection.use { conn ->
            val srcCount = conn.createStatement().use { it.executeUpdate(srcPartitionSql) }
            logger.info("Inserted {} edges into src partitions.", srcCount)
            val dstCount = conn.createStatement().use { it.executeUpdate(dstPartitionSql) }
            logger.info("Inserted {} edges into dst partitions.", dstCount)
            val edgeCount = conn.createStatement().use { it.executeUpdate(edgePartitionSql) }
            logger.info("Inserted {} edges into edge partitions.", edgeCount)
        }

        return true
    }

    fun addMigratedVersionColumn() {

        logger.info("About to add migrated_version to edges table")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                        "ALTER TABLE ${E.name} ADD COLUMN if not exists migrated_version bigint NOT NULL DEFAULT 0"
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
        return "SELECT $selectCols FROM ${EDGES.name} INNER JOIN (select id as ${joinColumn.name}, partitions, partitions_version from ${ENTITY_SETS.name} where entity_type_id in (select id from entity_types where id in (select id from association_types where '31cf5595-3fe9-4d3e-a9cf-39355a4b8cab' = ANY(src) or '31cf5595-3fe9-4d3e-a9cf-39355a4b8cab' = ANY(dst)) ) as entity_set_partitions USING(${joinColumn.name}) " +
                "WHERE ${COMPONENT_TYPES.name} = ${IdType.SRC.ordinal}"
    }

}