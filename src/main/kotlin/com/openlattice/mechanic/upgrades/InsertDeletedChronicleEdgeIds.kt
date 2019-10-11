package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable.*
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

// for chronicle_recorded_by
val CHRONICLE_EDGES_ENTITY_SET_ID: UUID = UUID.fromString("d960f6c4-08b7-4d75-af18-396af170639d")

class InsertDeletedChronicleEdgeIds(val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(InsertDeletedChronicleEdgeIds::class.java)
    }

    override fun upgrade(): Boolean {

        val edgeEntitySet = toolbox.entitySets.getValue(CHRONICLE_EDGES_ENTITY_SET_ID)
        val partitionsVersion = edgeEntitySet.partitionsVersion
        val partitions = edgeEntitySet.partitions
        val partitionsSql = "'{${partitions.joinToString(",")}}'::integer[]"

        val version = System.currentTimeMillis()

        val insertToIdsSql = insertToIdsQuery(partitionsSql, partitionsVersion, version)
        val insertToDataSql = insertToDataQuery(partitionsSql, partitionsVersion, version)

        logger.info("Insert to ids sql: {}", insertToIdsSql)
        logger.info("Insert to data sql: {}", insertToDataSql)


        logger.info("About to do update")
        try {
            toolbox.hds.connection.use { conn ->

                conn.createStatement().use { stmt ->
                    val sw = Stopwatch.createStarted()
                    val insertedToIdsCount = stmt.executeUpdate(insertToIdsSql)
                    logger.info(
                            "Inserted {} entity key ids into ids table in {} ms.",
                            insertedToIdsCount,
                            sw.elapsed(TimeUnit.MILLISECONDS)
                    )

                    sw.reset().start()
                    val insertedToDataCount = stmt.executeUpdate(insertToDataSql)
                    logger.info(
                            "Inserted {} entity key ids into data table in {} ms.",
                            insertedToDataCount,
                            sw.elapsed(TimeUnit.MILLISECONDS)
                    )
                }
            }

            logger.info("Update complete!")

        } catch (e: Exception) {
            logger.info("Something bad happened :(", e)
        }

        return true
    }

    private val withQuery = "WITH chronicle_edge_ids AS (" +
            "SELECT DISTINCT ${PostgresColumn.EDGE_ENTITY_KEY_ID.name} " +
            "FROM ${E.name} " +
            "WHERE ${PostgresColumn.EDGE_ENTITY_SET_ID.name} = '$CHRONICLE_EDGES_ENTITY_SET_ID') "

    private fun insertToIdsQuery(partitions: String, partitionsVersion: Int, version: Long): String {

        val insertCols = listOf(
                PostgresColumn.PARTITION,
                PostgresColumn.ENTITY_SET_ID,
                PostgresColumn.ID_VALUE,
                PostgresColumn.VERSION,
                PostgresColumn.VERSIONS,
                DataTables.LAST_WRITE,
                PostgresColumn.PARTITIONS_VERSION
        ).joinToString(",") { it.name }

        return "$withQuery INSERT INTO ${IDS.name} ($insertCols) " +
                "SELECT " +
                    "($partitions)[ 1 + (('x'||right(${PostgresColumn.EDGE_ENTITY_KEY_ID.name}::text,8))::bit(32)::int % array_length($partitions,1))] as ${PostgresColumn.PARTITION.name}, " +
                    "'$CHRONICLE_EDGES_ENTITY_SET_ID'::uuid as ${PostgresColumn.ENTITY_SET_ID.name}, " +
                    "${PostgresColumn.EDGE_ENTITY_KEY_ID.name} as ${PostgresColumn.ID_VALUE.name}, " +
                    "$version AS ${PostgresColumn.VERSION.name}, " +
                    "ARRAY[$version] AS ${PostgresColumn.VERSIONS.name}, " +
                    "now() AS ${DataTables.LAST_WRITE.name}, " +
                    "$partitionsVersion AS ${PostgresColumn.PARTITIONS_VERSION.name} " +
                " FROM chronicle_edge_ids ON CONFLICT DO NOTHING"
    }

    private fun insertToDataQuery(partitions: String, partitionsVersion: Int, version: Long): String {

        val insertCols = listOf(
                PostgresColumn.PARTITION,
                PostgresColumn.ENTITY_SET_ID,
                PostgresColumn.ID_VALUE,
                PostgresColumn.PROPERTY_TYPE_ID,
                PostgresColumn.HASH,
                PostgresColumn.VERSION,
                PostgresColumn.VERSIONS,
                DataTables.LAST_WRITE,
                PostgresColumn.PARTITIONS_VERSION
        ).joinToString(",") { it.name }

        return "$withQuery INSERT INTO ${DATA.name} ($insertCols) " +
                "SELECT " +
                    "($partitions)[ 1 + (('x'||right(${PostgresColumn.EDGE_ENTITY_KEY_ID.name}::text,8))::bit(32)::int % array_length($partitions,1))] as ${PostgresColumn.PARTITION.name}, " +
                    "'$CHRONICLE_EDGES_ENTITY_SET_ID'::uuid as ${PostgresColumn.ENTITY_SET_ID.name}, " +
                    "${PostgresColumn.EDGE_ENTITY_KEY_ID.name} as ${PostgresColumn.ID_VALUE.name}, " +
                    "'${IdConstants.ID_ID.id}'::uuid as ${PostgresColumn.PROPERTY_TYPE_ID.name}, " +
                    "'\\xdeadbeefdeadbeef'::bytea as ${PostgresColumn.HASH.name}, " +
                    "$version AS ${PostgresColumn.VERSION.name}, " +
                    "ARRAY[$version] AS ${PostgresColumn.VERSIONS.name}, " +
                    "now() AS ${DataTables.LAST_WRITE.name}, " +
                    "$partitionsVersion AS ${PostgresColumn.PARTITIONS_VERSION.name} " +
                " FROM chronicle_edge_ids ON CONFLICT DO NOTHING"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

}