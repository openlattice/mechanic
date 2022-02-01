package com.openlattice.mechanic.upgrades

import com.geekbeast.postgres.*
import com.google.common.base.Stopwatch
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.elasticsearch.common.util.set.Sets
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class UpdateDateTimePropertyHash(private val toolbox: Toolbox) : Upgrade {
    private val limiter = Semaphore(16)

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }

    companion object {
        private val logger = LoggerFactory.getLogger(UpdateDateTimePropertyHash::class.java)

        val TEMP_TABLE_NAME = "temp_datetime_data"
        val OLD_HASHES_COL = PostgresColumnDefinition("old_hashes", PostgresDatatype.BYTEA_ARRAY).notNull()
        val VALUE_COLUMN = PostgresColumnDefinition(PostgresDataTables.getSourceDataColumnName(PostgresDatatype.TIMESTAMPTZ, IndexType.NONE), PostgresDatatype.TIMESTAMPTZ)
        val LAST_MIGRATE = "last_migrate"
        val BATCH_SIZE = 16_000

        val NCRIC_ENTITY_SETS = mapOf(
                "2d20ecf8-17ca-43b5-9d49-1721b2e79cc7" to 166,
                "93969261-557b-4f8f-816a-381bb279bf9d" to 106,
                "d4601423-24ed-4d6b-aa0b-263d82fe67ab" to 206
        ).mapKeys { UUID.fromString(it.key) }
    }

    override fun upgrade(): Boolean {

        // create temp table
        createTempDataTable()

        // write rows from data table into temp table
        populateTempTable()

        // rewrite all datetime values from data table into new rows with newly computed hashes
        insertRowsWithNewHashesToDataTable()

        // delete old rows from data table
        deleteOldDateTimeRowsFromDataTable()

        return true
    }

    private fun createTempDataTable() {

        val tempTableDefinition = CitusDistributedTableDefinition(TEMP_TABLE_NAME)
                .addColumns(*TEMP_TABLE_COLS.toTypedArray())
                .primaryKey(ENTITY_SET_ID, ID_VALUE, ORIGIN_ID, PARTITION, PROPERTY_TYPE_ID, HASH)
                .distributionColumn(PARTITION)

        val createTableSql = tempTableDefinition.createTableQuery()
        val distributeTableSql = tempTableDefinition.createDistributedTableQuery()

        logger.info("About to create temp data table using sql: $createTableSql")
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createTableSql)
            }
        }

        logger.info("Distributing temp data table using sql: $distributeTableSql")
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(distributeTableSql)
            }
        }

        logger.info("Finished creating temp table")
    }

    private fun populateTempTable() {
        val sw = Stopwatch.createStarted()
        val insertSql = getPopulateTempTableSql()

        logger.info("About to populate temp table using sql: $insertSql")

        val propertyTypeIds = toolbox.propertyTypes.values
                .filter { it.datatype == EdmPrimitiveTypeKind.DateTimeOffset && it.postgresIndexType == IndexType.NONE }
                .map { it.id }
                .toMutableSet()

        val entityTypeIds = toolbox.entityTypes.values.filterNot { Sets.haveEmptyIntersection(it.properties, propertyTypeIds) }.map { it.id }.toSet()
        val entitySetIds = toolbox.entitySets.values.filter { entityTypeIds.contains(it.entityTypeId) }.map { it.id }

        toolbox.hds.connection.use { conn ->

            conn.prepareStatement(insertSql).use { ps ->

                ps.setArray(1, PostgresArrays.createUuidArray(conn, propertyTypeIds))
                ps.setArray(2, PostgresArrays.createUuidArray(conn, entitySetIds))

                ps.execute()
            }
        }

        logger.info("Finished populating temp table in ${sw.elapsed(TimeUnit.SECONDS)} seconds.")
    }

    private fun insertRowsWithNewHashesToDataTable() {
        logger.info("Inserting/updating hashes to the data table using sql")
        val sw = Stopwatch.createStarted()

        NCRIC_ENTITY_SETS.values.stream().parallel().forEach { partition ->
            limiter.acquire()

            val insertSql = insertRehashedRowsIntoDataTableSql(partition)
            logger.info("Migrating non-NCRIC data for partition $partition using SQL: $insertSql")

            val partitionSw = Stopwatch.createStarted()
            var partitionTotalUpdated = 0
            var insertCount = 1

            toolbox.hds.connection.use { conn ->

                conn.createStatement().use { stmt ->

                    while (insertCount > 0) {
                        insertCount = stmt.executeUpdate(insertSql)
                        partitionTotalUpdated += insertCount
                        logger.info("Migrated a batch of $insertCount elements for partition $partition")
                    }
                }

            }

            logger.info("Finished migrating $partitionTotalUpdated rows for partition $partition in ${partitionSw.elapsed(TimeUnit.MILLISECONDS)} ms.")
            limiter.release()
        }

        logger.info("Finished inserting new non-NCRIC datetime rows into the data table. Took ${sw.elapsed(TimeUnit.SECONDS)} seconds.")


        logger.info("About to insert NCRIC datetime rows into the data table.")

        NCRIC_ENTITY_SETS.entries.stream().parallel().forEach {
            val entitySetId = it.key
            val partition = it.value

            val sql = nonBatchedInsertRehashedRowsIntoDataTableSql(entitySetId, partition)

            logger.info("Updating all entity set $entitySetId data using SQL: $sql")

            toolbox.hds.connection.use { conn ->

                conn.createStatement().use { stmt ->
                    val updateCount = stmt.executeUpdate(sql)
                    logger.info("Updated $updateCount rows for entity set $entitySetId")
                }

            }
        }

        logger.info("All the data has been migrated to the data table! All that is left now is to delete......")
    }

    private fun deleteOldDateTimeRowsFromDataTable() {
        val deleteSql = deleteSql()
        logger.info("About to delete old datetime rows from the data table using sql: $deleteSql")
        val sw = Stopwatch.createStarted()


        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(deleteSql)
            }
        }

        logger.info("Finished deleting old datetime rows from the data table. Took ${sw.elapsed(TimeUnit.SECONDS)} seconds.")

    }

    private val TEMP_TABLE_UNCHANGED_COLS = listOf(
            ENTITY_SET_ID,
            ID_VALUE,
            ORIGIN_ID,
            PARTITION,
            PROPERTY_TYPE_ID,
            LAST_WRITE,
            VERSIONS,
            VALUE_COLUMN
    )

    private val TEMP_TABLE_COLS = TEMP_TABLE_UNCHANGED_COLS + listOf(
            HASH,
            OLD_HASHES_COL
    )

    private val DATA_TABLE_KEY_COLS = listOf(
            ENTITY_SET_ID,
            ID,
            ORIGIN_ID,
            PARTITION,
            PROPERTY_TYPE_ID,
            HASH
    ).map { it.name }

    private fun updateColumnIfLatestVersion(tableName: String, col: PostgresColumnDefinition): String {

        return "${col.name} = CASE " +
                "WHEN abs($tableName.${VERSION.name}) <= abs(EXCLUDED.${VERSION.name}) " +
                "THEN EXCLUDED.${col.name} " +
                "ELSE $tableName.${col.name} " +
                "END"
    }

    /**
     * Bind order:
     *
     * 1) property_type_id array
     * 2) entity_set_id array
     */
    private fun getPopulateTempTableSql(): String {
        val newHashComputation = "int8send(floor(extract(epoch from ${VALUE_COLUMN.name}) * 1000)::bigint)"
        val keyCols = (DATA_TABLE_KEY_COLS - HASH.name).joinToString(", ")

        val versionsOnConflict = "${VERSIONS.name} = CASE " +
                "WHEN $TEMP_TABLE_NAME.${LAST_WRITE.name} <= EXCLUDED.${LAST_WRITE.name} " +
                "THEN EXCLUDED.${VERSIONS.name} " +
                "ELSE $TEMP_TABLE_NAME.${VERSIONS.name} " +
                "END"

        val onConflict =  "ON CONFLICT ($keyCols, ${HASH.name}) DO UPDATE SET " +
                "${LAST_WRITE.name} = GREATEST($TEMP_TABLE_NAME.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
                "$versionsOnConflict," +
                "${OLD_HASHES_COL.name} = $TEMP_TABLE_NAME.${OLD_HASHES_COL.name} || EXCLUDED.${OLD_HASHES_COL.name}"

        val sortVersions = "ARRAY(SELECT DISTINCT ${VERSION.name} FROM (SELECT ${VERSION.name} FROM UNNEST(array_cat_agg(${VERSIONS.name})) AS foo(${VERSION.name}) ORDER BY abs(foo.${VERSION.name})) AS bar) AS ${VERSIONS.name}"

        return "INSERT INTO $TEMP_TABLE_NAME " +
                "SELECT $keyCols, " +
                "  max(${LAST_WRITE.name}) AS ${LAST_WRITE.name}, " +
                "  $sortVersions, " +
                "  max(${VALUE_COLUMN.name}), " +
                "  $newHashComputation AS ${HASH.name}, " +
                "  array_agg(${HASH.name}) AS ${OLD_HASHES_COL.name} " +
                "FROM ${DATA.name} " +
                "WHERE ${PROPERTY_TYPE_ID.name} = ANY(?) " +
                "AND ${ENTITY_SET_ID.name} = ANY(?) " +
                "AND length(${HASH.name}) = 16 " +
                "GROUP BY $keyCols, $newHashComputation " +
                onConflict
    }

    private fun insertRehashedRowsIntoDataTableSql(partition: Int): String {
        val keyCols = DATA_TABLE_KEY_COLS.joinToString(", ")
        val insertSelectCols = (TEMP_TABLE_UNCHANGED_COLS + HASH + VERSION).joinToString(", ") { it.name }

        val notNCRIC = " NOT (${ENTITY_SET_ID.name} = ANY('{${NCRIC_ENTITY_SETS.keys.joinToString(",")}}') ) "

        val maxAbsVersions = "${VERSIONS.name}[array_upper(${VERSIONS.name}, 1)]"

        val getBatch = "WITH batch AS ( " +
                "UPDATE $TEMP_TABLE_NAME " +
                "SET $LAST_MIGRATE = now() " +
                "WHERE ${PARTITION.name} = $partition " +
                "AND $notNCRIC " +
                "AND id in (" +
                "  SELECT id " +
                "  FROM $TEMP_TABLE_NAME " +
                "  WHERE $LAST_MIGRATE = '-infinity' " +
                "  AND ${PARTITION.name} = $partition " +
                "  AND $notNCRIC " +
                "  LIMIT $BATCH_SIZE" +
                ") RETURNING *, $maxAbsVersions as ${VERSION.name} )"

        return "$getBatch INSERT INTO ${DATA.name} ($insertSelectCols) " +
                "  SELECT $insertSelectCols " +
                "  FROM batch " +
                "ON CONFLICT ($keyCols) DO UPDATE SET " +
                "${LAST_WRITE.name} = GREATEST(${DATA.name}.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
                "${updateColumnIfLatestVersion(DATA.name, VERSION)}, " +
                "${updateColumnIfLatestVersion(DATA.name, VERSIONS)} "

    }

    private fun nonBatchedInsertRehashedRowsIntoDataTableSql(entitySetId: UUID, partition: Int): String {
        val keyCols = DATA_TABLE_KEY_COLS.joinToString(", ")
        val insertSelectCols = (TEMP_TABLE_UNCHANGED_COLS + HASH).joinToString(", ") { it.name }

        val maxAbsVersions = "${VERSIONS.name}[array_upper(${VERSIONS.name}, 1)]"

        return "INSERT INTO ${DATA.name} ($insertSelectCols, ${VERSION.name}) " +
                "  SELECT $insertSelectCols, $maxAbsVersions AS ${VERSION.name} " +
                "  FROM $TEMP_TABLE_NAME " +
                "  WHERE ${ENTITY_SET_ID.name} = '$entitySetId' " +
                "  AND ${PARTITION.name} = $partition " +
                "ON CONFLICT ($keyCols) DO UPDATE SET " +
                "${LAST_WRITE.name} = GREATEST(${DATA.name}.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
                "${updateColumnIfLatestVersion(DATA.name, VERSION)}, " +
                "${updateColumnIfLatestVersion(DATA.name, VERSIONS)} "

    }

    private fun deleteSql(): String {
        val colsToMatch = (DATA_TABLE_KEY_COLS - HASH.name).joinToString(" AND ") { "${DATA.name}.$it = $TEMP_TABLE_NAME.$it" }

        return "DELETE FROM ${DATA.name} " +
                "USING $TEMP_TABLE_NAME " +
                "WHERE $colsToMatch " +
                "AND ${DATA.name}.${HASH.name} = ANY($TEMP_TABLE_NAME.${OLD_HASHES_COL.name})"
    }

}