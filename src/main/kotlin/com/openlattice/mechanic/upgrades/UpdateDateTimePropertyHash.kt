package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.elasticsearch.common.util.set.Sets
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class UpdateDateTimePropertyHash(private val toolbox: Toolbox) : Upgrade {

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }

    companion object {
        private val logger = LoggerFactory.getLogger(UpdateDateTimePropertyHash::class.java)

        val TEMP_TABLE_NAME = "temp_datetime_data"
        val OLD_HASHES_COL = PostgresColumnDefinition("old_hashes", PostgresDatatype.BYTEA_ARRAY).notNull()
        val VALUE_COLUMN = PostgresColumnDefinition(PostgresDataTables.getSourceDataColumnName(PostgresDatatype.TIMESTAMPTZ, IndexType.NONE), PostgresDatatype.TIMESTAMPTZ)
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
        val sql = getPopulateTempTableSql()

        logger.info("About to populate temp table using sql: $sql")

        val propertyTypeIds = toolbox.propertyTypes.values
                .filter { it.datatype == EdmPrimitiveTypeKind.DateTimeOffset && it.postgresIndexType == IndexType.NONE }
                .map { it.id }
                .toMutableSet()

        val entityTypeIds = toolbox.entityTypes.values.filterNot { Sets.haveEmptyIntersection(it.properties, propertyTypeIds) }.map { it.id }.toSet()
        val entitySetIds = toolbox.entitySets.values.filter { entityTypeIds.contains(it.entityTypeId) }.map { it.id }

        toolbox.hds.connection.use { conn ->
            conn.prepareStatement(sql).use { ps ->

                ps.setArray(1, PostgresArrays.createUuidArray(conn, propertyTypeIds))
                ps.setArray(2, PostgresArrays.createUuidArray(conn, entitySetIds))

                ps.execute()
            }
        }

        logger.info("Finished populating temp table in ${sw.elapsed(TimeUnit.SECONDS)} seconds.")
    }

    private fun insertRowsWithNewHashesToDataTable() {
        val insertSql = insertRehashedRowsIntoDataTableSql()
        logger.info("Inserting/updating hashes to the data table using sql: $insertSql")
        val sw = Stopwatch.createStarted()

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(insertSql)
            }
        }

        logger.info("Finished inserting new datetime rows into the data table. Took ${sw.elapsed(TimeUnit.SECONDS)} seconds.")
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
            LAST_PROPAGATE,
            VERSION,
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

    private fun onConflictClause(tableName: String): String {
        val keyCols = DATA_TABLE_KEY_COLS.joinToString(", ")

        return "ON CONFLICT ($keyCols) DO UPDATE SET " +
                "${LAST_WRITE.name} = GREATEST($tableName.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
                "${updateColumnIfLatestVersion(tableName, VERSION)}, " +
                "${updateColumnIfLatestVersion(tableName, VERSIONS)} "
    }

    /**
     * Bind order:
     *
     * 1) property_type_id array
     * 2) entity_set_id array
     */
    private fun getPopulateTempTableSql(): String {
        val newHashComputation = "int8send(floor(extract(epoch from ${VALUE_COLUMN.name}) * 1000)::bigint)"

        val unchangedCols = TEMP_TABLE_UNCHANGED_COLS.joinToString(", ") { it.name }

        val onConflict = "${onConflictClause(TEMP_TABLE_NAME)}, " +
                "${OLD_HASHES_COL.name} = $TEMP_TABLE_NAME.${OLD_HASHES_COL.name} || EXCLUDED.${OLD_HASHES_COL.name}"

        return "INSERT INTO $TEMP_TABLE_NAME " +
                "SELECT $unchangedCols, $newHashComputation AS ${HASH.name}, ARRAY[${HASH.name}] AS ${OLD_HASHES_COL.name} " +
                "FROM ${DATA.name} " +
                "WHERE ${PROPERTY_TYPE_ID.name} = ANY(?) " +
                "AND ${ENTITY_SET_ID.name} = ANY(?) " +
                "AND length(${HASH.name}) = 16 " +
                onConflict
    }

    private fun insertRehashedRowsIntoDataTableSql(): String {

        val insertSelectCols = (TEMP_TABLE_UNCHANGED_COLS + HASH).joinToString(", ") { it.name }

        return "INSERT INTO ${DATA.name} ($insertSelectCols) " +
                "SELECT $insertSelectCols FROM $TEMP_TABLE_NAME " +
                onConflictClause(DATA.name)
    }

    private fun deleteSql(): String {
        val colsToMatch = (DATA_TABLE_KEY_COLS - HASH.name).joinToString(" AND ") { "${DATA.name}.$it = $TEMP_TABLE_NAME.$it" }

        return "DELETE FROM ${DATA.name} " +
                "USING $TEMP_TABLE_NAME " +
                "WHERE $colsToMatch " +
                "AND ${DATA.name}.${HASH.name} = ANY($TEMP_TABLE_NAME.${OLD_HASHES_COL.name})"
    }

}