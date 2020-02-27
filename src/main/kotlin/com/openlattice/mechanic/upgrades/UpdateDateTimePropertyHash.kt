package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumn.PROPERTY_TYPE_ID
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDataTables
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
    }

    override fun upgrade(): Boolean {

        val dateTimePropertyTypes = toolbox.propertyTypes.values.filter { it.datatype == EdmPrimitiveTypeKind.DateTimeOffset }

        // rewrite all datetime values from data table into new rows with newly computed hashes
        insertRowsWithNewHashesToDataTable(dateTimePropertyTypes)

        // delete old rows from data table
        deleteOldDateTimeRowsFromDataTable(dateTimePropertyTypes)

        return true
    }

    private fun insertRowsWithNewHashesToDataTable(dateTimePropertyTypes: Collection<PropertyType>) {
        logger.info("About to insert new datetime rows into the data table")

        val sw = Stopwatch.createStarted()

        val dateTimePropertyTypeIds = dateTimePropertyTypes.groupBy { it.postgresIndexType }

        dateTimePropertyTypeIds.entries.stream().parallel().forEach { pair ->

            val indexType = pair.key
            val propertyTypeIds = pair.value.map { it.id }.toSet()

            val dateTimeCol = PostgresDataTables.getColumnDefinition(indexType, EdmPrimitiveTypeKind.DateTimeOffset)
            val insertSql = getInsertSql(dateTimeCol.name)

            val entityTypeIds = toolbox.entityTypes.values.filterNot { Sets.haveEmptyIntersection(it.properties, propertyTypeIds) }.map { it.id }.toSet()
            val entitySetIds = toolbox.entitySets.values.filter { entityTypeIds.contains(it.entityTypeId) }.map { it.id }

            logger.info("Updating property types in column ${dateTimeCol.name} using SQL: $insertSql")

            toolbox.hds.connection.use { conn ->
                conn.prepareStatement(insertSql).use { ps ->

                    ps.setArray(1, PostgresArrays.createUuidArray(conn, propertyTypeIds))
                    ps.setArray(2, PostgresArrays.createUuidArray(conn, entitySetIds))

                    ps.execute()
                }
            }

        }

        logger.info("Finished inserting new datetime rows into the data table. Took ${sw.elapsed(TimeUnit.SECONDS)} seconds.")
    }

    private fun deleteOldDateTimeRowsFromDataTable(dateTimePropertyTypes: Collection<PropertyType>) {
        logger.info("About to delete old datetime rows from the data table")
        val sw = Stopwatch.createStarted()


        toolbox.hds.connection.use { conn ->
            conn.prepareStatement(DELETE_SQL).use { ps ->
                ps.setArray(1, PostgresArrays.createUuidArray(conn, dateTimePropertyTypes.map { it.id }))
                ps.execute()
            }
        }


        logger.info("Finished deleting old datetime rows from the data table. Took ${sw.elapsed(TimeUnit.SECONDS)} seconds.")

    }

    private val DATA_TABLE_KEY_COLS = listOf(
            ENTITY_SET_ID,
            ID,
            ORIGIN_ID,
            PARTITION,
            PROPERTY_TYPE_ID,
            HASH
    ).map{ it.name }

    private val DATA_TABLE_METADATA_COLS = listOf(
            LAST_WRITE,
            LAST_PROPAGATE,
            VERSION,
            VERSIONS
    ).map { it.name }


    private fun updateColumnIfLatestVersion(col: PostgresColumnDefinition): String {

        return "${col.name} = CASE " +
                "WHEN abs(${DATA.name}.${VERSION.name}) <= abs(EXCLUDED.${VERSION.name}) " +
                "THEN EXCLUDED.${col.name} " +
                "ELSE ${DATA.name}.${col.name} " +
                "END"
    }

    /**
     * Bind order:
     *
     * 1) property_type_id array
     * 2) entity_set_id array
     */
    private fun getInsertSql(dateTimeCol: String): String {

        val insertCols = (DATA_TABLE_KEY_COLS + DATA_TABLE_METADATA_COLS + dateTimeCol).joinToString(",")

        val newHashComputation = "int8send(floor(extract(epoch from $dateTimeCol) * 1000)::bigint)"
        val keyCols = DATA_TABLE_KEY_COLS.joinToString(", ")
        val selectDistinctCols = "${(DATA_TABLE_KEY_COLS - HASH.name).joinToString(", ")}, $newHashComputation AS ${HASH.name}"
        val selectMetadataCols = "${DATA_TABLE_METADATA_COLS.joinToString(", ")}, $dateTimeCol"

        return "INSERT INTO ${DATA.name} ( $insertCols ) " +
                "SELECT DISTINCT ON ($keyCols) $selectDistinctCols, $selectMetadataCols " +
                "FROM ${DATA.name} " +
                "WHERE ${PROPERTY_TYPE_ID.name} = ANY(?) " +
                "AND ${ENTITY_SET_ID.name} = ANY(?) " +
                "AND length(${HASH.name}) = 16 " +
                "ON CONFLICT ($keyCols) DO UPDATE SET " +
                "${LAST_WRITE.name} = GREATEST(${DATA.name}.${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
                "${updateColumnIfLatestVersion(VERSION)}, " +
                "${updateColumnIfLatestVersion(VERSIONS)} "
    }


    private val DELETE_SQL = "DELETE FROM ${DATA.name} WHERE ${PROPERTY_TYPE_ID.name} = ANY(?) AND length(${HASH.name}) = 16"

}