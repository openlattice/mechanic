package com.openlattice.mechanic.upgrades

import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory

class AdjustNCRICDataDateTimeHashes(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AdjustNCRICDataDateTimeHashes::class.java)
        private const val BATCH_SIZE = 16000
        private val MIGRATED_VERSION = "migrated_version"

        private val LAST_VALID_MIGRATE = "'-infinity'"

        val entitySetsToPropertyTypes = mapOf(
                "NCRICNotifications" to "general.datetime",
                "NCRICResultsIn" to "general.datetime",
                "NCRICVehicleRecords" to "ol.datelogged",
                "NCRICRecordedBy" to "ol.datelogged",
                "NCRICIncludes" to "date.completeddatetime"
        )
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }

    override fun upgrade(): Boolean {

        val entitySetsByName = toolbox.entitySets.values.associateBy { it.name }
        val propertyTypesByFqn = toolbox.propertyTypes.values.associate { it.type.fullQualifiedNameAsString to it.id }

        entitySetsToPropertyTypes.entries.stream().parallel().forEach {
            val entitySet = entitySetsByName.getValue(it.key)
            val propertyTypeId = propertyTypesByFqn.getValue(it.value)

            val entitySetName = it.key

            logger.info("About to update find ids needing updating from entity set $entitySetName")

            toolbox.hds.connection.use { conn ->
                conn.prepareStatement(createTempIdsTableSql(it.key)).use { ps ->
                    {
                        ps.setObject(1, entitySet.id)
                        ps.setObject(2, propertyTypeId)
                        ps.setArray(3, PostgresArrays.createIntArray(conn, entitySet.partitions))

                        ps.execute()
                    }
                }
            }

            logger.info("Finished identifying ids needing updating from entity set $entitySetName. About to update values.")

            try {
                var insertCounter = 0
                var insertCount = 1

                val readBatchSql = getReadBatchSql(it.key)

                while (insertCount > 0) {

                    toolbox.hds.connection.use { conn ->
                        conn.prepareStatement(UPDATE_SQL).use { ps ->

                            BasePostgresIterable(StatementHolderSupplier(toolbox.hds, readBatchSql)) { rs ->

                                val dateTimeObj = rs.getObject(DATETIME_COL)

                                ps.setObject(1, PostgresDataHasher.hashObject(dateTimeObj, EdmPrimitiveTypeKind.DateTimeOffset))
                                ps.setObject(2, entitySet.id)
                                ps.setObject(3, propertyTypeId)
                                ps.setInt(4, ResultSetAdapters.partition(rs))
                                ps.setObject(5, dateTimeObj)

                                ps.addBatch()
                            }


                            insertCount = ps.executeBatch().sum()
                            logger.info("Updated $insertCount property hashes for entity set $entitySetName.")
                            insertCounter += insertCount
                        }

                    }
                }

                logger.info("Finished migrating $insertCounter hashes for entity set $entitySetName")

            } catch (e: Exception) {
                logger.info("Something bad happened while updating entity set $entitySetName :(", e)
            }

        }

        return true
    }

    fun createTempIdsTableSql(entitySetName: String): String {
        val idsTable = "temp_ids_$entitySetName"

        return "CREATE TABLE $idsTable AS " +
                "SELECT ${PostgresColumn.ID.name}, ${PostgresColumn.PARTITION.name}, $DATETIME_COL, '-infinity'::timestamptz AS $MIGRATED_VERSION FROM ${DATA.name} " +
                "WHERE ${PostgresColumn.ENTITY_SET_ID.name} = ? " +
                "AND ${PostgresColumn.PROPERTY_TYPE_ID.name} = ? " +
                "AND ${PostgresColumn.PARTITION.name} = ANY(?) " +
                "AND $DATETIME_COL > '2018-12-17 00:00:00.000000-00' " + // this date was used as a lowerbound in the last migration (AdjustNcricDataDateTimes)
                "AND $DATETIME_COL < '2019-12-20 00:00:00.000000-00' " // the previous migration completed on 12/19/2019. Added a day as a buffer.
    }

    fun getReadBatchSql(entitySetName: String): String {
        val idsTable = "temp_ids_$entitySetName"

        return "UPDATE $idsTable SET $MIGRATED_VERSION = now() WHERE id IN (SELECT * FROM $idsTable WHERE $MIGRATED_VERSION < $LAST_VALID_MIGRATE LIMIT $BATCH_SIZE) RETURNING *"
    }

    val UPDATE_SQL = "UPDATE ${DATA.name} SET ${PostgresColumn.HASH.name} = ? " +
            "WHERE ${PostgresColumn.ENTITY_SET_ID.name} = ? " +
            "AND ${PostgresColumn.PROPERTY_TYPE_ID.name} = ? " +
            "AND ${PostgresColumn.PARTITION.name} = ? " +
            "AND $DATETIME_COL = ?"

}

