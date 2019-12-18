package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory

const val DATETIME_COL = "n_timestamptz"

class AdjustNCRICDataDateTimes(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AdjustNCRICDataDateTimes::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_12_12.value
    }

    override fun upgrade(): Boolean {

        val entitySetsToPropertyTypes = mapOf(
                "NCRICNotifications" to "general.datetime",
                "NCRICResultsIn" to "general.datetime",
                "NCRICVehicleRecords" to "ol.datelogged",
                "NCRICRecordedBy" to "ol.datelogged",
                "NCRICIncludes" to "date.completeddatetime"
        )

        val rangeClausesToOffsets = mapOf(
                " AND $DATETIME_COL >= '2019-11-03 10:00:00.000000-00' " to 8, // PST
                " AND $DATETIME_COL >= '2019-03-11 10:00:00.000000-00' AND $DATETIME_COL < '2019-11-03 10:00:00.000000-00' " to 7, // PDT
                " AND $DATETIME_COL >= '2018-12-17 10:00:00.000000-00' AND $DATETIME_COL < '2019-03-11 10:00:00.000000-00' " to 8 // PST (and earlier data should be expired)
        )

        val entitySetsByName = toolbox.entitySets.values.associateBy { it.name }
        val propertyTypesByFqn = toolbox.propertyTypes.values.associate { it.type.fullQualifiedNameAsString to it.id }

        entitySetsToPropertyTypes.entries.stream().parallel().forEach {
            val entitySet = entitySetsByName.getValue(it.key)
            val propertyTypeId = propertyTypesByFqn.getValue(it.value)

            logger.info("About to update values for entity set ${it.key}")

            rangeClausesToOffsets.entries.stream().parallel().forEach { rangeEntry ->

                val rangeSql = getUpdateSql(rangeEntry.key, rangeEntry.value)
                logger.info("Range SQL for entity set ${entitySet.name}: $rangeSql")

                toolbox.hds.connection.use { conn ->
                    conn.prepareStatement(rangeSql).use { ps ->
                        ps.setObject(1, entitySet.id)
                        ps.setObject(2, propertyTypeId)
                        ps.setArray(3, PostgresArrays.createIntArray(conn, entitySet.partitions))

                        ps.execute()
                    }
                }

                logger.info("Updated range ${rangeEntry.key} for entity set ${entitySet.name}")
            }

            logger.info("Finished updating values for entity set ${it.key}")
        }

        return true
    }

    private fun getUpdateSql(rangeClause: String, offset: Int): String {
        return "UPDATE ${DATA.name} SET $DATETIME_COL = $DATETIME_COL + '$offset hours' " +
                "WHERE ${ENTITY_SET_ID.name} = ? " +
                "AND ${PROPERTY_TYPE_ID.name} = ? " +
                "AND ${PARTITION.name} = ANY(?)" +
                rangeClause
    }

}
