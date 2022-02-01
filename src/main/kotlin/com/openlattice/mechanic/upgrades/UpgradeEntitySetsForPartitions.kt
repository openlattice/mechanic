package com.openlattice.mechanic.upgrades


import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import com.geekbeast.postgres.IndexType
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.slf4j.LoggerFactory

class UpgradeEntitySetsForPartitions(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(UpgradeEntitySetsForPartitions::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            toolbox.propertyTypes.entries.forEach { (propertyTypeId, propertyType) ->
                val insertSql = getInsertQuery(propertyType)
                val inserted = conn.createStatement().executeUpdate(insertSql)
                logger.info(
                        "Inserted {} properties into DATA table of type {} ({})",
                        inserted,
                        propertyType.type.fullQualifiedNameAsString,
                        propertyTypeId
                )

            }
        }

        return true
    }

    fun getInsertQuery(propertyType: PropertyType): String {
        val col = getColumnDefinition(IndexType.NONE, propertyType.datatype)
        val selectCols = PostgresDataTables
                .dataTableMetadataColumns
                .filter { it != ORIGIN_ID }
                .joinToString(",")
        val propertyTable = quote(propertyTableName(propertyType.id))
        val propertyColumn = quote(propertyType.type.fullQualifiedNameAsString)
        return "INSERT INTO ${DATA.name} ($selectCols,${col.name}) " +
                "SELECT $selectCols,$propertyColumn FROM $propertyTable INNER JOIN (select id as entity_set_id, partitions, partition_versions) as ${ENTITY_SETS.name} USING(entity_set_id)"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}
