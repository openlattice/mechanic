package com.openlattice.mechanic.upgrades


import com.openlattice.data.storage.getColumnDefinition
import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.slf4j.LoggerFactory

class MigratePropertyValuesToDataTable(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(MigratePropertyValuesToDataTable::class.java)
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

    private val GET_ENTITY_SET_COUNT = "SELECT count FROM entity_set_counts WHERE ${ENTITY_SET_ID.name} = ?"

    fun generatePartitionIDs() {
        toolbox.entitySets.keys.map{ esid ->
            toolbox.hds.connection.use { conn ->
                conn.prepareStatement(GET_ENTITY_SET_COUNT ).use { stmt ->
                    stmt.setObject(0, esid)
                    stmt.executeQuery().use { rs ->
                    }
                }
            }
        }
    }

    fun getInsertQuery(propertyType: PropertyType): String {
        val col = getColumnDefinition(IndexType.NONE, propertyType.datatype)
        val selectCols = PostgresDataTables.dataTableMetadataColumns.joinToString(",")
        val propertyTable = quote(propertyTableName(propertyType.id))
        val propertyColumn = quote(propertyType.type.fullQualifiedNameAsString)
        return "INSERT INTO ${DATA.name} (${PostgresDataTables.dataTableMetadataColumns},${col.name}) " +
                "SELECT $selectCols,$propertyColumn FROM $propertyTable INNER JOIN (select id as entity_set_id, partitions, partition_versions) as ${ENTITY_SETS.name} USING(entity_set_id)"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}
