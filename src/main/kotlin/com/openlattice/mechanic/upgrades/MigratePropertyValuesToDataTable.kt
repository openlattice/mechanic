package com.openlattice.mechanic.upgrades

import com.openlattice.data.storage.getColumnDefinition
import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

class MigratePropertyValuesToDataTable(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(MigratePropertyValuesToDataTable::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            toolbox.propertyTypes.entries.forEach { (propertyTypeId, propertyType) ->
                val insertSql = getInsertQuery( propertyType )
                val inserted = conn.createStatement().executeUpdate(insertSql)
                logger.info("Inserted {} properties into DATA table of type {} ({})", inserted, propertyType.type.fullQualifiedNameAsString, propertyTypeId)
            }
        }

        return true
    }

    fun getInsertQuery(propertyType: PropertyType): String {
        val col = getColumnDefinition(IndexType.NONE, propertyType.datatype)
        val selectCols = listOf(
                PostgresColumn.ENTITY_SET_ID.name,
                PostgresColumn.ID_VALUE.name,
                PostgresColumn.PARTITION.name,
                PostgresColumn.PROPERTY_TYPE_ID.name,
                PostgresColumn.HASH.name,
                DataTables.LAST_WRITE.name,
                PostgresColumn.LAST_PROPAGATE.name,
                PostgresColumn.LAST_MIGRATE.name,
                PostgresColumn.VERSION.name,
                PostgresColumn.VERSIONS.name,
                PostgresColumn.PARTITION_VERSION.name
        ).joinToString(",")
        val propertyTable = quote(propertyTableName(propertyType.id))
        val propertyColumn = quote( propertyType.type.fullQualifiedNameAsString )
        return "INSERT INTO ${DATA.name} (${PostgresDataTables.dataTableMetadataColumns},${col.name}) " +
                "SELECT $selectCols,$propertyColumn FROM $propertyTable INNER JOIN (select id as entity_set_id, partitions, partition_versions) as ${ENTITY_SETS.name} USING(entity_set_id)"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}
