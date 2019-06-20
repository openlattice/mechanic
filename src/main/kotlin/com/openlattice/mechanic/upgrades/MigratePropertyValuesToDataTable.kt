package com.openlattice.mechanic.upgrades


import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.slf4j.LoggerFactory
import java.util.*

class MigratePropertyValuesToDataTable(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private var THE_BIG_ONE = 0L
        private var PRIMEY_BOI = 127
        private val logger = LoggerFactory.getLogger(MigratePropertyValuesToDataTable::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            toolbox.entityTypes
                    .getValue(UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab")).properties //Only general.person
                    .associateWith { toolbox.propertyTypes.getValue(it) }
//            toolbox.propertyTypes.entries
                    .forEach { (propertyTypeId, propertyType) ->
                        val markSql = markAsMigrated(propertyType)
                        val insertSql = getInsertQuery(propertyType)
                        logger.info("Mark SQL: {}", markSql)
                        logger.info("Insert SQL: {}", insertSql)
                        val marked = conn.createStatement().executeUpdate(markSql)
                        logger.info(
                                "Marked {} properties as migrated in table of type {} ({}",
                                marked,
                                propertyType.type.fullQualifiedNameAsString,
                                propertyTypeId
                        )

                        val inserted = conn.createStatement().executeUpdate(insertSql)

                        logger.info(
                                "Inserted {} properties into DATA table of type {} ({})",
                                inserted,
                                propertyType.type.fullQualifiedNameAsString,
                                propertyTypeId
                        )
                        conn.commit()
                    }
            conn.autoCommit = true
        }
        return true
    }

    private fun markAsMigrated(propertyType: PropertyType): String {
        val propertyTable = quote(propertyTableName(propertyType.id))
        return "UPDATE $propertyTable SET ${PostgresColumn.LAST_MIGRATE.name} = now()"
    }

    private fun getInsertQuery(propertyType: PropertyType): String {
        val col = getColumnDefinition(IndexType.NONE, propertyType.datatype)
        val selectCols = PostgresDataTables.dataTableMetadataColumns.joinToString(",") { it.name }
        val propertyTable = quote(propertyTableName(propertyType.id))
        val propertyColumn = quote(propertyType.type.fullQualifiedNameAsString)
        return "INSERT INTO ${DATA.name} (${PostgresDataTables.dataTableMetadataColumns},${col.name}) " +
                "SELECT $selectCols,$propertyColumn, partitions[ 1 + (('x'||right(id::text,8))::bit(32)::int % array_length(partitions,1))] as partition" +
                "FROM $propertyTable INNER JOIN (select id as entity_set_id, partitions, partitions_versions from ${ENTITY_SETS.name}) as entity_set_partitions USING(entity_set_id)"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}

