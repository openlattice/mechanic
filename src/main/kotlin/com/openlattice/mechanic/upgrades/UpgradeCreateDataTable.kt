package com.openlattice.mechanic.upgrades


import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.CitusDistributedTableDefinition
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.slf4j.LoggerFactory

class UpgradeCreateDataTable(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(UpgradeCreateDataTable::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                val tableDefinition = PostgresDataTables.buildDataTableDefinition()
                stmt.execute(tableDefinition.createTableQuery())
                tableDefinition.createIndexQueries.forEach {
                    logger.info("Creating index with query {}", it)
                    stmt.execute(it)
                }
                
                stmt.execute((tableDefinition as CitusDistributedTableDefinition).createDistributedTableQuery())
            }
        }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}

