package com.openlattice.mechanic.upgrades


import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.CitusDistributedTableDefinition
import com.openlattice.postgres.PostgresDataTables
import org.slf4j.LoggerFactory

class UpgradeCreateDataTable(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(UpgradeCreateDataTable::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            val tableDefinition = PostgresDataTables.buildDataTableDefinition()
            conn.createStatement().use { stmt ->
                logger.info("Creating the data table.")
                stmt.execute(tableDefinition.createTableQuery())
            }
            tableDefinition.createIndexQueries.forEach {
                conn.createStatement().use { stmt ->
                    logger.info("Creating index with query {}", it)
                    stmt.execute(it)
                }
            }

            conn.createStatement().use { stmt ->
                logger.info("Making table a distributed table.")
                stmt.execute((tableDefinition as CitusDistributedTableDefinition).createDistributedTableQuery())
            }
        }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}

