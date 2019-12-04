package com.openlattice.mechanic.upgrades


import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresDataTables
import org.slf4j.LoggerFactory

class CreateDataTableIndexes(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CreateDataTableIndexes::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            val tableDefinition = PostgresDataTables.buildDataTableDefinition()
            tableDefinition.createIndexQueries.forEach {indexSql ->
                val filteredIndexSql = indexSql.replace("CONCURRENTLY","")
                conn.createStatement().use { stmt ->
                    logger.info("Creating index with query {}", filteredIndexSql )
                    stmt.execute(indexSql)
                }
            }
        }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}

