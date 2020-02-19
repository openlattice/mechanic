package com.openlattice.mechanic.upgrades

import com.dataloom.mappers.ObjectMappers
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.APPS
import com.openlattice.postgres.PostgresTable.APP_CONFIGS
import com.openlattice.postgres.PostgresTableDefinition
import org.slf4j.LoggerFactory

class UpdateAppTables(private val toolbox: Toolbox) : Upgrade {

    override fun upgrade(): Boolean {

        renameAndRecreateTable(APPS)

        renameAndRecreateTable(APP_CONFIGS)

        return true
    }

    companion object {
        private val logger = LoggerFactory.getLogger(UpdateAppTables::class.java)
    }

    private fun renameAndRecreateTable(table: PostgresTableDefinition) {
        val renameSql = "ALTER TABLE ${table.name} RENAME TO ${table.name}_legacy"
        val createSql = table.createTableQuery()

        logger.info("About to rename ${table.name} table and create new one")

        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->

                stmt.execute(renameSql)

                stmt.execute(createSql)

            }
        }

        logger.info("Finished updating ${table.name} table")
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }
}