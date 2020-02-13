package com.openlattice.mechanic.upgrades

import com.dataloom.mappers.ObjectMappers
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.APPS
import com.openlattice.postgres.PostgresTable.APP_CONFIGS
import org.slf4j.LoggerFactory

class UpdateAppTables(private val toolbox: Toolbox) : Upgrade {

    override fun upgrade(): Boolean {
        alterAppsTable()

        alterAppConfigsTable()

        return true
    }

    companion object {
        private val logger = LoggerFactory.getLogger(UpdateAppTables::class.java)
    }

    private fun alterAppsTable() {
        val sql = "ALTER TABLE ${APPS.name} " +
                "ADD COLUMN ${ENTITY_TYPE_COLLECTION_ID.sql()}, " +
                "ADD COLUMN ${ROLES.sql()}, " +
                "ADD COLUMN ${SETTINGS.sql()}"

        logger.info("About to add columns to apps table using: $sql")

        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->

                stmt.execute(sql)

            }
        }

        logger.info("Finished adding columns to apps table.")
    }

    private fun alterAppConfigsTable() {

        val renameSql = "ALTER TABLE ${APP_CONFIGS.name} RENAME TO ${APP_CONFIGS.name}_legacy"
        val createSql = APP_CONFIGS.createTableQuery()

        logger.info("About to rename app_configs table and create new one")

        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->

                stmt.execute(renameSql)

                stmt.execute(createSql)

            }
        }

        logger.info("Finished updating app_configs table")

    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }
}