package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.APPS

class UpdateAppConfigs(private val toolbox: Toolbox) : Upgrade {

    override fun upgrade(): Boolean {
        alterAppsTable()

        return true
    }

    private fun alterAppsTable() {
        val sql = "ALTER TABLE ${APPS.name} " +
                "ADD COLUMN ${ENTITY_TYPE_COLLECTION_ID.sql()}, " +
                "ADD COLUMN ${ROLES.sql()}, " +
                "ADD COLUMN ${SETTINGS.sql()}"

        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->
                stmt.execute(sql)
            }
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }
}