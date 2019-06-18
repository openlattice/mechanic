package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote

class LastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    override fun upgrade(): Boolean {
        // ADD COLUMN WITH DEFAULT TO -infinity
        toolbox.hds.connection.use {conn ->
            conn.autoCommit = true
            toolbox.propertyTypes.keys.forEach {propertyTypeId ->
                val propertyTable = quote(propertyTableName(propertyTypeId))
                conn.createStatement().use { statement ->
                    statement.execute(
                        "ALTER TABLE $propertyTable ADD COLUMN last_migrate timestamp with time zone NOT NULL DEFAULT '-infinity'"
                    )
                }
            }
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_14.value
    }
}
