package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote

class AddPTTypeLastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    override fun upgrade(): Boolean {
        // ADD COLUMN WITH DEFAULT TO -infinity
        toolbox.hds.connection.use {conn ->
            conn.autoCommit = true
            toolbox.propertyTypes.keys.forEach {propertyTypeId ->
                conn.createStatement().use { statement ->
                    val table = quote(propertyTableName(propertyTypeId))
                    statement.execute(
                        "ALTER TABLE $table ADD COLUMN last_migrate timestamp with time zone NOT NULL DEFAULT '-infinity'::timestamptz"
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
