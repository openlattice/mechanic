package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import org.slf4j.LoggerFactory

class LastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        val logger = LoggerFactory.getLogger(LastMigrateColumnUpgrade::class.java)
    }

    override fun upgrade(): Boolean {

        // ADD COLUMN WITH DEFAULT TO -infinity
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { statement ->
                toolbox.propertyTypes.keys.forEach { propertyTypeId ->
                    val table = quote(propertyTableName(propertyTypeId))
                    statement.execute(
                            "ALTER TABLE $table ADD COLUMN if not exists last_migrate timestamp with time zone NOT NULL DEFAULT '-infinity'"
                    )
                    logger.info("Upgraded table pt_$propertyTypeId")
                }
            }
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_14.value
    }
}
