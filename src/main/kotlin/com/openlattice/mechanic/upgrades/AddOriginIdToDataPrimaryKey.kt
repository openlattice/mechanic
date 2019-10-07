package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory

class AddOriginIdToDataPrimaryKey(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddOriginIdToDataPrimaryKey::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

    override fun upgrade(): Boolean {

        val pkey = (PostgresDataTables.buildDataTableDefinition().primaryKey + ORIGIN_ID).joinToString(",") { it.name }

        val dropPkey = "ALTER TABLE ${DATA.name} DROP CONSTRAINT ${DATA.name}_pkey"
        val updatePkey = "ALTER TABLE ${DATA.name} ADD PRIMARY KEY ($pkey)"

        logger.info("About to drop and recreate primary key of data.")

        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().executeUpdate( dropPkey )
            conn.createStatement().executeUpdate( updatePkey )
            conn.commit()
        }

        logger.info("Finished updating primary key.")

        return true
    }
}
