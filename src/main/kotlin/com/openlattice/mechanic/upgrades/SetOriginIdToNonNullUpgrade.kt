package com.openlattice.mechanic.upgrades

import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory

class SetOriginIdToNonNullUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(SetOriginIdToNonNullUpgrade::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

    override fun upgrade(): Boolean {

        val renameOriginIdCol = "ALTER TABLE ${DATA.name} RENAME COLUMN ${ORIGIN_ID.name} TO origin_id_old"
        val addOriginIdCol = "ALTER TABLE ${DATA.name} ADD COLUMN ${ORIGIN_ID.sql()}"


        logger.info("About to rename origin_id and add new origin_id column with default value.")

        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().executeUpdate( renameOriginIdCol )
            conn.createStatement().executeUpdate( addOriginIdCol )
            conn.commit()
        }

        logger.info("Finished adding new origin_id column.")

        return true

    }
}
