package com.openlattice.mechanic.upgrades

import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory


class SetOriginIdDefaultValueUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(SetOriginIdDefaultValueUpgrade::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_08_26.value
    }

    override fun upgrade(): Boolean {

        val updateDefaultValue = "ALTER TABLE ${DATA.name} ALTER COLUMN ${ORIGIN_ID.name} SET DEFAULT '${IdConstants.EMPTY_ORIGIN_ID.id}'"
        toolbox.hds.connection.use { conn ->
            conn.createStatement().executeUpdate( updateDefaultValue )
            return true
        }
    }

}
