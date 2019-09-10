package com.openlattice.mechanic.upgrades


import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import java.sql.Types

class DataExpirationUpgrade(private val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {
        toolbox.hds.connection.use{
            val stmt = it.prepareStatement(addExpirationColumnsQuery())
        }
        return true;
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }

    private fun addExpirationColumnsQuery(): String {
        return "ALTER TABLE ${PostgresTable.ENTITY_SETS.name} " +
                "ADD COLUMN ${PostgresColumn.TIME_TO_EXPIRATION.name} bigint, " +
                "ADD COLUMN ${PostgresColumn.EXPIRATION_FLAG.name} text, " +
                "ADD COLUMN ${PostgresColumn.EXPIRATION_START_ID.name} uuid"
    }
}