package com.openlattice.mechanic.upgrades


import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable

class DataExpirationUpgrade(private val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {
        toolbox.hds.connection.use {
            val stmt = it.prepareStatement(addExpirationColumnsQuery())
            stmt.execute()
        }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

    private fun addExpirationColumnsQuery(): String {
        return "ALTER TABLE ${PostgresTable.ENTITY_SETS.name} " +
                "ADD COLUMN ${PostgresColumn.TIME_TO_EXPIRATION.name} ${PostgresColumn.TIME_TO_EXPIRATION.datatype}, " +
                "ADD COLUMN ${PostgresColumn.EXPIRATION_BASE_FLAG.name} ${PostgresColumn.EXPIRATION_BASE_FLAG.datatype}, " +
                "ADD COLUMN ${PostgresColumn.EXPIRATION_DELETE_FLAG.name} ${PostgresColumn.EXPIRATION_DELETE_FLAG.datatype}, " +
                "ADD COLUMN ${PostgresColumn.EXPIRATION_START_ID.name} ${PostgresColumn.EXPIRATION_START_ID.datatype}"
    }
}