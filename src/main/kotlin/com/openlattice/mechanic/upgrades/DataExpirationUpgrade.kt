package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox

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
        return "ALTER TABLE entity_sets " +
                "ADD COLUMN time_to_expiration bigint, " +
                "ADD COLUMN expiration_flag text, " +
                "ADD COLUMN expiration_start_id uuid"
    }
}