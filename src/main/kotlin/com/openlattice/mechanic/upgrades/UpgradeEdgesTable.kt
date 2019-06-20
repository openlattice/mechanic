package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresTable
import org.slf4j.LoggerFactory

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class UpgradeEdgesTable(val toolbox: Toolbox) : Upgrade {
    companion object{
        private val logger = LoggerFactory.getLogger(UpgradeEdgesTable::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.createTable(PostgresTable.E)

        
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }



    private fun migrateEdgesTable() {

    }
}