package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresTable

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class UpgradeEdgesTable(val toolbox: Toolbox) : Upgrade {
    companion object{
        private val logger = LoggerFactory.getLogger(UpgradeEdgesTable::class.java)
    }
    override fun upgrade(): Boolean {

    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }

    private fun createEdgesTable() {
        toolbox.hds.connection.use { conn ->
            logger.info("Creating new edges table.")
            conn.createStatement().execute( PostgresTable.E.createTableQuery() );
        }
    }

    private fun migrateEdgesTable() {

    }
}