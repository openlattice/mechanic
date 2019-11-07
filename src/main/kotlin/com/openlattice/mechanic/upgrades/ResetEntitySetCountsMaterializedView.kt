package com.openlattice.mechanic.upgrades

import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask
import com.openlattice.mechanic.Toolbox
import org.slf4j.LoggerFactory

class ResetEntitySetCountsMaterializedView(val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(ResetEntitySetCountsMaterializedView::class.java)
    }

    override fun upgrade(): Boolean {

        logger.info("About to drop and recreate view ${PostgresEntitySetSizesInitializationTask.ENTITY_SET_SIZES_VIEW}.")

        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().executeUpdate( DROP_TABLE_SQL )
            conn.createStatement().executeUpdate( PostgresEntitySetSizesInitializationTask.CREATE_ENTITY_SET_COUNTS_VIEW )
            conn.commit()
        }

        logger.info("Finished recreating materialized view.")

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

    private val DROP_TABLE_SQL = "DROP MATERIALIZED VIEW IF EXISTS ${PostgresEntitySetSizesInitializationTask.ENTITY_SET_SIZES_VIEW}"

}