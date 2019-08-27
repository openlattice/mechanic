package com.openlattice.mechanic.upgrades

import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory

class SetOriginIdToNonNullUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(Toolbox::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_08_26.value
    }

    override fun upgrade(): Boolean {
        val updateQuery = "UPDATE ${DATA.name} SET ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' WHERE ${ORIGIN_ID.name} = NULL"

        return toolbox.rateLimitedQuery( 16, updateQuery, logger)
    }

}
