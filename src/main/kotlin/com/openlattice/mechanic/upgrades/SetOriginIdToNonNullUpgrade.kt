package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox

class SetOriginIdToNonNullUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(Toolbox::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_08_26.value
    }

    override fun upgrade(): Boolean {
        val updateQuery = "UPDATE ${DATA.name} SET ${ORIGIN_ID.name} = ${IdConstants.EMPTY_UUID}"

        toolbox.rateLimitedQuery( 1, updateQuery, logger)
    }

}
