package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import org.slf4j.LoggerFactory

class RemoveDuplicates(private val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(RemoveDuplicates::class.java)
    }

    override fun upgrade(): Boolean {
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_02_25.value
    }

}