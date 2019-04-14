package com.openlattice.mechanic.upgrades

import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

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