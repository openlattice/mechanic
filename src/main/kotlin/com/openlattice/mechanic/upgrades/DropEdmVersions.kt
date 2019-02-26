package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox

class DropEdmVersions(private val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {
        return toolbox.hds.connection.use {
            it.prepareStatement("DROP TABLE edm_versions").execute()
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_12_21.value
    }
}