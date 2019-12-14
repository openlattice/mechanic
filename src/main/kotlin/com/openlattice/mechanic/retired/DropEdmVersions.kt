package com.openlattice.mechanic.retired

import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.Version

class DropEdmVersions(private val toolbox: Toolbox) : Retiree {
    override fun retire(): Boolean {
        return toolbox.hds.connection.use {
            it.prepareStatement("DROP TABLE edm_versions").execute()
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_12_21.value
    }
}