package com.openlattice.mechanic.retired

import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.Upgrade
import com.openlattice.mechanic.upgrades.Version

class DropPrincipalTree(private val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {
        return toolbox.hds.connection.use {
            it.prepareStatement("DROP TABLE principal_tree").execute()
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_12_21.value
    }
}