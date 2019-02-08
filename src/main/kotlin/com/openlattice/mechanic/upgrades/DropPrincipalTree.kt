package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox

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