package com.openlattice.mechanic.upgrades

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import org.slf4j.LoggerFactory

class ClearJSONOrganizationRoles(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(ClearJSONOrganizationRoles::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_01_29.value
    }

    override fun upgrade(): Boolean {

        val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

        logger.info("About to clear roles for organizations")

        organizations.executeOnEntries(OrganizationEntryProcessor {
            it.roles.clear()
        })

        logger.info("Finished clearing roles for organizations")

        return true
    }
}

