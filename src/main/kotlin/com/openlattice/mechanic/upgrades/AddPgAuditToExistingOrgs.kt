package mechanic.src.main.kotlin.com.openlattice.mechanic.upgrades

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.Upgrade
import com.openlattice.mechanic.upgrades.Version
import com.openlattice.organizations.Organization
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory

/**
 * @author Andrew Carter andrew@openlattice.com
 */

class AddPgAuditToExistingOrgs(
    private val toolbox: Toolbox,
    private val extDbConnMan: ExternalDatabaseConnectionManager

) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddPgAuditToExistingOrgs::class.java)
        private const val PGAUDIT_EXTENSION = "pgaudit"
        private const val OPENLATTICE_SCHEMA = "openlattice"
    }

    override fun upgrade(): Boolean {
        val allOrgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).values.toList()

        allOrgs.forEach { addPgAuditToOrg(it) }
        return true
    }

    private fun addPgAuditToOrg(org: Organization) {
        logger.info("Adding pgaudit for org: ${org.title} [${org.id}]")

        extDbConnMan.connectToOrg(org.id).let { hds ->
            hds.connection.use { connection ->
                connection.createStatement().use { stmt ->

                    // create pgaudit extension
                    stmt.execute(CREATE_EXTENSION_SQL)
                }
            }
        }
        logger.info("Finished adding pgaudit extension for org: ${org.title}")
    }

    private val CREATE_EXTENSION_SQL = "CREATE EXTENSION IF NOT EXISTS $PGAUDIT_EXTENSION SCHEMA $OPENLATTICE_SCHEMA"

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}