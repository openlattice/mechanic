package mechanic.src.main.kotlin.com.openlattice.mechanic.upgrades

import com.openlattice.authorization.Principal
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.CreateStagingSchemaForExistingOrgs
import com.openlattice.mechanic.upgrades.Upgrade
import com.openlattice.mechanic.upgrades.Version
import com.openlattice.organizations.Organization
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.Schemas
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
        val allPrincipalsToDbIds = getAllUserPrincipals()

        allOrgs.forEach { addPgAuditToOrg(it, allPrincipalsToDbIds) }
        return true
    }

    private fun getAllUserPrincipals(): Map<Principal, String> {
        return HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).values.associate {
            it.principal to DataTables.quote("ol-internal|user|${it.id}")
        }
    }

    private fun addPgAuditToOrg(org: Organization, allPrincipals: Map<Principal, String>) {
        logger.info("Adding pg audit for org: ${org.title} [${org.id}]")
        logger.info("Executing statement: ${CREATE_EXTENSION_SQL}")

        extDbConnMan.connectToOrg(org.id).let { hds ->
            hds.connection.use { connection ->
                connection.createStatement().use { stmt ->

                    // create pgaudit extension
                    stmt.execute(CREATE_EXTENSION_SQL)
                }
            }
        }
        logger.info("Finished creating and configuring staging schema for org ${org.title}")
    }

    private val CREATE_EXTENSION_SQL = "CREATE EXTENSION IF NOT EXISTS $PGAUDIT_EXTENSION SCHEMA $OPENLATTICE_SCHEMA"

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}