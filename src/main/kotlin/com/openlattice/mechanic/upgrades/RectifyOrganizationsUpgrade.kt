package com.openlattice.mechanic.upgrades

import com.openlattice.assembler.Assembler
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.transporter.types.TransporterDatastore
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
class RectifyOrganizationsUpgrade(
        private val toolbox: Toolbox,
        private val assembler: Assembler
) : Upgrade {

    private val logger = LoggerFactory.getLogger(RectifyOrganizationsUpgrade::class.java)

    private val connMan = toolbox.extDbConMan

    private val allSchemas = arrayOf(
            AssemblerConnectionManager.OPENLATTICE_SCHEMA,
            AssemblerConnectionManager.INTEGRATIONS_SCHEMA,
            AssemblerConnectionManager.STAGING_SCHEMA,
            TransporterDatastore.ORG_FOREIGN_TABLES_SCHEMA,
            TransporterDatastore.ORG_VIEWS_SCHEMA
    ).map {
        "CREATE SCHEMA IF NOT EXISTS $it"
    }

    override fun upgrade(): Boolean {
        return createOrgDbsIfNotExist()
    }

    private fun createOrgDbsIfNotExist(): Boolean {
        val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)
        val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(toolbox.hazelcast)
        organizations.keys.forEach { orgId ->
            var orgDs: HikariDataSource? = null
            try {
                // try connecting to the org db, creating a new org if it doesn't exist
                logger.info("Trying to connect to org database {}", orgId)
                orgDs = connMan.connectToOrg(orgId)
            } catch (ex: Exception) {
                val org = organizations[orgId]
                if (org == null) {
                    logger.error("No organization found for org ID {}", orgId)
                } else {
                    logger.info("Creating organization database for org ID {}", orgId)
                    val orgDatabase = assembler.createOrganizationAndReturnOid(orgId)
                    organizationDatabases.set(orgId, orgDatabase)
                    logger.info("Created org database {}", orgId)
                }
            }
            // create schemas for existent organizations
            if (orgDs != null) {
                orgDs.connection.use { connection ->
                    connection.createStatement().use { statement ->
                        val schemasCreated = allSchemas.sumBy {
                            if (statement.execute(it)) {
                                1
                            } else {
                                0
                            }
                        }
                        logger.info("{} schemas created in org {}", schemasCreated, orgId)
                    }
                }
            }
        }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }

}