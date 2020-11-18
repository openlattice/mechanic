package com.openlattice.mechanic.upgrades

import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory
import java.util.*

class GrantAllOnStagingSchemaToOrgUser(
        private val toolbox: Toolbox,
        private val externalDatabaseConnectionManager: ExternalDatabaseConnectionManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(GrantAllOnStagingSchemaToOrgUser::class.java)
    }

    override fun upgrade(): Boolean {

        logger.info("About to grant all on staging schema to org users")

        val organizationIds = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).keys.toSet()
        val dbCreds = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast).toMap()

        val orgToGrantSql = organizationIds
                .associateWith { grantAllSql(it, dbCreds) }
                .filter { it.value.isNotBlank() }

        orgToGrantSql.forEach { (orgId, grantSql) ->
            logger.info("About to grant all for organization {} using sql: ", orgId, grantSql)

            try {
                externalDatabaseConnectionManager.connectToOrg(orgId).connection.use { conn ->
                    conn.createStatement().execute(grantSql)
                }
            } catch (e: Exception) {
                logger.error("Unable to connect to database for organization {}", orgId, e)
            }

            logger.info("Finished granting all for organization {}", orgId)
        }


        logger.info("Finished granting all on staging schema to org users")

        return true
    }

    private fun grantAllSql(orgId: UUID, dbCreds: Map<String, MaterializedViewAccount>): String {
        val lookupKey = "ol-internal|organization|$orgId"
        val username = dbCreds[lookupKey]?.username ?: return ""

        return "ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL PRIVILEGES ON TABLES TO ${quote(username)}"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }

}