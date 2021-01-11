package com.openlattice.mechanic.upgrades

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory

class GrantCreateOnOLSchemaToOrgMembers(
        val toolbox: Toolbox,
        val externalDbConnMan: ExternalDatabaseConnectionManager,
        val securePrincipalsManager: SecurePrincipalsManager
): Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(GrantCreateOnOLSchemaToOrgMembers::class.java)
    }

    override fun upgrade(): Boolean {
        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()
        val dbCreds = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast).toMap()

        val memberUsernamesByOrg = securePrincipalsManager.getOrganizationMembers(orgs.keys).mapValues {
            it.value.mapNotNull { sp -> dbCreds["ol-internal|user|${sp.id}"]?.username }
        }

        orgs.forEach { (id, org) ->
            logger.info("About to grant create to members of org ${org.title} [$id]")

            val members = memberUsernamesByOrg[id] ?: listOf()

            if (members.isEmpty()) {
                logger.info("No members for org $id, continuing.")
                return@forEach
            }

            try {
                externalDbConnMan.connectToOrg(id).connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.execute(grantSql(members))
                    }
                }
            } catch (e: Exception) {
                logger.error("ERROR: unable to connect to org $id")
                return@forEach
            }

            logger.info("Finished org ${org.title} [$id]")
        }

        return true
    }

    private fun grantSql(usernames: List<String>): String {
        return "GRANT USAGE, CREATE ON SCHEMA openlattice TO ${usernames.joinToString()}"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }
}