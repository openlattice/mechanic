package com.openlattice.mechanic.upgrades

import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.quote
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

private const val ORGANIZATION_PREFIX = "ol-internal|organization|"

class SetPasswordDbCredUsernames(
        private val toolbox: Toolbox,
        private val assemblerConfiguration: AssemblerConfiguration
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(SetPasswordDbCredUsernames::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_09_15.value
    }

    override fun upgrade(): Boolean {
        val userIdsToUsernames = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast).entries
                .asSequence()
                .filter { !it.key.startsWith(ORGANIZATION_PREFIX) }
                .map { it.value.username to it.value.credential }
                .toMap()

        logger.info("Loaded ${userIdsToUsernames.size} users to set password or.")

        updateExternalDatabasePasswords(userIdsToUsernames)

        logger.info("Finished setting passwords for usernames in db creds")

        return true
    }

    private fun connectToExternalDatabase(): HikariDataSource {
        return AssemblerConnectionManager.createDataSource(
                "postgres",
                assemblerConfiguration.server.clone() as Properties,
                assemblerConfiguration.ssl
        )
    }


    private fun updateExternalDatabasePasswords(userIdsToUsernames: Map<String, String>) {
        logger.info("About to update usernames in external database")

        val numUpdates = connectToExternalDatabase().connection.use { conn ->
            conn.createStatement().use { stmt ->

                userIdsToUsernames.map { (username, password) ->

                    logger.info("Resetting password for $username")
                    stmt.executeUpdate(setPasswordIfExists(username, password))

                }.sum()

            }
        }

        logger.info("Finished updating $numUpdates usernames in external database")
    }

    private fun setPasswordIfExists(username: String, password: String): String {
        return "DO\n" +
                "\$do\$\n" +
                "BEGIN\n" +
                "   IF EXISTS (\n" +
                "      SELECT\n" +
                "      FROM   pg_catalog.pg_roles\n" +
                "      WHERE  rolname = '$username') THEN\n" +
                "\n" +
                "      ALTER ROLE ${quote(username)} WITH PASSWORD '$password';\n" +
                "   END IF;\n" +
                "END\n" +
                "\$do\$;"

    }
}