package com.openlattice.mechanic.upgrades

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory

class AlterAtlasUsersWithInherit(
        private val toolbox: Toolbox,
        private val externalDbConnMan: ExternalDatabaseConnectionManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AlterAtlasUsersWithInherit::class.java)
    }

    override fun upgrade(): Boolean {
        logger.info("About up alter atlas accounts with inherit")

        externalDbConnMan.connectAsSuperuser().connection.use { conn ->
            conn.createStatement().use { stmt ->
                HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast).map { it.value.username }.toList().forEach { roleName ->
                    try {
                        stmt.execute(alterRoleStmt(roleName))
                    } catch (e: Exception) {
                        logger.error("Unable to alter account {}", roleName)
                    }
                }
            }
        }

        logger.info("Finished altering atlas accounts with inherit")
        return true
    }

    private fun alterRoleStmt(roleName: String): String {
        return "ALTER ROLE ${quote(roleName)} WITH INHERIT"
    }


    override fun getSupportedVersion(): Long {
        return Version.V2021_02_14.value
    }
}