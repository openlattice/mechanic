package com.openlattice.mechanic.upgrades

import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID
import com.openlattice.postgres.PostgresColumn.USERNAME
import com.openlattice.postgres.PostgresTable.DB_CREDS
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory

private const val ORGANIZATION_PREFIX = "ol-internal|organization|"

class AddDbCredUsernames(
        private val toolbox: Toolbox,
        assemblerConfiguration: AssemblerConfiguration,
        private val externalDbConMan: ExternalDatabaseConnectionManager = ExternalDatabaseConnectionManager(assemblerConfiguration, toolbox.hazelcast)
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddDbCredUsernames::class.java)
        private val USER_PREFIX = "user"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_09_15.value
    }

    override fun upgrade(): Boolean {
        addColumnToTable()

//        val userIdsToUsernames = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast).keys
//                .mapIndexed { index, userId -> userId to getUsername(userId, index) }
//                .toMap()

//        addUsernamesToTable(userIdsToUsernames)

//        updateExternalDatabaseUsernames(userIdsToUsernames)

        logger.info("Finished adding usernames to principal db creds")

        return true
    }

    private fun getUsername(userId: String, index: Int): String {
        if (userId.startsWith(ORGANIZATION_PREFIX)) {
            return userId
        }
        val unpaddedLength = (USER_PREFIX.length + index.toString().length)
        return if (unpaddedLength < 8) {
            "user" + ("0".repeat(8 - unpaddedLength)) + index
        } else {
            "user$index"
        }
    }

    private fun addColumnToTable() {
        logger.info("About to add username column using sql: {}", addUsernameColumnSql)

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(addUsernameColumnSql)
            }
        }

        logger.info("Finished adding column to table.")
    }

    private fun addUsernamesToTable(userIdsToUsernames: Map<String, String>) {
        logger.info("About to add usernames to db_creds table")

        val numUpdates = toolbox.hds.connection.use { conn ->
            conn.prepareStatement(addUsernameValueSql).use { ps ->

                userIdsToUsernames.forEach { (userId, username) ->
                    ps.setString(1, username)
                    ps.setString(2, userId)
                    ps.addBatch()
                }

                ps.executeBatch().sum()
            }
        }

        logger.info("Finished adding $numUpdates usernames to db_creds table")
    }

    private fun updateExternalDatabaseUsernames(userIdsToUsernames: Map<String, String>) {
        logger.info("About to update usernames in external database")

        val numUpdates = externalDbConMan.connect("postgres").connection.use { conn ->
            conn.createStatement().use { stmt ->

                userIdsToUsernames.map { (userId, username) ->
                    if (!userId.startsWith(ORGANIZATION_PREFIX)) {
                        stmt.executeUpdate(getUpdateRoleSql(userId, username))
                    } else {
                        0
                    }
                }.sum()

            }
        }

        logger.info("Finished updating $numUpdates usernames in external database")
    }

    private val addUsernameColumnSql = "ALTER TABLE ${DB_CREDS.name} ADD COLUMN IF NOT EXISTS ${USERNAME.sql()}"

    /**
     * Bind order for sql:
     *
     * 1. username
     * 2. user id
     */
    private val addUsernameValueSql = "UPDATE ${DB_CREDS.name} SET ${USERNAME.name} = ? WHERE ${PRINCIPAL_ID.name} = ?"

    private fun getUpdateRoleSql(userId: String, username: String): String {
        return "ALTER ROLE ${quote(userId)} RENAME TO $username"
    }
}