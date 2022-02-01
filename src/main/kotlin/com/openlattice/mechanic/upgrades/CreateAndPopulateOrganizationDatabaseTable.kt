package com.openlattice.mechanic.upgrades

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.NAME
import com.openlattice.postgres.PostgresColumn.OID
import com.openlattice.postgres.PostgresTable.ORGANIZATION_DATABASES
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabaseType
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory

class CreateAndPopulateOrganizationDatabaseTable(
        private val toolbox: Toolbox,
        private val externalDatabaseConnectionManager: ExternalDatabaseConnectionManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CreateAndPopulateOrganizationDatabaseTable::class.java)
    }

    override fun upgrade(): Boolean {
        createTable()

        createTableEntries()

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_09_15.value
    }

    private fun createTable() {
        val createSql = ORGANIZATION_DATABASES.createTableQuery()

        logger.info("About to create table using SQL: $createSql")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createSql)
            }
        }

        logger.info("Finished creating table")
    }

    private fun connectToExternalDatabase(): HikariDataSource {
        return externalDatabaseConnectionManager.connectAsSuperuser()
    }

    private fun getDatabasesToOid(): Map<String, Long> {
        val lookupSql = "SELECT datname, oid FROM pg_database"

        logger.info("About to lookup databases to oids using sql: $lookupSql")

        val dbsToOids = BasePostgresIterable(StatementHolderSupplier(connectToExternalDatabase(), lookupSql)) {
            val dbName = it.getString(1)
            val oid = it.getLong(2)

            dbName to oid
        }.toMap()

        logger.info("Retrieved ${dbsToOids.size} database to oid mappings")

        return dbsToOids
    }

    private fun createTableEntries() {
        logger.info("About to create rows in org databases table using insert sql: $insertSql")

        val databaseNamesToOids = getDatabasesToOid()

        val orgIds = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).keys.toSet()

        toolbox.hds.connection.use { conn ->
            conn.prepareStatement(insertSql).use { ps ->

                orgIds.forEach { orgId ->
                    val dbName = ExternalDatabaseType.ORGANIZATION.generateName(orgId)
                    val oid = databaseNamesToOids.getOrDefault(dbName, -1)

                    ps.setObject(1, orgId)
                    ps.setLong(2, oid)
                    ps.setString(3, dbName)

                    ps.addBatch()
                }

                ps.executeBatch()
            }
        }

        logger.info("Finished writing rows into org databases table")
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) organization id
     * 2) oid
     * 3) database name
     */
    private val insertSql = """
        INSERT INTO ${ORGANIZATION_DATABASES.name} 
        (${ID.name}, ${OID.name}, ${NAME.name}) 
        VALUES (?, ?, ?)
    """.trimIndent()
}
