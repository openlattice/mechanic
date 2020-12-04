package com.openlattice.mechanic.upgrades

import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.PrincipalType
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.slf4j.LoggerFactory

class MigrateDbCredsKeyToAclKey(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(MigrateDbCredsKeyToAclKey::class.java)

        private const val OLD_TABLE_NAME = "db_creds_old"
    }

    override fun upgrade(): Boolean {

        val dbCredsMapstore = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast)

        renameOldTable()
        createNewTable()

        dbCredsMapstore.putAll(getNewKeys())

        return true
    }

    private fun renameOldTable() {
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("ALTER TABLE db_creds RENAME TO $OLD_TABLE_NAME")
            }
        }
    }

    private fun createNewTable() {
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(PostgresTable.DB_CREDS.createTableQuery())
            }
        }
    }

    private fun getAccountsByOldKey(): Map<String, MaterializedViewAccount> {

        return BasePostgresIterable(StatementHolderSupplier(toolbox.hds, "SELECT * FROM $OLD_TABLE_NAME")) { rs ->
            rs.getString(PostgresColumn.PRINCIPAL_ID.name) to ResultSetAdapters.materializedViewAccount(rs)
        }.toMap()
    }

    private fun getNewKeys(): Map<AclKey, MaterializedViewAccount> {

        val oldKeysToAccounts = getAccountsByOldKey()

        val principals = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).values.filter {
            it.principalType == PrincipalType.USER || it.principalType == PrincipalType.ROLE || it.principalType == PrincipalType.ORGANIZATION
        }.toList()

        return principals
                .filter {
                    it.principalType == PrincipalType.USER || it.principalType == PrincipalType.ROLE || it.principalType == PrincipalType.ORGANIZATION
                }
                .mapNotNull {
                    val principalId = PostgresRoles.buildExternalPrincipalId(it.aclKey, it.principalType)
                    if (!oldKeysToAccounts.containsKey(principalId)) {
                        logger.error("Could not find account info for principal {}", it)
                        return@mapNotNull null
                    }

                    it.aclKey to oldKeysToAccounts.getValue(principalId)
                }.toMap()
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }
}