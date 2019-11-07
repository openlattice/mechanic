package com.openlattice.mechanic.upgrades

import com.google.common.base.Preconditions
import com.openlattice.IdConstants
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.datastore.util.Util
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

class GrantPublicSchemaAccessToOrgs(
        private val toolbox: Toolbox,
        private val securePrincipalsManager: SecurePrincipalsManager) : Upgrade {
    private val acmConfig: AssemblerConfiguration
    private val PUBLIC_SCHEMA = "public"

    init {
        val acmConfig = ResourceConfigurationLoader.loadConfigurationFromResource("assembler.yaml", AssemblerConfiguration::class.java)
        this.acmConfig = acmConfig
    }

    override fun upgrade(): Boolean {
        val userNamesByDBName = HashMap<String, Set<String>>()
        BasePostgresIterable(
                StatementHolderSupplier(toolbox.hds, getOrgIdsQuery())
        ) { rs ->
            ResultSetAdapters.id(rs) to ResultSetAdapters.members(rs)
        }
                .filter { it.first != IdConstants.OPENLATTICE_ORGANIZATION_ID.id && it.first != IdConstants.GLOBAL_ORGANIZATION_ID.id }
                .forEach {
                    val dbName = PostgresDatabases.buildOrganizationDatabaseName(it.first)
                    val userNames = getUserNames(it.second)
                    userNamesByDBName[dbName] = userNames
                }
        userNamesByDBName.forEach { entry ->
            connect(entry.key, acmConfig.server.clone() as Properties, acmConfig.ssl).use { dataSource ->
                dataSource.connection.createStatement().use{stmt ->
                    stmt.executeQuery(getGrantOnPublicSchemaQuery(entry.value))
                }
            }
        }

        return true
    }


    override fun getSupportedVersion(): Long {
        return Version.V2019_11_07.value
    }

    private fun getOrgIdsQuery(): String {
        return "SELECT ${PostgresColumn.ID.name} from ${PostgresTable.ORGANIZATIONS}"
    }

    private fun getUserNames(principalIds: Set<String>): Set<String> {
        return principalIds.map {
            securePrincipalsManager.getPrincipal(it)
        }.filter {
            it.principalType == PrincipalType.USER
        }.map { DataTables.quote(PostgresRoles.buildPostgresUsername(it)) }.toSet()
    }

    private fun getGrantOnPublicSchemaQuery(userIds: Collection<String>): String {
        val userIdsSql = userIds.joinToString(", ")
        return "GRANT USAGE ON SCHEMA $PUBLIC_SCHEMA TO $userIdsSql"
    }

    private fun connect(dbName: String, config: Properties, useSsl: Boolean): HikariDataSource {
        config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
            "${(jdbcUrl as String).removeSuffix(
                    "/"
            )}/$dbName" + if (useSsl) {
                "?sslmode=require"
            } else {
                ""
            }
        }
        return HikariDataSource(HikariConfig(config))
    }
}