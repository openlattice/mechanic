package com.openlattice.mechanic.upgrades

import com.openlattice.IdConstants
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.organizations.PrincipalSet
import com.openlattice.organizations.mapstores.OrganizationsMapstore
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(GrantPublicSchemaAccessToOrgs::class.java)

class GrantPublicSchemaAccessToOrgs(
        private val organizationsMapstore: OrganizationsMapstore,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val acmConfig: AssemblerConfiguration) : Upgrade {

    companion object {
        private const val BATCH_SIZE = 1000
        private const val PUBLIC_SCHEMA = "public"
    }

    override fun upgrade(): Boolean {
        organizationsMapstore.loadAllKeys()
                .filter { it != IdConstants.GLOBAL_ORGANIZATION_ID.id }
                .asSequence()
                .chunked(BATCH_SIZE)
                .forEach {
                    organizationsMapstore.loadAll(it).forEach { (orgId, organization) ->
                        grantUsageOnPublicSchema(orgId, organization.members)
                    }
                }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_11_21.value
    }

    private fun grantUsageOnPublicSchema(orgId: UUID, principals: Set<Principal>) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val userNames = getUserNames(principals)
        logger.info("granting access to public schema")
        connect(dbName, acmConfig.server.clone() as Properties, acmConfig.ssl).use { dataSource ->
            dataSource.connection.createStatement().use { stmt ->
                stmt.execute(getGrantOnPublicSchemaQuery(userNames))
            }
        }
    }

    private fun getUserNames(principals: Set<Principal>): Set<String> {
        logger.info("getting user names")
        return principals.asSequence().filter {
            it.id != "openlatticeRole"
        }.map {
            securePrincipalsManager.getPrincipal(it.id)
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