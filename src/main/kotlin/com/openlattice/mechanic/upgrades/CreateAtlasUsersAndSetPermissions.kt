package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.MEMBER_ORG_DATABASE_PERMISSIONS
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.Organization
import com.openlattice.postgres.DataTables
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

class CreateAtlasUsersAndSetPermissions(
        private val toolbox: Toolbox,
        private val assemblerConfiguration: AssemblerConfiguration,
        private val authorizationManager: AuthorizationManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CreateAtlasUsersAndSetPermissions::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_09_15.value
    }

    override fun upgrade(): Boolean {
        val dbCreds = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast).toMap()
        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()

        val userPrincipalsToAccounts = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast)
                .values(Predicates.equal(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.USER))
                .toSet()
                .associate {
                    val dbUserId = buildPostgresUsername(it)
                    it.principal to dbCreds.getValue(dbUserId)
                }

        configureUsersInAtlas(userPrincipalsToAccounts.values)

        orgs.values.forEach { configureUsersInOrganization(it, userPrincipalsToAccounts) }

        return true
    }


    private fun connectToOrgDatabase(org: Organization): HikariDataSource {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(org.id)
        return connectToExternalDatabase(dbName)
    }

    private fun connectToExternalDatabase(dbName: String = "postgres"): HikariDataSource {
        return AssemblerConnectionManager.createDataSource(
                dbName,
                assemblerConfiguration.server.clone() as Properties,
                assemblerConfiguration.ssl
        )
    }

    private fun configureUsersInAtlas(userMVAccounts: Collection<MaterializedViewAccount>) {
        logger.info("About to create users in external database")

        val numUpdates = connectToExternalDatabase().connection.use { conn ->
            conn.createStatement().use { stmt ->

                userMVAccounts.map {
                    stmt.executeUpdate(createUserIfNotExistsSql(it.username, it.credential))
                }.sum()

            }
        }

        logger.info("Finished creating $numUpdates users in external database")
    }

    private fun configureUsersInOrganization(organization: Organization, userPrincipalsToAccounts: Map<Principal, MaterializedViewAccount>) {
        val userIds = organization.members.mapNotNull { userPrincipalsToAccounts[it] }
        val userIdsSql = userIds.joinToString(", ")

        logger.info("Configuring users $userIds in organization ${organization.title}")

        val dbName = PostgresDatabases.buildOrganizationDatabaseName(organization.id)
        val grantDefaultPermissionsOnDatabaseSql = "GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                "ON DATABASE ${DataTables.quote(dbName)} TO $userIdsSql"
        val grantOLSchemaPrivilegesSql = "GRANT USAGE ON SCHEMA ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA} TO $userIdsSql"
        val grantStagingSchemaPrivilegesSql = "GRANT USAGE, CREATE ON SCHEMA ${AssemblerConnectionManager.STAGING_SCHEMA} TO $userIdsSql"

        connectToOrgDatabase(organization).connection.use { connection ->
            connection.createStatement().use { statement ->

                statement.execute(grantDefaultPermissionsOnDatabaseSql)
                statement.execute(grantOLSchemaPrivilegesSql)
                statement.execute(grantStagingSchemaPrivilegesSql)

                userIds.forEach { userId -> statement.addBatch(setSearchPathSql(userId)) }
                statement.executeBatch()
            }
        }
    }

    private fun setSearchPathSql(granteeId: String): String {
        val searchPathSchemas = listOf(
                AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA,
                AssemblerConnectionManager.STAGING_SCHEMA
        )
        return "ALTER USER $granteeId SET search_path TO ${searchPathSchemas.joinToString()}"
    }


    private fun createUserIfNotExistsSql(dbUser: String, dbUserPassword: String): String {
        return "DO\n" +
                "\$do\$\n" +
                "BEGIN\n" +
                "   IF NOT EXISTS (\n" +
                "      SELECT\n" +
                "      FROM   pg_catalog.pg_roles\n" +
                "      WHERE  rolname = '$dbUser') THEN\n" +
                "\n" +
                "      CREATE ROLE ${
                    DataTables.quote(
                            dbUser
                    )
                } NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN ENCRYPTED PASSWORD '$dbUserPassword';\n" +
                "   END IF;\n" +
                "END\n" +
                "\$do\$;"
    }
}