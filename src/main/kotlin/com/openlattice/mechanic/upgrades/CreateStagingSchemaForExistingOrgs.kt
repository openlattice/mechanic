package com.openlattice.mechanic.upgrades

import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.Principal
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.Organization
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory

class CreateStagingSchemaForExistingOrgs(
        private val toolbox: Toolbox,
        private val assemblerConfiguration: AssemblerConfiguration,
        private val externalDatabaseConnectionManager: ExternalDatabaseConnectionManager
) : Upgrade {

    companion object {
        private const val STAGING_SCHEMA = "staging"
        private val logger = LoggerFactory.getLogger(CreateStagingSchemaForExistingOrgs::class.java)
    }

    override fun upgrade(): Boolean {
        val allOrgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).values.toList()
        val allPrincipalsToDbIds = getAllUserPrincipals()

        allOrgs.forEach { createAndConfigureSchemaForOrg(it, allPrincipalsToDbIds) }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_06_11.value
    }

    private fun getAllUserPrincipals(): Map<Principal, String> {
        return HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast).values.associate {
            it.principal to DataTables.quote("ol-internal|user|${it.id}")
        }
    }

    private fun createAndConfigureSchemaForOrg(org: Organization, allPrincipals: Map<Principal, String>) {
        logger.info("About to create staging schema for org ${org.title} [${org.id}]")

        val members = org.members.mapNotNull { allPrincipals[it] } + DataTables.quote(PostgresRoles.buildOrganizationUserId(org.id))

        connectToDatabase(org).let { hds ->
            hds.connection.use { connection ->
                connection.createStatement().use { stmt ->

                    // create schema
                    stmt.execute(CREATE_SCHEMA_SQL)

                    // grant usage, create on schema to members (including org user)
                    stmt.execute(grantPrivilegesSql(members.joinToString()))

                    // update search path for users (including org user) to include staging schema
                    members.forEach {
                        stmt.execute(setSearchPathSql(
                                it,
                                true,
                                AssemblerConnectionManager.OPENLATTICE_SCHEMA,
                                STAGING_SCHEMA
                        ))
                    }

                    // update search path for server user to include staging schema
                    stmt.execute(setSearchPathSql(
                            assemblerConfiguration.server["username"].toString(),
                            false,
                            AssemblerConnectionManager.INTEGRATIONS_SCHEMA,
                            AssemblerConnectionManager.OPENLATTICE_SCHEMA,
                            AssemblerConnectionManager.PUBLIC_SCHEMA,
                            STAGING_SCHEMA
                    ))
                }
            }
        }

        logger.info("Finished creating and configuring staging schema for org ${org.title}")
    }

    private fun connectToDatabase(org: Organization): HikariDataSource {
        return externalDatabaseConnectionManager.connect(
                ExternalDatabaseConnectionManager.buildDefaultOrganizationDatabaseName(org.id)
        )
    }


    private val CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS $STAGING_SCHEMA"

    private fun grantPrivilegesSql(userIdsSql: String): String {
        return "GRANT USAGE, CREATE ON SCHEMA $STAGING_SCHEMA TO $userIdsSql"
    }

    private fun setSearchPathSql(granteeId: String, isUser: Boolean, vararg schemas: String): String {
        val granteeType = if (isUser) "USER" else "ROLE"
        return "ALTER $granteeType $granteeId SET search_path TO ${schemas.joinToString()}"
    }


}