package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.MEMBER_ORG_DATABASE_PERMISSIONS
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.Organization
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

class CreateAtlasUsersAndSetPermissions(
        private val toolbox: Toolbox,
        private val externalDatabaseConnectionManager: ExternalDatabaseConnectionManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(CreateAtlasUsersAndSetPermissions::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_09_15.value
    }

    override fun upgrade(): Boolean {
        val dbCreds = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast).toMap()

        resetUserCredentials(dbCreds)

//        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()
//
//        val principalsToAccounts = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast)
//                .values
//                .toSet()
//                .associate {
//
//                    val mvAccount = when (it.principalType) {
//                        PrincipalType.USER -> dbCreds[buildPostgresUsername(it)]
//                        PrincipalType.ROLE -> dbCreds[buildPostgresRoleName(it as Role)]
//                        PrincipalType.ORGANIZATION -> dbCreds[buildOrganizationUserId(it.id)]
//                        else -> null
//                    }
//                    it.principal to mvAccount
//                }.filterValues { it != null }.mapValues { it.value!! }
//
//        configureUsersInAtlas(principalsToAccounts.filter { it.key.type == PrincipalType.USER }.values)
//
//        orgs.values.forEach { configureUsersInOrganization(it, principalsToAccounts) }
//
//        grantPrivilegesBasedOnStoredPermissions(principalsToAccounts)

        return true
    }

    private fun resetUserCredentials(dbCreds: Map<String, MaterializedViewAccount>) {
        connectToExternalDatabase().connection.use { conn ->
            conn.createStatement().use { stmt ->

                dbCreds.values.forEach { mvAccount ->
                    val sql = updateUserCredentialSql(mvAccount.username, mvAccount.credential)

                    try {
                        stmt.execute(sql)
                    } catch (e: Exception) {
                        logger.error("Unable to reset credential for user ${mvAccount.username} using sql $sql", e)
                    }
                }
            }
        }
    }

    internal fun updateUserCredentialSql(dbUser: String, credential: String): String {
        return "ALTER ROLE $dbUser WITH ENCRYPTED PASSWORD '$credential'"
    }

    private fun getColumnsToUserPermissions(): Map<AclKey, Map<Principal, EnumSet<Permission>>> {
        return HazelcastMap.PERMISSIONS.getMap(toolbox.hazelcast).entrySet(
                Predicates.and(
                        Predicates.equal<AceKey, AceValue>(PermissionMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.USER),
                        Predicates.equal<AceKey, AceValue>(PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX, SecurableObjectType.OrganizationExternalDatabaseColumn)
                )
        ).groupBy { it.key.aclKey }.toMap().mapValues {
            it.value.associate { entry ->
                entry.key.principal to entry.value.permissions
            }
        }
    }


    private fun connectToOrgDatabase(org: Organization): HikariDataSource {
        val dbName = ExternalDatabaseConnectionManager.buildDefaultOrganizationDatabaseName(org.id)
        return connectToExternalDatabase(dbName)
    }

    private fun connectToExternalDatabase(dbName: String = "postgres"): HikariDataSource {
        return externalDatabaseConnectionManager.connect(dbName)
    }

    private fun configureUsersInAtlas(userMVAccounts: Collection<MaterializedViewAccount>) {
        logger.info("About to create users in external database")

        val numUpdates = connectToExternalDatabase().connection.use { conn ->
            conn.createStatement().use { stmt ->

                userMVAccounts.map {
                    stmt.executeUpdate(createUserIfNotExistsSql(it.username, it.credential))
                    stmt.executeUpdate(revokePublicSchemaAccessSql(it.username))
                }.sum()

            }
        }

        logger.info("Finished creating $numUpdates users in external database")
    }

    private fun configureUsersInOrganization(organization: Organization, principalsToAccounts: Map<Principal, MaterializedViewAccount>) {

        try {

            val usernames = organization.members.mapNotNull { principalsToAccounts[it]?.username }
            val usernamesSql = usernames.joinToString(", ")

            logger.info("Configuring users $usernames in organization ${organization.title} [${organization.id}]")

            val dbName = ExternalDatabaseConnectionManager.buildDefaultOrganizationDatabaseName(organization.id)
            val grantDefaultPermissionsOnDatabaseSql = "GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                    "ON DATABASE ${DataTables.quote(dbName)} TO $usernamesSql"
            val grantOLSchemaPrivilegesSql = "GRANT USAGE ON SCHEMA ${AssemblerConnectionManager.OPENLATTICE_SCHEMA} TO $usernamesSql"
            val grantStagingSchemaPrivilegesSql = "GRANT USAGE, CREATE ON SCHEMA ${AssemblerConnectionManager.STAGING_SCHEMA} TO $usernamesSql"

            logger.info("grantDefaultPermissionsOnDatabaseSql: $grantDefaultPermissionsOnDatabaseSql")
            logger.info("grantOLSchemaPrivilegesSql: $grantOLSchemaPrivilegesSql")
            logger.info("grantStagingSchemaPrivilegesSql: $grantStagingSchemaPrivilegesSql")

            connectToOrgDatabase(organization).connection.use { connection ->
                connection.createStatement().use { statement ->

                    statement.execute(grantDefaultPermissionsOnDatabaseSql)
                    statement.execute(grantOLSchemaPrivilegesSql)
                    statement.execute(grantStagingSchemaPrivilegesSql)

                    usernames.forEach { userId -> statement.addBatch(setSearchPathSql(userId)) }
                    statement.executeBatch()
                }
            }

        } catch (e: Exception) {
            logger.error("Unable to configure users in organization ${organization.title} [${organization.id}]: ", e)
        }
    }

    private fun setSearchPathSql(granteeId: String): String {
        val searchPathSchemas = listOf(
                AssemblerConnectionManager.OPENLATTICE_SCHEMA,
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

    private fun revokePublicSchemaAccessSql(dbUser: String): String {
        return "REVOKE USAGE ON SCHEMA ${AssemblerConnectionManager.PUBLIC_SCHEMA} FROM ${quote(dbUser)}"

    }

    private fun grantPrivilegesBasedOnStoredPermissions(principalsToAccounts: Map<Principal, MaterializedViewAccount>) {
        val tablesMap = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(toolbox.hazelcast).toMap()
        val columnsMap = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(toolbox.hazelcast).toMap()
        val orgsMap = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

        val colsToUserPermissions = getColumnsToUserPermissions()

        columnsMap.values.groupBy { it.organizationId }.filter { orgsMap.containsKey(it.key) }.forEach { (orgId, columns) ->
            logger.info("Granting privileges for tables in org $orgId")

            val orgUserAce = Ace(orgsMap.getValue(orgId).principal, EnumSet.allOf(Permission::class.java))

            val orgColumnsAcls = columns.map {
                val columnAclKey = AclKey(it.tableId, it.id)
                val userAces = colsToUserPermissions.getOrDefault(columnAclKey, mapOf())
                        .map { entry -> Ace(entry.key, entry.value) }
                        .plus(orgUserAce)
                Acl(columnAclKey, userAces)
            }

            executePrivilegesUpdate(
                    orgId,
                    orgColumnsAcls,
                    principalsToAccounts,
                    tablesMap,
                    columnsMap
            )

            logger.info("Completed privilege grants for tables in org $orgId")
        }

    }

// taken + modified from ExternalDatabaseManagementService

    private fun executePrivilegesUpdate(
            organizationId: UUID,
            columnAcls: List<Acl>,
            principalsToAccounts: Map<Principal, MaterializedViewAccount>,
            tablesMap: Map<UUID, OrganizationExternalDatabaseTable>,
            columnsMap: Map<UUID, OrganizationExternalDatabaseColumn>
    ) {
        try {
            val columnIds = columnAcls.map { it.aclKey[1] }.toSet()
            val columnsById = columnIds.associateWith { columnsMap[it] }
            val columnAclsByOrg = columnAcls
                    .filter { columnsById[it.aclKey[1]] != null }
                    .groupBy { columnsById[it.aclKey[1]]!!.organizationId }

            columnAclsByOrg.forEach { (orgId, columnAcls) ->
                val dbName = ExternalDatabaseConnectionManager.buildDefaultOrganizationDatabaseName(orgId)
                connectToExternalDatabase(dbName).connection.use { conn ->
                    conn.autoCommit = false
                    val stmt = conn.createStatement()
                    columnAcls.forEach {
                        val tableAndColumnNames = getTableAndColumnNames(AclKey(it.aclKey), tablesMap, columnsMap)
                        val tableName = tableAndColumnNames.first
                        val columnName = tableAndColumnNames.second
                        it.aces.forEach { ace ->
                            val dbUser = principalsToAccounts[ace.principal]

                            if (dbUser == null) {
                                logger.info("Could not load MV account for user ${ace.principal}. Skipping DB grant.")
                                return@forEach
                            }

                            val privileges = getPrivilegesFromPermissions(ace.permissions)
                            val grantSql = createPrivilegesUpdateSql(privileges, tableName, columnName, quote(dbUser.username))
                            try {
                                stmt.execute(grantSql)
                            } catch (e: Exception) {
                                logger.error("Unable to execute privilege grant using sql: $grantSql ", e)
                            }
                        }
                        conn.commit()
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Unable to assign ${columnAcls.size} privileges for users in organization $organizationId : ", e)

        }
    }

    private fun getTableAndColumnNames(
            aclKey: AclKey,
            tablesMap: Map<UUID, OrganizationExternalDatabaseTable>,
            columnsMap: Map<UUID, OrganizationExternalDatabaseColumn>
    ): Pair<String, String> {
        val securableObjectId = aclKey[1]
        val organizationAtlasColumn = columnsMap.getValue(securableObjectId)
        val tableName = tablesMap.getValue(organizationAtlasColumn.tableId).name
        val columnName = organizationAtlasColumn.name
        return Pair(tableName, columnName)
    }

    private fun getPrivilegesFromPermissions(permissions: EnumSet<Permission>): List<String> {
        val privileges = mutableListOf<String>()
        if (permissions.contains(Permission.OWNER)) {
            privileges.add(PostgresPrivileges.ALL.toString())
        } else {
            if (permissions.contains(Permission.WRITE)) {
                privileges.addAll(listOf(
                        PostgresPrivileges.INSERT.toString(),
                        PostgresPrivileges.UPDATE.toString()))
            }
            if (permissions.contains(Permission.READ)) {
                privileges.add(PostgresPrivileges.SELECT.toString())
            }
        }
        return privileges
    }

    private fun createPrivilegesUpdateSql(privileges: List<String>, tableName: String, columnName: String, dbUser: String): String {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        return "GRANT $privilegesAsString (${quote(columnName)}) ON $tableName TO $dbUser"
    }
}