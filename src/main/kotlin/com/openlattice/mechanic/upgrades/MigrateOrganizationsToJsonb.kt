package com.openlattice.mechanic.upgrades

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.query.Predicates
import com.openlattice.IdConstants
import com.openlattice.authorization.*
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.OrganizationPrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.Grant
import com.openlattice.organizations.GrantType
import com.openlattice.organizations.Organization
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ORGANIZATIONS
import com.openlattice.postgres.ResultSetAdapters.*
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.slf4j.LoggerFactory
import java.util.*

class MigrateOrganizationsToJsonb(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(MigrateOrganizationsToJsonb::class.java)
        private val mapper = ObjectMappers.getJsonMapper()
    }

    override fun upgrade(): Boolean {

        addColumn()

        populateColumn()

        return true
    }

    private fun addColumn() {

        logger.info("About to add column to organizations table with sql {}", ADD_COLUMN_SQL)

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { statement ->
                statement.execute(ADD_COLUMN_SQL)
            }
        }

        logger.info("Finished adding column.")

    }

    private fun populateColumn() {

        logger.info("About to populate column with json from existing organizations.")

        val principals = toolbox.hazelcast.getMap<AclKey, SecurablePrincipal>(HazelcastMap.PRINCIPALS.name)

        val orgPrincipalsById = principals
                .entrySet(Predicates.equal("principalType", PrincipalType.ORGANIZATION))
                .map { it.key[0] to it.value.principal }
                .toMap()

        val rolesByOrgId = principals.values(Predicates.equal("principalType", PrincipalType.ROLE))
                .map { it as Role }
                .groupBy { it.organizationId }
                .mapValues { it.value.toMutableSet() }


        val orgs = BasePostgresIterable(StatementHolderSupplier(toolbox.hds, "SELECT * FROM ${ORGANIZATIONS.name}")) { rs ->

            val id = id(rs)
            val title = title(rs)
            val description = description(rs)
            val maybeEmails = rs.getArray(ALLOWED_EMAIL_DOMAINS.name)

            val allowedEmailDomains = if (maybeEmails != null && maybeEmails.array != null)
                (rs.getArray(ALLOWED_EMAIL_DOMAINS.name).array as Array<String>).toMutableSet()
            else
                mutableSetOf()
            val memberPrincipals = members(rs).map { Principal(PrincipalType.USER, it) }.toMutableSet()
            val appIds = (rs.getArray(APP_IDS.name).array as Array<UUID>).toMutableSet()
            val partitions = partitions(rs).toMutableList()

            val orgPrincipal = OrganizationPrincipal(
                    Optional.of(id),
                    orgPrincipalsById.getValue(id),
                    title,
                    Optional.of(description)
            )

            val roles = rolesByOrgId.getValue(id)

            val grants = if (id == IdConstants.GLOBAL_ORGANIZATION_ID.id) {
                mutableMapOf(
                        AuthorizationInitializationTask.GLOBAL_USER_ROLE.id to mutableMapOf(GrantType.Automatic to Grant(GrantType.Automatic, setOf())),
                        AuthorizationInitializationTask.GLOBAL_ADMIN_ROLE.id to mutableMapOf(
                                GrantType.Roles to Grant(GrantType.Roles, setOf(SystemRole.ADMIN.principal.id))
                        ))
            } else {
                mutableMapOf()
            }

            Organization(
                    orgPrincipal,
                    allowedEmailDomains,
                    memberPrincipals,
                    roles,
                    mutableSetOf(),
                    partitions,
                    appIds,
                    mutableSetOf(),
                    grants
            )
        }.toList()

        toolbox.hds.connection.use { conn ->
            conn.prepareStatement(POPULATE_COLUMN_SQL).use { ps ->
                orgs.forEach {
                    ps.setString(1, mapper.writeValueAsString(it))
                    ps.setObject(2, it.id)
                    ps.addBatch()
                }

                ps.executeBatch()
            }
        }

        logger.info("Finished populating column.")
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_11_21.value
    }

    private val ADD_COLUMN_SQL = "ALTER TABLE ${ORGANIZATIONS.name} ADD COLUMN IF NOT EXISTS ${ORGANIZATION.sql()} default '{}'"

    private val POPULATE_COLUMN_SQL = "UPDATE ${ORGANIZATIONS.name} SET ${ORGANIZATION.name} = ?::jsonb WHERE ${ID.name} = ? "
}