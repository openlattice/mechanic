package com.openlattice.mechanic.upgrades

import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ExportOrganizationMembers(
    private val toolbox: Toolbox,
    private val hds: HikariDataSource,
    private val principalService: SecurePrincipalsManager,
    hazelcast :HazelcastInstance
) : Upgrade {
    companion object {
        private const val USERS_EXPORT_TABLE_NAME = "users_export"
        const val USERS_EXPORT_TABLE = """
            CREATE TABLE $USERS_EXPORT_TABLE_NAME ( 
                organization_id uuid,
                principal_id text NOT NULL,
                principal_email text, 
                principal_username text,
                PRIMARY KEY( organization_id, principal_id ) );
        """
        const val INSERT_USER_SQL = """
            INSERT INTO $USERS_EXPORT_TABLE_NAME (organization_id, principal_id, principal_email, principal_username) VALUES (?,?,?,?)
        """
        private val logger = LoggerFactory.getLogger(ExportOrganizationMembers::class.java)
    }

    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcast)
    private val users = HazelcastMap.USERS.getMap(hazelcast)

    override fun upgrade(): Boolean {
        val organizationsIds = organizations.keys.toMutableSet()
        val members = principalService.getOrganizationMembers(organizationsIds)

        hds.connection.use { connection ->
            connection.createStatement().use { stmt -> stmt.execute(USERS_EXPORT_TABLE) }
            connection.prepareStatement(INSERT_USER_SQL).use { ps ->
            members.forEach { (orgId, orgMembers) ->
                orgMembers.forEach { orgMember ->
                    val user = users[ orgMember.name ]
                        ps.setObject(1, orgId)
                        ps.setString(2, orgMember.name)
                        ps.setString(3, user?.email)
                        ps.setString(4, user?.username)
                    ps.addBatch()
                    }
                }
                val userCount = ps.executeBatch().sum()
                logger.info("Exported $userCount users across ${organizationsIds.size} organizations.")
            }
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }

}