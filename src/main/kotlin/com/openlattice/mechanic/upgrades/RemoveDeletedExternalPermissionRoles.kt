package com.openlattice.mechanic.upgrades

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory

class RemoveDeletedExternalPermissionRoles(
        private val toolbox: Toolbox,
        private val externalDbConnMan: ExternalDatabaseConnectionManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(RemoveDeletedExternalPermissionRoles::class.java)
        private const val BATCH_SIZE = 10_000
    }

    override fun upgrade(): Boolean {
        logger.info("About to remove deleted external permission roles")
        val propertyTypeAndColumnIds = toolbox.propertyTypes.keys + HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(toolbox.hazelcast).keys.toSet()

        val hds = externalDbConnMan.connectAsSuperuser()

        val externalPermissionRoles = HazelcastMap.EXTERNAL_PERMISSION_ROLES.getMap(toolbox.hazelcast)

        externalPermissionRoles.entries.chunked(BATCH_SIZE).forEach { chunk ->
            val toDelete = chunk.filter { !propertyTypeAndColumnIds.contains(it.key.aclKey.last()) }

            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    toDelete.forEach { (accessTarget, roleName) ->
                        try {
                            stmt.execute("DROP ROLE ${quote(roleName.toString())}")
                            externalPermissionRoles.delete(accessTarget)
                        } catch (e: Exception) {
                            logger.error("Unable to delete role {} for AccessTarget {}", roleName, accessTarget)
                        }
                    }
                }
            }
        }

        logger.info("Finished removing deleted external permission roles")
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_02_14.value
    }
}