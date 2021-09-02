package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.hazelcast.query.Predicates
import com.openlattice.authorization.Ace
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.Acl
import com.openlattice.authorization.Action
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.TimeUnit

class MigrateOrgPermissionsUpgrade(
    toolbox: Toolbox,
    private val exDbPermMan: ExternalDatabasePermissioningService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(MigrateOrgPermissionsUpgrade::class.java)

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val permissions = HazelcastMap.PERMISSIONS.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {

        logger.info("starting migration")

        try {
            val timer = Stopwatch.createStarted()
            val targetOrgId: UUID? = null

            val filteredPermissions = permissions.entrySet(
                Predicates.`in`(
                    PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX,
                    SecurableObjectType.PropertyTypeInEntitySet,
                    SecurableObjectType.OrganizationExternalDatabaseColumn
                )
            ).filter { entry ->
                orgIdPredicate(entry, targetOrgId)
            }

            val acls = filteredPermissions
                .groupBy({ it.key.aclKey }, { (aceKey, aceVal) ->
                    Ace(aceKey.principal, aceVal.permissions, aceVal.expirationDate)
                })
                .map { (aclKey, aces) -> Acl(aclKey, aces) }

            // actual permission migration
            val revokeSuccess = revokeFromOldPermissionRoles(acls)
            logger.info(
                "revoking permissions took {} ms - {}",
                timer.elapsed(TimeUnit.MILLISECONDS),
                acls
            )

            val assignSuccess = assignAllPermissions(acls)
            logger.info(
                "granting permissions took {} ms - {}",
                timer.elapsed(TimeUnit.MILLISECONDS),
                acls
            )

            if (revokeSuccess && assignSuccess) {
                return true
            }

            logger.error("migration failed - revoke {} grant {}", revokeSuccess, assignSuccess)
            return false
        } catch (e: Exception) {
            logger.error("something went wrong with the migration", e)
            return false
        }
    }

    private fun revokeFromOldPermissionRoles(acls: List<Acl>): Boolean {
        logger.info("Number of acls to be processed: {}", acls.size)
        logger.info("Revoking membership from old perms roles")
        exDbPermMan.executePrivilegesUpdate(Action.DROP, acls)
        return true
    }

    private fun assignAllPermissions(acls: List<Acl>): Boolean {
        logger.info("Granting direct permissions")
        exDbPermMan.executePrivilegesUpdate(Action.SET, acls)
        return true
    }

    private fun orgIdPredicate(entry: MutableMap.MutableEntry<AceKey, AceValue>, orgId: UUID?): Boolean {
        if (orgId == null) {
            return true
        }
        try {
            val tableId = entry.key.aclKey[0]
            val securableObjectType = entry.value.securableObjectType
            return when (securableObjectType) {
                SecurableObjectType.OrganizationExternalDatabaseColumn -> {
                    externalTables[tableId]!!.organizationId == orgId
                }
                SecurableObjectType.PropertyTypeInEntitySet -> {
                    entitySets[tableId]!!.organizationId == orgId
                }
                else -> {
                    logger.error("SecurableObjectType {} is unexpected, filtering out {}", securableObjectType, entry)
                    false
                }
            }
        }
        catch (e: Exception) {
            logger.error("something went wrong filtering permissions for {}", entry, e)
            return false
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
