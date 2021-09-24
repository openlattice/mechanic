package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.hazelcast.query.Predicates
import com.openlattice.authorization.Ace
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Action
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.mapstores.SECURABLE_OBJECT_TYPE_INDEX
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.set.EntitySetFlag
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

    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val legacyPermissions = HazelcastMap.LEGACY_PERMISSIONS.getMap(toolbox.hazelcast)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {

        logger.info("starting migration")

        try {
            val timer = Stopwatch.createStarted()
            val targetOrgId: UUID? = null

            val acls = legacyPermissions.entrySet(
                Predicates.`in`(
                    SECURABLE_OBJECT_TYPE_INDEX,
                    SecurableObjectType.OrganizationExternalDatabaseColumn
                )
            ).filter { entry ->
                orgIdPredicate(entry, targetOrgId)
            }.groupBy({ it.key.aclKey }, { (aceKey, aceVal) ->
                Ace(aceKey.principal, aceVal.permissions, aceVal.expirationDate)
            })
            .map { (aclKey, aces) ->
                securableObjectTypes.putIfAbsent(AclKey(aclKey[0]), SecurableObjectType.OrganizationExternalDatabaseTable)
                securableObjectTypes.putIfAbsent(aclKey, SecurableObjectType.OrganizationExternalDatabaseColumn)
                Acl(aclKey, aces)
            }

            // actual permission migration
            val assignSuccess = assignAllPermissions(acls)
            logger.info(
                "granting permissions took {} ms - {}",
                timer.elapsed(TimeUnit.MILLISECONDS),
                acls
            )

            if (assignSuccess) {
                return true
            }

            logger.error("migration failed - revoke {} grant {}", assignSuccess)
            return false
        } catch (e: Exception) {
            logger.error("something went wrong with the migration", e)
        }

        return true
    }

    private fun assignAllPermissions(acls: List<Acl>): Boolean {
        logger.info("Granting direct permissions")
        exDbPermMan.executePrivilegesUpdate(Action.SET, acls)
        return true
    }

    private fun orgIdPredicate(entry: MutableMap.MutableEntry<AceKey, AceValue>, orgId: UUID?): Boolean {
        try {
            val tableId = entry.key.aclKey[0]
            val securableObjectType = entry.value.securableObjectType
            return when (securableObjectType) {
                SecurableObjectType.OrganizationExternalDatabaseColumn -> {
                    externalTables[tableId]!!.organizationId == orgId || orgId == null
                }
                else -> {
                    logger.warn("Looking only for OrganizationExternalDatabaseColumn, filtering out {} of SecurableObjectType {}", entry, securableObjectType)
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
