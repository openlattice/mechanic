package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.hazelcast.query.Predicates
import com.openlattice.authorization.Ace
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Action
import com.openlattice.authorization.mapstores.SECURABLE_OBJECT_TYPE_INDEX
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

    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(toolbox.hazelcast)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val legacyPermissions = HazelcastMap.LEGACY_PERMISSIONS.getMap(toolbox.hazelcast)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(toolbox.hazelcast)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {

        logger.info("starting migration")

        try {
            val timer = Stopwatch.createStarted()
            val processedOrgsIds = setOf<UUID>()

            val entries = legacyPermissions.entrySet(
                Predicates.equal(
                    SECURABLE_OBJECT_TYPE_INDEX,
                    SecurableObjectType.OrganizationExternalDatabaseColumn
                )
            )
            logger.info("getting permissions entry set took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.reset().start()


            val filtered = entries.filter {
                if (
                    it.key.aclKey.size > 2
                    || !externalColumns.containsKey(it.key.aclKey[1])
                    || it.value.securableObjectType != SecurableObjectType.OrganizationExternalDatabaseColumn
                ) {
                    logger.warn("ignoring permission entry {}", it)
                    return@filter false
                }
                return@filter true
            }
            logger.info("filtering permissions took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.reset().start()


            val grouped = filtered.groupBy({ it.key.aclKey }, { (aceKey, aceVal) ->
                Ace(aceKey.principal, aceVal.permissions, aceVal.expirationDate)
            })
            logger.info("grouping by acl key took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.reset().start()


            grouped.forEach { (aclKey) ->
                securableObjectTypes.putIfAbsent(AclKey(aclKey[0]), SecurableObjectType.OrganizationExternalDatabaseTable)
                securableObjectTypes.putIfAbsent(aclKey, SecurableObjectType.OrganizationExternalDatabaseColumn)
            }
            logger.info("populating securableObjectTypes map store took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.reset().start()


            val aclsByOrg = grouped
                .map { (aclKey, aces) -> Acl(aclKey, aces) }
                .groupBy { externalTables[it.aclKey[0]]?.organizationId }
                .toList()
                .sortedBy { it.second.size }
                .toMap()
            logger.info("grouping by org took {} ms", timer.elapsed(TimeUnit.MILLISECONDS))
            timer.stop()

            val targetOrgIds = mutableListOf<UUID?>()
            aclsByOrg.forEach {
                logger.info("org ${it.key} acls ${it.value.size}")
                if (!processedOrgsIds.contains(it.key)) {
                    targetOrgIds.add(it.key)
                }
            }
            logger.info("sorted orgs $targetOrgIds")

            targetOrgIds.forEachIndexed { index, orgId ->
                try {
                    logger.info("================================")
                    logger.info("================================")
                    logger.info("starting to process org $orgId")

                    if (aclsByOrg.containsKey(orgId)) {

                        val acls = aclsByOrg.getValue(orgId)
                        logger.info("org acls {}", acls.size)

                        timer.reset().start()
                        logger.info("granting permissions - org $orgId")
                        acls.chunked(128).forEach { aclChunk ->
                            // prevents from rolling back everything in case of error(s)
                            // by feeding it one acl at a time
                            exDbPermMan.executePrivilegesUpdate(Action.SET, chunkedAcl)
                        }
                        logger.info(
                            "granting permissions took {} ms - org $orgId acls {}",
                            timer.elapsed(TimeUnit.MILLISECONDS),
                            acls.size
                        )
                    }
                    else {
                        logger.warn("aclsByOrg does not contain org $orgId")
                    }
                } catch (e: Exception) {
                    logger.error("something went wrong processing org $orgId", e)
                } finally {
                    logger.info("progress ${index + 1}/${organizations.size}")
                    logger.info("================================")
                    logger.info("================================")
                }
            }

        } catch (e: Exception) {
            logger.error("something went wrong with the migration", e)
            return false
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
