package com.openlattice.mechanic.upgrades

import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.transporter.processors.GetPropertyTypesFromTransporterColumnSetEntryProcessor
import org.slf4j.LoggerFactory

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class SyncOrgPermissionsUpgrade(
        toolbox: Toolbox,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val exDbPermMan: ExternalDatabasePermissioningService
): Upgrade {

    val logger = LoggerFactory.getLogger(SyncOrgPermissionsUpgrade::class.java)

    private val entitySets = toolbox.entitySets
    private val propertyTypes = toolbox.propertyTypes
    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {
        val assemblies = updatePermissionsForAssemblies()
        val mapPTrees = mapAllPrincipalTrees()
        val createPRoles = createAllPermRoles()
        if (assemblies  && mapPTrees  && createPRoles) {
            return true
        }
        logger.error("Sync permissions upgrade failed, final status:\n" +
                "updatePermissionsForAssemblies: {}\n" +
                "mapAllPrincipalTrees: {}\n" +
                "createAllPermRoles: {}\n", assemblies, mapPTrees, createPRoles)
        return false
    }

    fun updatePermissionsForAssemblies(): Boolean {
        val assembliesByOrg = entitySets.values.filter { es ->
            es.flags.contains(EntitySetFlag.TRANSPORTED)
        }.groupBy { it.organizationId }

        val etids = assembliesByOrg.values.flatten().mapTo( mutableSetOf() ) { it.entityTypeId }

        val etIdsToPtFqns = transporterState.submitToKeys(
                etids, GetPropertyTypesFromTransporterColumnSetEntryProcessor()
        ).thenApply { etIdsToPtIds ->
            etIdsToPtIds.mapValues { (_, ptids) ->
                ptids.mapTo(mutableSetOf()) { ptid ->
                    PropertyTypeIdFqn.fromPropertyType(propertyTypes.getValue(ptid))
                }
            }
        }.toCompletableFuture().get()

        // final shape is etid -> Set<PtIdFqn>
        assembliesByOrg.map { (orgId, entitySets) ->
            exConnMan.connectToOrg(orgId).use { hds ->
                entitySets.forEach { es ->
                    exDbPermMan.initializeAssemblyPermissions(
                        hds,
                        es.id,
                        es.name,
                        etIdsToPtFqns.getValue(es.entityTypeId)
                    )
                }
            }
        }

        return true
    }

    fun createAllPermRoles(): Boolean {
        return true
    }

    fun mapAllPrincipalTrees(): Boolean {
        return true
    }

    override fun getSupportedVersion(): Long {
        TODO("Not yet implemented")
    }
}
