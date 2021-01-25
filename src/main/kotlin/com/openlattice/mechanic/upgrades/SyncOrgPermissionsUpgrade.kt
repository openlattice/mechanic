package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.processors.GetFqnFromPropertyTypeEntryProcessor
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.transporter.processors.GetPropertyTypesFromTransporterColumnSetEntryProcessor
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class SyncOrgPermissionsUpgrade(
        toolbox: Toolbox,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val exDbPermMan: ExternalDatabasePermissioningService
): Upgrade {

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap(toolbox.hazelcast)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {
        val assembledEntitySets = entitySets.entrySet(Predicates.equal<UUID, EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.TRANSPORTED))

        val assembliesByOrg = assembledEntitySets.groupBy({ (_, value) ->
            value.organizationId
        }, { (_, value) ->
            value
        })

        val etids = assembliesByOrg.values.flatten().mapTo( mutableSetOf() ) { it.entityTypeId }

        val etIdsToPts = transporterState.submitToKeys(
                etids, GetPropertyTypesFromTransporterColumnSetEntryProcessor()
        )

        val etIdsToFqns = etIdsToPts.thenCompose { etsToPts ->
            val allPtids = etsToPts.values.flatMapTo(mutableSetOf()) { it }
            propertyTypes.submitToKeys(allPtids, GetFqnFromPropertyTypeEntryProcessor())
        }.thenCombine( etIdsToPts ) { ptIdToFqn, etIdToPt ->
            etIdToPt.mapValues { (_, ptIds) ->
                ptIds.mapTo( mutableSetOf() ) { ptid ->
                    PropertyTypeIdFqn(ptid, ptIdToFqn.getValue(ptid))
                }.toSet()
            }
        }.toCompletableFuture().get()


        // final shape is etid -> ptfqns
        assembliesByOrg.map { (orgId, entitySets) ->
            exConnMan.connectToOrg(orgId).use { hds ->
                entitySets.forEach { es ->
                    exDbPermMan.initializeAssemblyPermissions(
                        hds,
                        es.id,
                        es.name,
                        etIdsToFqns.getValue(es.entityTypeId)
                    )
                }
            }
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        TODO("Not yet implemented")
    }
}