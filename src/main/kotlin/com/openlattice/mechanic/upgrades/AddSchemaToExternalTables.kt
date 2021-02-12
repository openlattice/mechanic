package com.openlattice.mechanic.upgrades

import com.google.common.collect.Sets
import com.hazelcast.map.IMap
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import org.slf4j.LoggerFactory
import java.util.*

class AddSchemaToExternalTables(
        private val toolbox: Toolbox,
        private val edms: ExternalDatabaseManagementService,
        private val reservationService: HazelcastAclKeyReservationService
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddSchemaToExternalTables::class.java)
    }

    override fun upgrade(): Boolean {
        logger.info("About to begin adding schema and missing oids")

        val orgs = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast).toMap()
        val orgDbs = HazelcastMap.ORGANIZATION_DATABASES.getMap(toolbox.hazelcast).toMap()
        val orgIdsWithDbs = Sets.intersection(orgs.keys, orgDbs.keys).immutableCopy()

        val externalTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(toolbox.hazelcast)
        val externalColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(toolbox.hazelcast)

        cleanUpStrayTablesAndCols(orgIdsWithDbs, externalTables, externalColumns)

        val tablesByOrg = externalTables.values.toList().groupBy { it.organizationId }
        val columnsByTable = externalColumns.values.toList().groupBy { it.tableId }

        val tableIdsToDelete = mutableSetOf<UUID>()
        val tablesWithSchemaAndOid = mutableMapOf<UUID, OrganizationExternalDatabaseTable>()

        orgIdsWithDbs.forEach { orgId ->
            logger.info("About to scrape org $orgId")

            val dbName = orgDbs.getValue(orgId).name

            val tablesAndColumns = edms.getColumnNamesByTableName(dbName).associateBy { it.tableName }

            (tablesByOrg[orgId] ?: listOf()).forEach {

                tablesAndColumns[it.name]?.let { (oid, _, schemaName) ->

                    try {
                        reservationService.renameReservation(it.id, it.getUniqueName())
                    } catch (e: Exception) {
                        logger.info("reservation ${it.getUniqueName()} was already renamed, skipping...")
                    }

                    tablesWithSchemaAndOid[it.id] = OrganizationExternalDatabaseTable(
                            id = it.id,
                            name = it.name,
                            title = it.title,
                            description = Optional.of(it.description),
                            organizationId = it.organizationId,
                            oid = oid,
                            schema = schemaName
                    )
                } ?: tableIdsToDelete.add(it.id)

            }

        }

        logger.info("Deleting ${tableIdsToDelete.size} stale tables, and updating ${tablesWithSchemaAndOid.size} existing ones")

        externalTables.putAll(tablesWithSchemaAndOid)

        edms.deleteOrganizationExternalDatabaseTableObjects(tableIdsToDelete)
        val columnIdsTodDelete = tableIdsToDelete
                .flatMap { columnsByTable[it] ?: listOf() }
                .groupBy { it.tableId }
                .mapValues { it.value.map { c -> c.id }.toSet() }
        edms.deleteOrganizationExternalDatabaseColumnObjects(columnIdsTodDelete)

        logger.info("Finished adding schema and missing oids")

        return true
    }

    private fun cleanUpStrayTablesAndCols(
            orgIds: Set<UUID>,
            externalTables: IMap<UUID, OrganizationExternalDatabaseTable>,
            externalColumns: IMap<UUID, OrganizationExternalDatabaseColumn>
    ) {

        edms.deleteOrganizationExternalDatabaseTableObjects(externalTables
                .values
                .toList()
                .filter { !orgIds.contains(it.organizationId) }
                .map { it.id }
                .toSet()
        )

        val tableIds = externalTables.keys.toSet()

        edms.deleteOrganizationExternalDatabaseColumnObjects(externalColumns
                .values
                .toList()
                .filter { !tableIds.contains(it.tableId) || !orgIds.contains(it.organizationId) }
                .groupBy { it.tableId }
                .mapValues {
                    it.value.map { c -> c.id }.toSet()
                }
        )

    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }
}