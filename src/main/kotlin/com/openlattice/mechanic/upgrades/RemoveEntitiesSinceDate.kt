package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.CitusDistributedTableDefinition
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*

const val SYNCLESS_ENTITY_KEY_IDS_TABLE = "syncless_entity_key_ids"

class RemoveEntitiesSinceDate(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(RemoveEntitiesSinceDate::class.java)
    }

    override fun upgrade(): Boolean {
        val latestValidVersion = OffsetDateTime.now().minusDays(3).toInstant().toEpochMilli()
        createTableOfSynclessEntities(latestValidVersion)

        deleteSynclessProperties()
        deleteSynclessEdges(latestValidVersion)
        deleteSynclessEntityKeyIds()

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_05_13.value
    }


    private fun createTableOfSynclessEntities(latestValidVersion: Long) {

        val table = CitusDistributedTableDefinition(SYNCLESS_ENTITY_KEY_IDS_TABLE)
                .addColumns(PostgresColumn.ID)
                .distributionColumn(PostgresColumn.ID)

        toolbox.tableManager.registerTables(setOf(table))

        logger.info("Created $SYNCLESS_ENTITY_KEY_IDS_TABLE table")

        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(loadSynclessEntityKeyIds(latestValidVersion))
            }
        }

        logger.info("Entities written to $SYNCLESS_ENTITY_KEY_IDS_TABLE table")
    }

    private fun deleteSynclessProperties() {
        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->

                toolbox.propertyTypes.keys.forEach { pt -> stmt.addBatch(deletePropertyValuesSql(pt)) }
                stmt.executeBatch()
            }
        }

        logger.info("Deleted syncless properties")

    }

    private fun deleteSynclessEdges(latestValidVersion: Long) {
        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->

                stmt.execute(deleteSynclessEdgesSql(latestValidVersion))
            }
        }

        logger.info("Deleted syncless edges")

    }

    private fun deleteSynclessEntityKeyIds() {
        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->

                stmt.execute(deleteFromTableOnIdCol(PostgresTable.IDS.name, PostgresColumn.ID.name))

            }
        }

        logger.info("Deleted syncless entity key ids")

    }


    private fun loadSynclessEntityKeyIds(latestValidVersion: Long): String {
        return "INSERT INTO $SYNCLESS_ENTITY_KEY_IDS_TABLE SELECT ${PostgresColumn.ID.name} FROM ${PostgresTable.IDS.name} " +
                " ${versionsClause(latestValidVersion)} ON CONFLICT DO NOTHING"
    }

    private fun versionsClause(latestValidVersion: Long): String {
        return " WHERE NOT EXISTS  (SELECT * from UNNEST(${PostgresColumn.VERSIONS.name}) as v where v > 0 AND v < $latestValidVersion)" +
                " AND NOT EXISTS (SELECT * from UNNEST(${PostgresColumn.VERSIONS.name}) as v where v < -1 AND v > -$latestValidVersion)"
    }

    private fun deletePropertyValuesSql(propertyTypeId: UUID): String {
        val table = DataTables.quote(DataTables.propertyTableName(propertyTypeId))
        return deleteFromTableOnIdCol(table, PostgresColumn.ID.name)
    }

    private fun deleteFromTableOnIdCol(table: String, col: String): String {
        return "DELETE FROM $table USING $SYNCLESS_ENTITY_KEY_IDS_TABLE " +
                "WHERE $table.$col = $SYNCLESS_ENTITY_KEY_IDS_TABLE.${PostgresColumn.ID.name}"
    }

    private fun deleteSynclessEdgesSql(latestValidVersion: Long): String {
        return "DELETE FROM ${PostgresTable.EDGES.name} ${versionsClause(latestValidVersion)}"
    }
}