package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.LAST_LINK_INDEX
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.LINKING_ID
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.PostgresColumnsIndexDefinition
import com.openlattice.postgres.PostgresExpressionIndexDefinition
import com.openlattice.postgres.PostgresTable.IDS
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

class LinkedEntityIndexing(private val toolbox: Toolbox):Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(LinkedEntityIndexing::class.java)
    }

    override fun upgrade(): Boolean {
        addLastLinkIndexColumn()
        addNewIndicesToEntityKeys()
        return true
    }

    private fun addLastLinkIndexColumn() {
        logger.info("Starting to add ${LAST_LINK_INDEX.name} column to ${IDS.name} table")

        val updateStatement =
                "alter table ${IDS.name} " +
                        "add column if not exists ${LAST_LINK_INDEX.name} timestamp with time zone " +
                        "not null " +
                        "default '-infinity'::timestamp with time zone; "
        toolbox.executor.submit(
                Callable {
                    toolbox.hds.connection.use {
                        it.use {
                            it.createStatement().execute(updateStatement)
                        }
                    }
                }
        ).get()
        logger.info("Finished linked entities indexing update")
    }

    private fun addNewIndicesToEntityKeys() {
        logger.info("Starting to add entity_key_ids_needing_linking_indexing_idx and " +
                "entity_key_ids_last_link_index_idx index to ${IDS.name} table")

        val linkingEntitiesNeededIndexing = PostgresExpressionIndexDefinition(IDS,
                ENTITY_SET_ID.name
                        + ", ( ${LINKING_ID.name} IS NOT NULL )"
                        + ", ( ${LAST_LINK.name} >= ${LAST_WRITE.name} )"
                        + ", ( ${LAST_INDEX.name} >= ${LAST_WRITE.name} )"
                        + ", ( ${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} )")
                .name("entity_key_ids_needing_linking_indexing_idx")
                .ifNotExists()
        val lastLinkIndex = PostgresColumnsIndexDefinition(IDS, LAST_LINK_INDEX)
                .name("entity_key_ids_last_link_index_idx")
                .ifNotExists()

        toolbox.executor.submit(
                Callable {
                    toolbox.hds.connection.use {
                        it.use {
                            it.createStatement().execute(linkingEntitiesNeededIndexing.sql())
                            it.createStatement().execute(lastLinkIndex.sql())
                        }
                    }
                }
        ).get()
        logger.info("Finished linked entities indexing update")
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_12_21.value
    }

}