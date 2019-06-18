package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnsIndexDefinition
import com.openlattice.postgres.PostgresExpressionIndexDefinition
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.PostgresTable.ENTITY_KEY_IDS
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

class LinkedEntityIndexing(private val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(LinkedEntityIndexing::class.java)
    }

    override fun upgrade(): Boolean {
        addLastLinkIndexColumn()
        addNewIndicesToEntityKeys()
        addLinkedEntitySetIndex()
        return true
    }

    private fun addLastLinkIndexColumn() {
        logger.info("Starting to add ${LAST_LINK_INDEX.name} column to ${ENTITY_KEY_IDS.name} table")

        val updateStatement =
                "alter table ${ENTITY_KEY_IDS.name} " +
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
                "entity_key_ids_last_link_index_idx index to ${ENTITY_KEY_IDS.name} table")

        val linkingEntitiesNeededIndexing = PostgresExpressionIndexDefinition(ENTITY_KEY_IDS,
                                                                              ENTITY_SET_ID.name
                        + ", ( ${LINKING_ID.name} IS NOT NULL )"
                        + ", ( ${LAST_LINK.name} >= ${LAST_WRITE.name} )"
                        + ", ( ${LAST_INDEX.name} >= ${LAST_WRITE.name} )"
                        + ", ( ${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} )")
                .name("entity_key_ids_needing_linking_indexing_idx")
                .ifNotExists()
        val lastLinkIndex = PostgresColumnsIndexDefinition(ENTITY_KEY_IDS, LAST_LINK_INDEX)
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

    private fun addLinkedEntitySetIndex() {
        logger.info("Starting to add entity_sets_linked_entity_sets_idx and to ${ENTITY_SETS.name} table")

        val linkedEntitySetsIndex = PostgresColumnsIndexDefinition(ENTITY_SETS, LINKED_ENTITY_SETS)
                .method(IndexType.GIN)
                .ifNotExists()

        toolbox.executor.submit(
                Callable {
                    toolbox.hds.connection.use {
                        it.use {
                            it.createStatement().execute(linkedEntitySetsIndex.sql())
                        }
                    }
                }
        ).get()
        logger.info("Finished linked entity sets indexing update")
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_12_21.value
    }

}