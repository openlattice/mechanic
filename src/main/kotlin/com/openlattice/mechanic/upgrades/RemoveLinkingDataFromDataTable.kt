package com.openlattice.mechanic.upgrades

import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import org.slf4j.LoggerFactory
import java.util.*

class RemoveLinkingDataFromDataTable(val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(RemoveLinkingDataFromDataTable::class.java)

        private const val CLEANUP_TABLE_NAME = "linking_data_cleanup"
        private const val DATA_PKEY_NAME = "data_pkey"
        private const val OLD_DATA_PKEY_NAME = "data_pkey1"

        private val NEW_DATA_PKEY_COLS = listOf(
                ENTITY_SET_ID,
                ID,
                PARTITION,
                PROPERTY_TYPE_ID,
                HASH
        ).map { it.name }

        private val ENTITY_PKEY_COLS = listOf(PARTITION, ID, ENTITY_SET_ID).joinToString { it.name }

        private val ENTITY_SET_IDS_NEEDING_CLEANUP = listOf(
                "e9dc56bf-7cf9-4e25-8969-1ac4c1b1e1ec",
                "d189f747-fa39-4b41-b86e-a056100ed2fa",
                "2deb5292-11b5-4874-b4b1-2a5a57804e68",
                "9d9a6c3e-fd82-4599-9dcc-a2602e2bd54d",
                "0843f5ca-90a3-42a4-aeeb-db8fbb75107f",
                "8bfd8145-f4bd-4091-85b4-30980c39b933",
                "94ad69c8-6953-4980-bd27-b214d311d83d",
                "c55ea580-f234-491a-ae71-1db355025f41",
                "c4214341-4151-4f4d-bf7d-400001682f33",
                "e1a9c1ae-5a38-401d-861e-01ffc326887c",
                "6ca4a13d-db38-4220-ae37-b5cef76f7ccb",
                "c0d47ad6-6dc0-4eab-9a06-be6eb3892513",
                "2f3afb54-f757-414c-b7c1-0b07c194edad",
                "8bfb4aa6-6917-4884-a641-82d35f28034a",
                "51ae6b46-acfc-4e48-b4ec-50f945280418",
                "51992ab5-c315-4696-be93-7780c29d490f",
                "836699a2-75b1-4c3c-beb1-2be0e426b9b7",
                "ed5716db-830b-41b7-9905-24fa82761ace",
                "0ca8043f-d577-4daf-b894-55e8eea47c63",
                "fed0eb58-e55e-46c9-a0dd-a0f30a8bd719",
                "68cd6423-d288-4018-9ec1-a5ae6787b101",
                "9cc5536b-d164-48f9-a240-6c82f61d28fb",
                "7e21ccd3-b978-4a4b-9cc3-a51ebf4f2219",
                "d480eafe-cd37-4d3c-911d-803ae6c3e397",
                "b4a33df2-b8ee-4af5-ba6b-bc2dee4b3918"
        ).map { UUID.fromString(it) }

        /* create cleanup table queries */

        private val CREATE_NEEDS_CLEANUP_TABLE_SQL = """
            CREATE TABLE $CLEANUP_TABLE_NAME AS 
              SELECT ( ${NEW_DATA_PKEY_COLS.joinToString()} )
              FROM ${DATA.name}
              WHERE ${ENTITY_SET_ID.name} = ANY( ${ENTITY_SET_IDS_NEEDING_CLEANUP.joinToString { "'$it'" }} )
              GROUP BY ${NEW_DATA_PKEY_COLS.joinToString()}
              HAVING COUNT(*) > 1
        """.trimIndent()

        /* merge + remove dups queries */

        private val MERGE_AND_REMOVE_DUPS_SQL = """
            TODO
        """.trimIndent()

        /* reset linking ids queries */

        private val UPDATE_LINKING_ID_IN_IDS_SQL = """
            UPDATE ${IDS.name} 
            SET ${LINKING_ID.name} = NULL,
              ${LAST_LINK.name} = '-infinity'
            WHERE ($ENTITY_PKEY_COLS) IN (
              SELECT DISTINCT $ENTITY_PKEY_COLS FROM $CREATE_NEEDS_CLEANUP_TABLE_SQL
            )
        """.trimIndent()

        private val UPDATE_LINKING_ID_IN_DATA_SQL = """
            UPDATE ${DATA.name} 
            SET ${LINKING_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' 
            WHERE ($ENTITY_PKEY_COLS) IN (SELECT DISTINCT $ENTITY_PKEY_COLS FROM $CREATE_NEEDS_CLEANUP_TABLE_SQL)
        """.trimIndent()


        /* recreate pkey queries*/

        private val CREATE_NEW_INDEX_ON_DATA_SQL = """
            CREATE UNIQUE INDEX CONCURRENTLY ${DATA_PKEY_NAME}_idx 
            ON ${DATA.name} (${NEW_DATA_PKEY_COLS.joinToString()})
        """.trimIndent()

        private val DROP_OLD_PKEY_SQL = """
            ALTER TABLE ${DATA.name} DROP CONSTRAINT $OLD_DATA_PKEY_NAME
        """.trimIndent()

        private val PROMOTE_NEW_IDX_TO_PKEY_SQL = """
            ALTER TABLE ${DATA.name} 
            ADD CONSTRAINT $DATA_PKEY_NAME PRIMARY KEY 
            USING INDEX ${DATA_PKEY_NAME}_idx;
        """.trimIndent()
    }

    override fun upgrade(): Boolean {

        createNeedsCleanupTable()

        removeDupsFromDataTable()

        hardResetLinkingIdForUpdatedEntities()

        updatePrimaryKeyOnData()

        return true
    }

    private fun createNeedsCleanupTable() {
        logger.info("About to create $CLEANUP_TABLE_NAME table using sql: $CREATE_NEEDS_CLEANUP_TABLE_SQL")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_NEEDS_CLEANUP_TABLE_SQL)
            }
        }

        logger.info("Finished creating $CLEANUP_TABLE_NAME table.")
    }

    private fun removeDupsFromDataTable() {
        logger.info("About to merge duplicates from data table using sql: $MERGE_AND_REMOVE_DUPS_SQL")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(MERGE_AND_REMOVE_DUPS_SQL)
            }
        }

        logger.info("Finished merging duplicates from data table.")
    }

    private fun hardResetLinkingIdForUpdatedEntities() {
        logger.info("About to reset linking ids in ids and data table.")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->

                logger.info("About to reset linking id in ids table using sql: $UPDATE_LINKING_ID_IN_IDS_SQL")
                stmt.execute(UPDATE_LINKING_ID_IN_IDS_SQL)

                logger.info("About to reset linking id in data table using sql: $UPDATE_LINKING_ID_IN_DATA_SQL")
                stmt.execute(UPDATE_LINKING_ID_IN_DATA_SQL)
            }
        }

        logger.info("Finished updating linking id in ids and data table.")
    }

    private fun updatePrimaryKeyOnData() {
        logger.info("About to update primary key of data table.")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->

                logger.info("About to create new index on data table using sql: $CREATE_NEW_INDEX_ON_DATA_SQL")
                stmt.execute(CREATE_NEW_INDEX_ON_DATA_SQL)

                logger.info("About to drop old primary key on data using sql: $DROP_OLD_PKEY_SQL")
                stmt.execute(DROP_OLD_PKEY_SQL)

                logger.info("About to promote new index on data to primary key using sql: $PROMOTE_NEW_IDX_TO_PKEY_SQL")
                stmt.execute(PROMOTE_NEW_IDX_TO_PKEY_SQL)
            }
        }

        logger.info("Finished updating primary key of data table.")
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_06_11.value
    }

}