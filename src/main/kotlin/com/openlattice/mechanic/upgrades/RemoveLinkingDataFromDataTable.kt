package com.openlattice.mechanic.upgrades

import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.DataTables.LAST_WRITE
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
        ).joinToString { it.name }

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

        private val DATA_COLUMNS_NAMES = listOf(
                "b_text",
                "b_uuid",
                "b_smallint",
                "b_integer",
                "b_bigint",
                "b_date",
                "b_timestamptz",
                "b_double",
                "b_boolean",
                "b_timetz",
                "n_text",
                "n_uuid",
                "n_smallint",
                "n_integer",
                "n_bigint",
                "n_date",
                "n_timestamptz",
                "n_double",
                "n_boolean",
                "n_timetz"
        )

        /** Step 1: create cleanup table queries **/

        private val CREATE_NEEDS_CLEANUP_TABLE_SQL = """
            CREATE TABLE $CLEANUP_TABLE_NAME AS 
              SELECT $NEW_DATA_PKEY_COLS
              FROM ${DATA.name}
              WHERE ${ENTITY_SET_ID.name} = ANY( ${ENTITY_SET_IDS_NEEDING_CLEANUP.joinToString { "'$it'" }} )
              GROUP BY $NEW_DATA_PKEY_COLS
              HAVING COUNT(*) > 1
        """.trimIndent()


        /** Step 2: merge + remove dups queries **/

        private fun firstNonNullAgg(colName: String): String {
            return "ARRAY_AGG( $colName ) FILTER (WHERE $colName IS NOT NULL)[ 1 ]"
        }

        private val sortVersions = """
            ARRAY(
              SELECT DISTINCT ${VERSION.name}
              FROM (
                SELECT ${VERSION.name}
                FROM UNNEST(array_cat_agg(${VERSIONS.name}))
                  AS foo(${VERSION.name})
                ORDER BY abs(foo.${VERSION.name})
              ) AS bar
            )
        """.trimIndent()

        private val maxAbsVersion = """
            (
              SELECT version 
              FROM UNNEST(array_agg(${VERSION.name}))
                AS foo( ${VERSION.name} )
              ORDER BY abs(foo.${VERSION.name}) DESC
              LIMIT 1
            )
        """.trimIndent()

        private val MERGE_AND_REMOVE_DUPS_SQL = """
            WITH deleted_rows AS (
              DELETE FROM ${DATA.name} WHERE ( $NEW_DATA_PKEY_COLS ) IN (
                SELECT DISTINCT $NEW_DATA_PKEY_COLS FROM $CLEANUP_TABLE_NAME
              ) RETURNING *
            ) INSERT INTO ${DATA.name} SELECT (
              $NEW_DATA_PKEY_COLS,
              '${IdConstants.EMPTY_ORIGIN_ID.id}' AS ${ORIGIN_ID.name},
               MAX(${LAST_WRITE.name}) AS ${LAST_WRITE.name},
               MAX(${LAST_PROPAGATE.name}) AS ${LAST_PROPAGATE.name},
               MAX(${LAST_TRANSPORT.name}) AS ${LAST_TRANSPORT.name},
               ${DATA_COLUMNS_NAMES.joinToString { firstNonNullAgg(it) }},
               $sortVersions AS ${VERSIONS.name},
               $maxAbsVersion AS ${VERSION.name}
            ) FROM deleted_rows
              GROUP BY $NEW_DATA_PKEY_COLS
        """.trimIndent()


        /** Step 3: reset linking ids queries **/

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
            SET ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' 
            WHERE ($ENTITY_PKEY_COLS) IN (SELECT DISTINCT $ENTITY_PKEY_COLS FROM $CREATE_NEEDS_CLEANUP_TABLE_SQL)
        """.trimIndent()


        /** Step 4: recreate pkey queries **/

        private val CREATE_NEW_INDEX_ON_DATA_SQL = """
            CREATE UNIQUE INDEX CONCURRENTLY ${DATA_PKEY_NAME}_idx 
            ON ${DATA.name} ( $NEW_DATA_PKEY_COLS )
        """.trimIndent()

        private val DROP_OLD_PKEY_SQL = """
            ALTER TABLE ${DATA.name} DROP CONSTRAINT $OLD_DATA_PKEY_NAME
        """.trimIndent()

        private val PROMOTE_NEW_IDX_TO_PKEY_SQL = """
            ALTER TABLE ${DATA.name} 
            ADD CONSTRAINT $DATA_PKEY_NAME PRIMARY KEY 
            USING INDEX ${DATA_PKEY_NAME}_idx;
        """.trimIndent()

        /** Step 5: update origin_id to be nullable, and null by default **/

        private val REMOVE_NOT_NULL_ORIGIN_ID_CONSTRAINT_SQL = """
            ALTER TABLE ${DATA.name} ALTER COLUMN ${ORIGIN_ID.name} DROP NOT NULL
        """.trimIndent()

        private val SET_ORIGIN_ID_DEFAULT_TO_NULL_SQL = """
            ALTER TABLE ${DATA.name} ALTER COLUMN ${ORIGIN_ID.name} SET DEFAULT NULL
        """.trimIndent()
    }

    override fun upgrade(): Boolean {

        createNeedsCleanupTable()

        removeDupsFromDataTable()

        hardResetLinkingIdForUpdatedEntities()

        updatePrimaryKeyOnData()

        dropNotNullConstraint()

        return true
    }

    /** Step 1 **/
    private fun createNeedsCleanupTable() {
        logger.info("About to create $CLEANUP_TABLE_NAME table using sql: $CREATE_NEEDS_CLEANUP_TABLE_SQL")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_NEEDS_CLEANUP_TABLE_SQL)
            }
        }

        logger.info("Finished creating $CLEANUP_TABLE_NAME table.")
    }

    /** Step 2 **/
    private fun removeDupsFromDataTable() {
        logger.info("About to merge duplicates from data table using sql: $MERGE_AND_REMOVE_DUPS_SQL")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(MERGE_AND_REMOVE_DUPS_SQL)
            }
        }

        logger.info("Finished merging duplicates from data table.")
    }

    /** Step 3 **/
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

    /** Step 4 **/
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

    private fun dropNotNullConstraint() {
        logger.info("About to drop the non-null constraint on origin_id and set default to null.")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->

                logger.info("About to drop non-null constraint using sql: $REMOVE_NOT_NULL_ORIGIN_ID_CONSTRAINT_SQL")
                stmt.execute(REMOVE_NOT_NULL_ORIGIN_ID_CONSTRAINT_SQL)

                logger.info("About to set origin_id default to null using sql: $SET_ORIGIN_ID_DEFAULT_TO_NULL_SQL")
                stmt.execute(SET_ORIGIN_ID_DEFAULT_TO_NULL_SQL)

            }

            logger.info("Finished updating origin_id column.")
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_06_11.value
    }

}