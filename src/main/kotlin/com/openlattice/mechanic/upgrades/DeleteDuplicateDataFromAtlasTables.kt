package com.openlattice.mechanic.upgrades

import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory
import java.util.*

class DeleteDuplicateDataFromAtlasTables(
        private val externalDBManager: ExternalDatabaseConnectionManager
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(DeleteDuplicateDataFromAtlasTables::class.java)

        private const val BATCH_SIZE = 100_000
        private const val MIN_ID = 187_532_734L
        private const val MAX_ID = 227_298_888L

        private val orgId = UUID.fromString("")
        private const val TABLE_NAME = ""
        private const val ID_COLUMN = ""
        private val ALL_COLS = listOf(
                ID_COLUMN,
                "..."
        )
    }

    override fun upgrade(): Boolean {
        var currId = MIN_ID

        while (currId < MAX_ID) {

            externalDBManager.connectToOrg(orgId).connection.use { conn ->
                logger.info("About to start cleaning up a batch from id {}", currId)

                val nextId = currId + BATCH_SIZE

                val numUpdates = conn.prepareStatement(cleanUpSql).use { ps ->
                    ps.setLong(1, currId)
                    ps.setLong(2, nextId)
                    ps.executeUpdate()
                }

                logger.info("Updated {} rows for batch.", numUpdates)
                currId = nextId
            }
        }

        return true
    }

    private val cleanUpSql = """
        WITH to_delete AS (
          DELETE FROM $TABLE_NAME
          WHERE ${quote(ID_COLUMN)} >= ? AND ${quote(ID_COLUMN)} < ?
          RETURNING DISTINCT ${ALL_COLS.joinToString { quote(it) }}
        )
        INSERT INTO $TABLE_NAME SELECT * FROM to_delete
    """.trimIndent()

    override fun getSupportedVersion(): Long {
        return Version.V2020_10_14.value
    }
}