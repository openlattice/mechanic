package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import org.slf4j.LoggerFactory
import java.util.*

class RemoveLinkingDataFromDataTable(val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(RemoveLinkingDataFromDataTable::class.java)

        private val PERSON_ENTITY_TYPE_ID = UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab")

        /**
         * Bind order:
         * 1) entity set id
         * 2) partitions
         * 3) entity set id
         * 4) partitions
         */
        private val DELETE_SQL = """
          DELETE FROM ${DATA.name} 
          WHERE 
            ${ENTITY_SET_ID.name} = ? 
            AND ${PARTITION.name} = ANY(?)
            AND ${ID.name} NOT IN (
              SELECT DISTINCT ${ID.name} 
              FROM ${IDS.name} 
              WHERE
                ${ENTITY_SET_ID.name} = ?
                AND ${PARTITION.name} = ANY(?)
            )
        """.trimIndent()
    }

    override fun upgrade(): Boolean {
        toolbox.entitySets.values.filter { it.entityTypeId == PERSON_ENTITY_TYPE_ID }.forEach {

            logger.info("About to clean up entity set ${it.name} [${it.id}]")

            val numUpdates = toolbox.hds.connection.use { conn ->
                val partitions = PostgresArrays.createIntArray(conn, it.partitions)

                conn.prepareStatement(DELETE_SQL).use { ps ->
                    ps.setObject(1, it.id)
                    ps.setArray(2, partitions)
                    ps.setObject(3, it.id)
                    ps.setArray(4, partitions)

                    ps.executeUpdate()
                }
            }

            logger.info("Finished cleaning up $numUpdates rows for entity set ${it.name} [${it.id}]")

        }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_06_11.value
    }

}