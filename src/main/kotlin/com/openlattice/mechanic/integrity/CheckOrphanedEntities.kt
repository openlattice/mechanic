package com.openlattice.mechanic.integrity

import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.checks.Check
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier


private val logger = LoggerFactory.getLogger(CheckOrphanedEntities::class.java)

/**
 *
 * This class check
 */
class CheckOrphanedEntities(private val toolbox: Toolbox) : Check {
    override fun check() {
        val entitySets = toolbox.entitySets.keys
        val entitySetsInEntitiesTable = PostgresIterable<UUID>(
                Supplier {
                    val conn = toolbox.hds.connection
                    val stmt = conn.createStatement()
                    stmt.fetchSize = 100
                    val query = stmt.executeQuery("SELECT DISTINCT entity_set_id from entity_key_ids")
                    StatementHolder(toolbox.hds.connection, stmt, query)
                }, Function<ResultSet, UUID> { ResultSetAdapters.entitySetId(it) }).toSet()
        val entitySetsToDelete = entitySetsInEntitiesTable - entitySets
        logger.info("Deleting the following entity set ids: {}", entitySetsToDelete)


        val deletedEntityCount = entitySetsToDelete.parallelStream().mapToInt {entitySetId ->
            toolbox.hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    val ekIdsCount = stmt.executeUpdate("DELETE FROM entity_key_ids WHERE entity_set_id = $entitySetId")
                    val propertiesCount =
                    logger.info("Delete ")
                    count
                }
            }
        }.sum()


    }

}


}