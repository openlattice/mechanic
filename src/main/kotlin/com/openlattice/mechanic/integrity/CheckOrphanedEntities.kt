package com.openlattice.mechanic.integrity

import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.checks.Check
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import org.slf4j.LoggerFactory


private val logger = LoggerFactory.getLogger(CheckOrphanedEntities::class.java)

/**
 * This class cleans up any data that has been accidentally orphaned.
 */
class CheckOrphanedEntities(private val toolbox: Toolbox) : Check {
    override fun check() : Boolean {
//        val entitySets = toolbox.entitySets.keys
//        val entitySetsInEntitiesTable = PostgresIterable<UUID>(
//                Supplier {
//                    val conn = toolbox.hds.connection
//                    val stmt = conn.createStatement()
//                    stmt.fetchSize = 100
//                    val query = stmt.executeQuery("SELECT * from es_ek_ids")
//                    StatementHolder(toolbox.hds.connection, stmt, query)
//                }, Function<ResultSet, UUID> { ResultSetAdapters.entitySetId(it) }).toSet()
//        val entitySetsToDelete = "'{" + (entitySetsInEntitiesTable - entitySets).joinToString { quote(it.toString()) } + "}'"
//        logger.info("Deleting the following entity set ids: {}", entitySetsToDelete)


        val deletedCount = toolbox.propertyTypes.keys.parallelStream()
                .mapToInt { propertyTypeId ->
                    toolbox.hds.connection.use { conn ->
                        conn.createStatement().use { stmt ->
                            val propertyTable = quote(propertyTableName(propertyTypeId))
                            stmt.executeUpdate(
                                    "DELETE from $propertyTable WHERE entity_set_id IN (SELECT entity_set_id from es_ek_ids)"
                            )
                        }
                    }
                }.sum()

        logger.info("Cleaned up $deletedCount properties.")
//        val deletedEntityCount = entitySetsToDelete.parallelStream().mapToInt {entitySetId ->
//            toolbox.hds.connection.use { conn ->
//                conn.createStatement().use { stmt ->
//                    val ekIdsCount = stmt.executeUpdate("DELETE FROM entity_key_ids WHERE entity_set_id = '$entitySetId'")
//                    val propertiesCount =
//                    logger.info("Delete ")
//                    count
//                }
//            }
//        }.sum()

        return true
    }

}


