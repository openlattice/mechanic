package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Callable

class ReadLinking(private val toolbox: Toolbox):Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(GraphProcessing::class.java)
    }

    override fun upgrade(): Boolean {
        logger.info("Starting to add new fields with default values to ${ENTITY_SETS.name}")
        alterEntitySetsTable()
        logger.info("Done adding new fields with default values to ${ENTITY_SETS.name}")

        logger.info("Start to drop entity set tables")
        dropEntitySetTables()
        logger.info("Done dropping entity set tables")

        return true
    }

    private fun alterEntitySetsTable() {
        val updateStatements =
                "alter table entity_sets add column if not exists linking boolean not null default false; " +
                        "alter table entity_sets add column if not exists linked_entity_sets uuid[] not null default array[]::uuid[]; " +
                        "alter table entity_sets add column if not exists external boolean not null default true;"
        toolbox.executor.submit(
                Callable {
                    toolbox.hds.connection.use {
                        it.use {
                            it.createStatement().execute(updateStatements)
                        }
                    }
                }
        ).get()
    }

    private fun dropEntitySetTables() {
        toolbox.entitySets.map { es ->
            toolbox.executor.submit(
                    Callable {
                        toolbox.hds.connection.use {
                            it.use {
                                val sql = dropEntitySetTableSql(es.key)
                                it.createStatement().execute(sql)
                                logger.info("Dropped entity set table (if exists): es_{}", es.key)
                            }
                        }
                    }
            )
        }.forEach { it.get() }
    }

    private fun dropEntitySetTableSql(esId: UUID): String {
        val entitySetTableName = DataTables.quote("es_$esId")
        return "DROP TABLE if exists $entitySetTableName"
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_10_10.value
    }

}