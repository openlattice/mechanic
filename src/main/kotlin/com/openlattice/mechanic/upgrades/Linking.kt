/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit


private val IDS = PostgresTableDefinition("new_entity_key_ids")
        .addColumns(
                ENTITY_SET_ID,
                ID,
                LINKING_ID,
                ENTITY_ID,
                VERSION,
                VERSIONS,
                LAST_WRITE,
                LAST_INDEX,
                LAST_LINK,
                LAST_PROPAGATE,
                READERS,
                WRITERS
        )

private val MIGRATION_PROGRESS = PostgresTableDefinition("linking_migration")
        .addColumns(ENTITY_SET_ID, VERSION)
        .primaryKey(ENTITY_SET_ID)

private val INDEXES = listOf(PostgresColumnsIndexDefinition(IDS, ENTITY_SET_ID)
                                     .name("v2_entity_key_ids_entity_set_id_idx")
                                     .ifNotExists(),
                             PostgresColumnsIndexDefinition(IDS, ENTITY_SET_ID, ENTITY_ID)
                                     .unique()
                                     .name("v2_entity_key_ids_entity_key_idx")
                                     .ifNotExists(),
                             PostgresColumnsIndexDefinition(IDS, VERSION)
                                     .name("v2_entity_key_ids_version_idx")
                                     .ifNotExists(),
                             PostgresColumnsIndexDefinition(IDS, VERSIONS)
                                     .name("v2_entity_key_ids_versions_idx")
                                     .method(IndexMethod.GIN)
                                     .ifNotExists(),
                             PostgresColumnsIndexDefinition(IDS, LINKING_ID)
                                     .name("v2_entity_key_ids_linking_id_idx")
                                     .ifNotExists(),
                             PostgresColumnsIndexDefinition(IDS, LAST_WRITE)
                                     .name("v2_entity_key_ids_last_write_idx")
                                     .ifNotExists(),
                             PostgresColumnsIndexDefinition(IDS, LAST_INDEX)
                                     .name("v2_entity_key_ids_last_index_idx")
                                     .ifNotExists(),
                             PostgresColumnsIndexDefinition(IDS, LAST_PROPAGATE)
                                     .name("v2_entity_key_ids_last_propagate_idx")
                                     .ifNotExists(),
                             PostgresExpressionIndexDefinition(
                                     IDS,
                                     "${ENTITY_SET_ID.name}, (${LAST_INDEX.name} < ${LAST_WRITE.name})"
                             )
                                     .name("v2_entity_key_ids_needs_linking_idx")
                                     .ifNotExists(),
                             PostgresExpressionIndexDefinition(
                                     IDS,
                                     "${ENTITY_SET_ID.name}, (${LAST_LINK.name} < ${LAST_WRITE.name})"
                             )
                                     .name("v2_entity_key_ids_needs_linking_idx")
                                     .ifNotExists(),
                             PostgresExpressionIndexDefinition(
                                     IDS,
                                     "${ENTITY_SET_ID.name}, (${LAST_PROPAGATE.name} < ${LAST_WRITE.name})"
                             )
                                     .name("v2_entity_key_ids_needs_propagation_idx")
                                     .ifNotExists())
/**
 * Migrations for linking.
 */
class Linking(private val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(Linking::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.tableManager.registerTables(createNewTables())
        INDEXES.forEach { logger.info("SQL: ${it.sql()}") }

        //This will setup the max version migrated as 0
        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(initMigrationProgressTable())
            }
        }

        return if (migrateEntitySetTables()) {
            logger.info("Done migrating tables. Starting copy of entity ids.")
            toolbox.hds.connection.use {
                it.createStatement().use {
                    val w = Stopwatch.createStarted()
                    val count = it.executeUpdate(copyOverEntityIds())
                    logger.info("Copying over {} entity ids took {}s", count, w.elapsed(TimeUnit.SECONDS))
                    true
                }
            }
        } else {
            false
        }

    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }

    private fun migrateEntitySetTables(): Boolean {
        val w = Stopwatch.createStarted()
        val migrationCount = toolbox.entitySets.map { es ->
            toolbox.executor.submit(Callable {
                toolbox.hds.connection.use {
                    it.use {
                        val sql = updateIdsTable(es.key)
                        logger.info("Processing entity set $es.key (${es.value.name})")
                        it.createStatement().use {
                            val entitySetTableCount = it.executeUpdate(sql)
                            logger.info("Inserted {} entity key ids for entity set {}", entitySetTableCount, es)

                            it.executeUpdate(updateMigrationProgressTable(es.key))
                            logger.info("Updated migration progress table with version")
                            entitySetTableCount
                        }
                    }
                }
            })
        }.map { it.get() }.sum()

        logger.info("Migrated {} entities in {} s.", migrationCount, w.elapsed(TimeUnit.SECONDS))
        return true
    }
}

private fun swapTables(): String {
    return "ALTER TABLE entity_key_ids RENAME to old_entity_key_ids; " +
            "ALTER TABLE new_entity_key_ids RENAME to entity_key_ids;"
}

private fun copyOverEntityIds(): String {
    return "UPDATE ${IDS.name} " +
            "SET (${ENTITY_ID.name}) = (SELECT ${ENTITY_ID.name} FROM entity_key_ids WHERE entity_key_ids.id = ${IDS.name}.id) " +
            "WHERE ${ENTITY_ID.name} IS NULL"
}

private fun updateIdsTable(entitySetId: UUID): String {
    val entitySetTableSubquery = unmigratedEntitySetTable(entitySetId)

    return "INSERT INTO ${IDS.name} ($idColumns) $entitySetTableSubquery"
}

private val idColumns = listOf(
        ENTITY_SET_ID.name,
        ID.name,
        LINKING_ID.name,
        ENTITY_ID.name,
        VERSION.name,
        VERSIONS.name,
        LAST_WRITE.name,
        LAST_INDEX.name,
        LAST_LINK.name,
        LAST_PROPAGATE.name,
        READERS.name,
        WRITERS.name
).joinToString(",")

private fun unmigratedEntitySetTable(entitySetId: UUID): String {
    val esColumns = listOf(
            "'$entitySetId'::uuid as ${ENTITY_SET_ID.name}",
            ID.name,
            "null as ${LINKING_ID.name}",
            "null as ${ENTITY_ID.name}",
            VERSION.name,
            VERSIONS.name,
            LAST_WRITE.name,
            LAST_INDEX.name,
            "'-infinity' as ${LAST_LINK.name}",
            "'-infinity' as ${LAST_PROPAGATE.name}",
            READERS.name,
            WRITERS.name
    ).joinToString(",")
    val entitySetTableName = quote(entityTableName(entitySetId))
    //return "SELECT $esColumns FROM $entitySetTableName LEFT JOIN (SELECT ${ENTITY_ID.name} FROM entity_key_ids where entity_set_id = ?) as ids USING(id) WHERE version > (select * from migration_progress where entity_set_id = ?)"
    return "SELECT $esColumns FROM $entitySetTableName WHERE version > (select version from ${MIGRATION_PROGRESS.name} where entity_set_id = '$entitySetId') " +
            "ON CONFLICT ON CONSTRAINT new_entity_key_ids_pkey DO UPDATE SET version = EXCLUDED.version, versions = EXCLUDED.versions"
}

private fun initMigrationProgressTable(): String {
    return "INSERT INTO ${MIGRATION_PROGRESS.name} SELECT id as entity_set_id, 0 as version FROM entity_sets ON CONFLICT DO NOTHING"
}

private fun updateMigrationProgressTable(entitySetId: UUID): String {
    val entitySetTableName = quote(entityTableName(entitySetId))
    return "UPDATE ${MIGRATION_PROGRESS.name} SET (version) = (select COALESCE(max(version),0) from $entitySetTableName) " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId'"
}

private fun setConstraintsForIdsTable(): List<String> {
    return listOf(VERSION, VERSIONS, LAST_WRITE, LAST_INDEX, LAST_LINK,
                  LAST_PROPAGATE, LINKING_ID)
            .map {
                val defaultValue = when (it) {
                    VERSION -> "-1 "
                    VERSIONS -> "ARRAY[-1] "
                    else -> "'-infinity'"

                }
                "UPDATE ${IDS.name} SET ${it.name} = $defaultValue WHERE ${it.name} IS NULL;\n" +
                        "ALTER TABLE ${IDS.name} ALTER COLUMN ${it.name} SET NOT NULL;\n" +
                        "ALTER TABLE ${IDS.name} ALTER COLUMN ${it.name} SET DEFAULT $defaultValue;"
            }

}

private fun createNewTables(): List<PostgresTableDefinition> {
    IDS.addIndexes(
            PostgresColumnsIndexDefinition(IDS, ENTITY_SET_ID)
                    .name("v2_entity_key_ids_entity_set_id_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, ENTITY_SET_ID, ENTITY_ID)
                    .unique()
                    .name("v2_entity_key_ids_entity_key_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, VERSION)
                    .name("v2_entity_key_ids_version_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, VERSIONS)
                    .name("v2_entity_key_ids_versions_idx")
                    .method(IndexMethod.GIN)
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LINKING_ID)
                    .name("v2_entity_key_ids_linking_id_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LAST_WRITE)
                    .name("v2_entity_key_ids_last_write_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LAST_INDEX)
                    .name("v2_entity_key_ids_last_index_idx")
                    .ifNotExists(),
            PostgresColumnsIndexDefinition(IDS, LAST_PROPAGATE)
                    .name("v2_entity_key_ids_last_propagate_idx")
                    .ifNotExists(),
            PostgresExpressionIndexDefinition(
                    IDS,
                    ENTITY_SET_ID.name + ",(" + LAST_INDEX.name + " < " + LAST_WRITE.name + ")"
            )
                    .name("v2_entity_key_ids_needs_linking_idx")
                    .ifNotExists(),
            PostgresExpressionIndexDefinition(
                    IDS,
                    ENTITY_SET_ID.name + ",(" + LAST_LINK.name + " < " + LAST_WRITE.name + ")"
            )
                    .name("v2_entity_key_ids_needs_linking_idx")
                    .ifNotExists(),
            PostgresExpressionIndexDefinition(
                    IDS,
                    ENTITY_SET_ID.name + ",(" + LAST_PROPAGATE.name + " < " + LAST_WRITE.name + ")"
            )
                    .name("v2_entity_key_ids_needs_propagation_idx")
                    .ifNotExists()
    )
    return listOf(IDS, MIGRATION_PROGRESS)
}