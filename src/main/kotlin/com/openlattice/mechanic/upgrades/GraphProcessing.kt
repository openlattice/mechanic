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
import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.postgres.PostgresIndexDefinition
import com.openlattice.postgres.PostgresTableDefinition
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit


private val MIGRATION_PROGRESS = PostgresTableDefinition("linking_migration")
        .addColumns(ENTITY_SET_ID, VERSION)
        .primaryKey(ENTITY_SET_ID)

private fun buildIndexDefinitions(propertyType: PropertyType): List<PostgresIndexDefinition> {
    return listOf()
//    val propertyTypeTable = DataTables.buildPropertyTableDefinition(propertyType)
//    return listOf(
//            PostgresColumnsIndexDefinition(
//                    propertyTypeTable,
//                    LAST_PROPAGATE
//            ).ifNotExists(),
//            PostgresExpressionIndexDefinition(
//                    propertyTypeTable,
//                    "(${LAST_PROPAGATE.name} < ${LAST_WRITE.name})"
//            )
//                    .name(quote("${propertyTypeTable.name}_needs_propagation_idx"))
//                    .ifNotExists(),
//            PostgresExpressionIndexDefinition(
//                    propertyTypeTable,
//                    "(${LAST_PROPAGATE.name} >= ${LAST_WRITE.name})"
//            )
//                    .name(quote("${propertyTypeTable.name}_active_idx"))
//                    .ifNotExists()
//    )
}

/**
 * Migrations for linking.
 */
class GraphProcessing(private val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(GraphProcessing::class.java)
    }

    override fun upgrade(): Boolean {
        return if (migratePropertyTypeTables()) {
            logger.info("Done migrating tables. Starting updating property type tables with last_propagate field")
            updatePropertyTablesLastPropagateField()
        } else {
            false
        }

    }

    private fun updatePropertyTablesLastPropagateField(): Boolean {
        toolbox.propertyTypes.map { pt ->
            toolbox.executor.submit(
                    Callable {
                        toolbox.hds.connection.use {
                            it.use {
                                val w = Stopwatch.createStarted()
                                val sql = updatePropertyTypeTableSql(pt.key)
                                val count = it.createStatement().use {
                                    it.execute(sql)
                                    it.execute(alterPropertyTypeTableWithDefaults(pt.key))
                                }

                                logger.info(
                                        "Set {} default values for property type {} in {} ms",
                                        count,
                                        pt.value.type,
                                        w.elapsed(TimeUnit.MILLISECONDS)
                                )

                            }
                        }
                    }
            )
        }.forEach { it.get() }
        return true
    }


    private fun migratePropertyTypeTables(): Boolean {
        toolbox.propertyTypes.map { pt ->
            toolbox.executor.submit(
                    Callable {
                        toolbox.hds.connection.use {
                            it.use {
                                val sql = alterPropertyTypeTableSql(pt.key)
                                it.createStatement().use { it.execute(sql) }
                                val indexSql = buildIndexDefinitions(pt.value)
                                it.createStatement().use { it.execute(sql) }
                                logger.info("Added column to property type:{}", pt.value.type)
                            }
                        }
                    }
            )
        }.forEach { it.get() }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }
}

private fun alterPropertyTypeTableWithDefaults(propertyTypeId: UUID): String {
    val propertyTableName = quote(propertyTableName(propertyTypeId))
    return "ALTER TABLE $propertyTableName ALTER COLUMN ${LAST_PROPAGATE.name} SET DEFAULT now();" +
            "ALTER TABLE $propertyTableName ALTER COLUMN ${LAST_PROPAGATE.name} SET NOT NULL;"
}

private fun updatePropertyTypeTableSql(propertyTypeId: UUID): String {
    val propertyTableName = quote(propertyTableName(propertyTypeId))
    return "UPDATE $propertyTableName SET ${LAST_PROPAGATE.name} = now() WHERE LAST_PROPAGATE IS NULL"
}

private fun alterPropertyTypeTableSql(propertyTypeId: UUID): String {
    val propertyTableName = quote(propertyTableName(propertyTypeId))
    return "ALTER TABLE $propertyTableName DROP COLUMN IF EXISTS ${LAST_PROPAGATE.name}; " +
            "ALTER TABLE $propertyTableName ADD COLUMN IF NOT EXISTS ${LAST_PROPAGATE.name} ${PostgresDatatype.TIMESTAMPTZ.sql()}; "
}

private fun swapTables(): String {
    return "ALTER TABLE entity_key_ids RENAME to old_entity_key_ids; " +
            "ALTER TABLE new_entity_key_ids RENAME to entity_key_ids;"
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