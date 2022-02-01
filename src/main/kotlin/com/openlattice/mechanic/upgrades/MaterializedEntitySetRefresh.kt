/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

import com.geekbeast.postgres.IndexType
import com.geekbeast.postgres.PostgresColumnsIndexDefinition
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import org.slf4j.LoggerFactory

class MaterializedEntitySetRefresh(private val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(MaterializedEntitySetRefresh::class.java)
    }

    override fun upgrade(): Boolean {
        /*logger.info("Removing unused column dn_name from ${PostgresTable.ORGANIZATION_ASSEMBLIES.name} table")
        removeDbNameColumn()
        logger.info("Finished removing column.")*/

        logger.info("Starting to add columns ${PostgresColumn.REFRESH_RATE.name}, ${PostgresColumn.LAST_REFRESH.name}" +
                " to ${PostgresTable.MATERIALIZED_ENTITY_SETS.name} table.")
        addRefreshColumns()
        logger.info("Finished adding columns.")

        logger.info("Starting to add indices for columns ${PostgresColumn.ENTITY_SET_FLAGS.name}, " +
                "${PostgresColumn.LAST_REFRESH.name} to ${PostgresTable.MATERIALIZED_ENTITY_SETS.name} table.")
        addIndices()
        logger.info("Finished adding indices.")

        return true
    }

    private fun removeDbNameColumn() {
        val DB_NAME_FIELD = "db_name"

        toolbox.hds.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.executeUpdate("ALTER TABLE ${PostgresTable.ORGANIZATION_ASSEMBLIES.name} " +
                        "DROP COLUMN IF EXISTS $DB_NAME_FIELD")
            }
        }
    }

    /**
     * Add columns required to refresh automatically data changes to materialized entity sets.
     * REFRESH_RATE, LAST_REFRESH
     */
    private fun addRefreshColumns() {
        toolbox.hds.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.executeUpdate("ALTER TABLE ${PostgresTable.MATERIALIZED_ENTITY_SETS.name} " +
                        "ADD COLUMN IF NOT EXISTS ${PostgresColumn.REFRESH_RATE.sql()}, " +
                        "ADD COLUMN IF NOT EXISTS ${PostgresColumn.LAST_REFRESH.sql()}")
            }
        }
    }

    private fun addIndices() {
        val flagsIndex = PostgresColumnsIndexDefinition(
                PostgresTable.MATERIALIZED_ENTITY_SETS, PostgresColumn.ENTITY_SET_FLAGS)
                .name("materialized_entity_sets_entity_set_flags_idx")
                .method(IndexType.GIN)
                .ifNotExists()
        val lastRefreshIndex = PostgresColumnsIndexDefinition(
                PostgresTable.MATERIALIZED_ENTITY_SETS, PostgresColumn.LAST_REFRESH)
                .name("materialized_entity_sets_last_refresh_idx")
                .ifNotExists()

        toolbox.hds.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(flagsIndex.sql())
                stmt.execute(lastRefreshIndex.sql())
            }
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_03.value
    }
}