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

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ORGANIZATION_ID
import com.openlattice.postgres.PostgresTable.MATERIALIZED_ENTITY_SETS
import com.openlattice.postgres.PostgresTable.ORGANIZATION_ASSEMBLIES
import org.slf4j.LoggerFactory

class MaterializedEntitySets(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(MaterializedEntitySets::class.java)
    }

    override fun upgrade(): Boolean {
        createMaterializedEntitySetsIfNotExists()
        migrateOrganizationEntitySetFlags()
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_02_25.value
    }

    /**
     * Create materialized_entitysets table
     */
    private fun createMaterializedEntitySetsIfNotExists() {
        logger.info("Creating ${MATERIALIZED_ENTITY_SETS.name} table")
        toolbox.hds.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(MATERIALIZED_ENTITY_SETS.createTableQuery())
            }
        }
    }

    /**
     * Migrate values of materialized entity sets from organization assembly to materialized_entitysets table and
     * delete unused entity_set_ids field.
     */
    private fun migrateOrganizationEntitySetFlags() {
        logger.info("Migrate values of materialized entity sets from ${ORGANIZATION_ASSEMBLIES.name} to " +
                "${MATERIALIZED_ENTITY_SETS.name} table")

        val insertMaterializedEntitySetSql = "INSERT INTO ${MATERIALIZED_ENTITY_SETS.name} " +
                "(${ENTITY_SET_ID.name}, ${ORGANIZATION_ID.name}) " +
                "(SELECT UNNEST(entity_set_ids), ${ORGANIZATION_ID.name} from ${ORGANIZATION_ASSEMBLIES.name})"

        // todo do we drop??
        val dropEntitySetIdsColumn = "ALTER TABLE ${ORGANIZATION_ASSEMBLIES.name} DROP COLUMN entity_set_ids"

        toolbox.hds.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(insertMaterializedEntitySetSql)
            }
        }
    }
}