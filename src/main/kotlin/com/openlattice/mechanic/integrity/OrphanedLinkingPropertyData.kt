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
package com.openlattice.mechanic.integrity

import com.google.common.base.Stopwatch
import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class OrphanedLinkingPropertyData(private val toolbox: Toolbox) : Check {
    companion object {
        private val logger = LoggerFactory.getLogger(OrphanedLinkingPropertyData::class.java)
    }

    override fun check(): Boolean {
        tombstoneOrphanedLinkingProperties()
        deleteOrphanedLinkingProperties()
        return true
    }

    private fun tombstoneOrphanedLinkingProperties() {
        val sw = Stopwatch.createStarted()
        logger.info("Starting task to clear orphaned linking property types values.")
        val count = toolbox.hds.connection.use { conn ->
            conn.createStatement().use {
                it.executeUpdate(tombStoneSql)
            }
        }
        logger.info("Cleared $count orphaned linking entries in ${DATA.name} table in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
    }

    private fun deleteOrphanedLinkingProperties() {
        val sw = Stopwatch.createStarted()
        logger.info("Starting task to delete orphaned linking property types values.")
        val count = toolbox.hds.connection.use { conn ->
            conn.createStatement().use {
                it.executeUpdate(deleteSql)
            }
        }
        logger.info("Deleted $count orphaned linking property types in ${DATA.name} table in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
    }

    // @formatter:off
    private val tombStoneSql =
            "WITH cleared_entities AS " +
            "( " +
                "SELECT ${ID.name}, ${PROPERTY_TYPE_ID.name}, ${VERSION.name} as actual_version " +
                "FROM ${DATA.name} " +
                "WHERE " +
                    "${VERSION.name} < 0 " +
                    "AND ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
            ")" +
            "UPDATE ${DATA.name} as d " +
            "SET " +
                "${VERSION.name} = actual_version, " +
                "${VERSIONS.name} = ${VERSIONS.name} || ARRAY[actual_version], " +
                "$LAST_WRITE_FIELD = 'now()' " +
            "FROM cleared_entities " +
            "WHERE " +
                "d.${ORIGIN_ID.name} = cleared_entities.${ID.name} " +
                "AND d.${PROPERTY_TYPE_ID.name} = cleared_entities.${PROPERTY_TYPE_ID.name} " +
                "AND d.${VERSION.name} > 0 " +
                "AND d.${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}'"


    private val deleteSql =
            "DELETE FROM ${DATA.name} " +
            "WHERE " +
                "( ${ORIGIN_ID.name}, ${PROPERTY_TYPE_ID.name} ) NOT IN " +
                    "( " +
                        "SELECT ${ID.name}, ${PROPERTY_TYPE_ID.name} " +
                        "FROM ${DATA.name} " +
                        "WHERE ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
                    ") " +
                "AND ${VERSION.name} > 0 " +
                "AND ${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}' "
    // @formatter:on
}