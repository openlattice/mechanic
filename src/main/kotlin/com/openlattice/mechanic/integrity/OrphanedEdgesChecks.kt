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
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Mechanic check job to clear out orphaned edges.
 */
class OrphanedEdgesChecks(private val toolbox: Toolbox) : Check {

    companion object {
        private val logger = LoggerFactory.getLogger(OrphanedEdgesChecks::class.java)
    }

    override fun check(): Boolean {
        logger.info("Starting job to delete orphaned edges.")
        val sw = Stopwatch.createStarted()

        // delete edges where src or dst or association entity set does not exist
        logger.info("Starting job to delete edges, whose entity sets are non-existent anymore.")

        val deleteEntitySetsCount = toolbox.hds.connection.use { conn ->
            conn.createStatement().use {
                it.executeUpdate(deleteOrphanedEdges)
            }
        }

        logger.info(
                "Finished deleting {} edges with non-existing entity set in {} ms.",
                deleteEntitySetsCount,
                sw.elapsed(TimeUnit.MILLISECONDS)
        )


        // delete edges, where src or dst or association does not exist
        sw.reset().start()
        logger.info("Starting job to delete/clear edges, whose src, edge or dst entities are non-existent anymore.")

        val deleteEntitiesCount = toolbox.entitySets.keys.map { esId ->
            logger.info("Starting to delete/clear edges from entity set {}.", esId)
            val countDelete = toolbox.hds.connection.use { conn ->
                conn.prepareStatement(deleteOrphanedEdgesOfEntitySet).use {
                    it.setObject(1, esId)
                    it.setObject(2, esId)
                    it.setObject(3, esId)
                    it.setObject(4, esId)

                    it.executeUpdate()
                }
            }
            logger.info("Deleted {} edges, from entity set {}.", countDelete, esId)

            val countClear = toolbox.hds.connection.use { conn ->
                conn.prepareStatement(clearOrphanedEdgesOfEntitySet).use {
                    it.setObject(1, esId)
                    it.setObject(2, esId)
                    it.setObject(3, esId)
                    it.setObject(4, esId)

                    it.executeUpdate()
                }
            }
            logger.info("Cleared {} edges, from entity set {}.", countClear, esId)

            countDelete + countClear
        }.sum()

        logger.info(
                "Finished deleting/clearing {} edges with non-existing entities in {} ms.",
                deleteEntitiesCount,
                sw.elapsed(TimeUnit.MILLISECONDS)
        )


        logger.info("Finished job to delete orphaned edges.")
        return true
    }

    // @formatter:off

    // delete entries, where one or more entity sets are non-existent anymore
    private val entitySetIds =
            "SELECT ${ID.name} " +
            "FROM ${ENTITY_SETS.name}"
    private val deleteOrphanedEdges =
            "WITH entitySetIds AS ( $entitySetIds ) " +
            "DELETE FROM ${E.name} " +
            "WHERE ( ${SRC_ENTITY_SET_ID.name} NOT IN ( SELECT ${ID.name} FROM entitySetIds ) ) " +
                "OR ( ${DST_ENTITY_SET_ID.name} NOT IN ( SELECT ${ID.name} FROM entitySetIds ) ) " +
                "OR ( ${EDGE_ENTITY_SET_ID.name} NOT IN ( SELECT ${ID.name} FROM entitySetIds ) )"

    // delete entries, where one or more entities in entity set are non-existent
    private val idsOfEntitySet =
            "SELECT ${ID.name} " +
            "FROM ${IDS.name} " +
            "WHERE ${ENTITY_SET_ID.name} = ?"
    private val deleteOrphanedEdgesOfEntitySet =
            "WITH idsOfEntitySet AS ( $idsOfEntitySet ) " +
            "DELETE FROM ${E.name} " +
            "WHERE ( ${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} NOT IN ( SELECT  ${ID.name} FROM idsOfEntitySet ) ) " +
                "OR ( ${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} NOT IN ( SELECT  ${ID.name} FROM idsOfEntitySet ) ) " +
                "OR ( ${EDGE_ENTITY_SET_ID.name} = ? AND ${EDGE_ENTITY_KEY_ID.name} NOT IN ( SELECT  ${ID.name} FROM idsOfEntitySet ) )"

    // clear entries (update version), where one or more entities in entity set are non-existent
    private val clearedIdsOfEntitySet =
            "SELECT ${ID.name}, ${VERSION.name} as actual_version " +
            "FROM ${IDS.name} " +
            "WHERE ${ENTITY_SET_ID.name} = ? AND ${VERSION.name} < 0"
    private val clearOrphanedEdgesOfEntitySet =
            "WITH idsOfEntitySet AS ( $clearedIdsOfEntitySet ) " +
                    "UPDATE ${E.name} " +
                    "SET ${VERSION.name} = actual_version, " +
                        "${VERSIONS.name} = ${VERSIONS.name} || ARRAY[actual_version] " +
                    "FROM idsOfEntitySet " +
                    "WHERE ${E.name}.${VERSION.name} > 0 AND " +
                    "( " +
                        "( ${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = idsOfEntitySet.${ID.name} ) " +
                        "OR ( ${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = idsOfEntitySet.${ID.name} ) " +
                        "OR ( ${EDGE_ENTITY_SET_ID.name} = ? AND ${EDGE_ENTITY_KEY_ID.name} = idsOfEntitySet.${ID.name} ) " +
                    ")"
    // @formatter:on
}