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
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.ResultSetAdapters
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolderSupplier
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

data class LinkedEntityRow(val linkingId: UUID, val originId: UUID, val propertyTypeId: UUID)
data class LinkedEntityMetaData(val linkingId: UUID, val originId: UUID, val entitySetId: UUID, val partition: Int)

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
        logger.info("Starting task to clear orphaned linking property types values.")
        val sw = Stopwatch.createStarted()

        do {
            sw.reset().start()

            val count = toolbox.hds.connection.use { conn ->
                conn.createStatement().use {
                    it.executeUpdate(tombStoneSql)
                }
            }

            logger.info(
                    "Cleared $count orphaned linking property types in ${DATA.name} table in " +
                            "${sw.elapsed(TimeUnit.MILLISECONDS)} ms."
            )
        } while (count > 0)
    }

    private fun deleteOrphanedLinkingProperties() {
        logger.info("Starting task to delete orphaned linking property types values.")
        val sw = Stopwatch.createStarted()
        val swTotal = Stopwatch.createStarted()
        var countTotal = 0L

        selectAllLinkingIds()
                .groupBy { it.partition }
                .asSequence()
                .chunked(fetchDelete)
                .forEachIndexed { batchNum, idsBatchByPartition ->
                    sw.reset().start()
                    var count = 0L

                    idsBatchByPartition.forEach {

                        val linkingIds = it.value.map { it.linkingId }.toSet()
                        val originIds = it.value.map { it.originId }.toSet()
                        val entitySetIds = it.value.map { it.entitySetId }.toSet()
                        val partition = it.key

                        BasePostgresIterable(
                                PreparedStatementHolderSupplier(toolbox.hds, selectDeletableSql) { ps ->
                                    val originIdsArray = PostgresArrays.createUuidArray(ps.connection, originIds)
                                    val entitySetIdsArray = PostgresArrays.createUuidArray(ps.connection, entitySetIds)
                                    val linkingIdsArray = PostgresArrays.createUuidArray(ps.connection, linkingIds)

                                    ps.setArray(1, originIdsArray)
                                    ps.setArray(2, entitySetIdsArray)
                                    ps.setArray(3, linkingIdsArray)
                                    ps.setArray(4, originIdsArray)
                                    ps.setArray(5, entitySetIdsArray)
                                    ps.setInt(6, partition)
                                }
                        ) { rs ->
                            LinkedEntityRow(
                                    ResultSetAdapters.id(rs),
                                    ResultSetAdapters.originId(rs),
                                    ResultSetAdapters.propertyTypeId(rs)
                            )
                        }
                                .groupBy { it.linkingId }
                                .forEach { (linkingId, deletablesOfLinkingId) ->
                                    deletablesOfLinkingId
                                            .groupBy { it.originId }
                                            .forEach { (originId, deletablesOfOriginId) ->
                                                count += toolbox.hds.connection.use { conn ->
                                                    val propertyTypeIdsArr = PostgresArrays.createUuidArray(
                                                            conn,
                                                            deletablesOfOriginId.map { it.propertyTypeId }
                                                    )
                                                    conn.prepareStatement(deleteSql).use {
                                                        it.setObject(1, linkingId)
                                                        it.setObject(2, originId)
                                                        it.setInt(3, partition) // same partition
                                                        it.setArray(4, propertyTypeIdsArr)
                                                        it.executeUpdate()
                                                    }
                                                }
                                            }
                                }
                    }

                    countTotal += count

                    logger.info(
                            "Batch number $batchNum: Deleted $count orphaned linking property types from " +
                                    "${DATA.name} table in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms."
                    )
                }

        logger.info(
                "Finished task to delete orphaned linking property types values. Deleted $countTotal total orphaned " +
                        "linking property types in ${swTotal.elapsed(TimeUnit.MILLISECONDS)} ms."
        )
    }

    private fun selectAllLinkingIds(): BasePostgresIterable<LinkedEntityMetaData> {
        return BasePostgresIterable(
                StatementHolderSupplier(toolbox.hds, selectAllLinkingIds, fetchDelete)
        ) { rs ->
            LinkedEntityMetaData(
                    ResultSetAdapters.id(rs),
                    ResultSetAdapters.originId(rs),
                    ResultSetAdapters.entitySetId(rs),
                    ResultSetAdapters.partition(rs)
            )
        }
    }


    private val limitClear = 3000
    private val fetchDelete = 3000

    // @formatter:off
    private val tombStoneSql =
            "WITH cleared_entities AS " +
            "( " +
                "SELECT ${ID.name}, ${PROPERTY_TYPE_ID.name}, ${VERSION.name} as actual_version " +
                "FROM ${DATA.name} " +
                "WHERE " +
                    "${VERSION.name} < 0 " +
                    "AND ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
                "LIMIT $limitClear " +
            ") " +
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
                "AND d.${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}' "


    private val selectAllLinkingIds =
            "SELECT ${ID.name}, ${ORIGIN_ID.name}, ${ENTITY_SET_ID.name}, ${PARTITION.name} " +
            "FROM ${DATA.name} " +
            "WHERE " +
                "${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
                "AND ${VERSION.name} > 0 "

    private val selectDeletableSql =
            "SELECT ${ID.name}, ${ORIGIN_ID.name}, ${PROPERTY_TYPE_ID.name} " +
            "FROM ${DATA.name} " +
            "WHERE " +
                "( ${ORIGIN_ID.name}, ${PROPERTY_TYPE_ID.name} ) NOT IN " +
                    "( " +
                        "SELECT ${ID.name}, ${PROPERTY_TYPE_ID.name} " +
                        "FROM ${DATA.name} " +
                        "WHERE ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
                            "AND ${ID.name} = ANY( ? ) " +
                            "AND ${ENTITY_SET_ID.name} = ANY( ? ) " +
                            "AND ${VERSION.name} > 0 " +
                    ") " +
                "AND ${ID.name} = ANY( ? ) " +
                "AND ${ORIGIN_ID.name} = ANY( ? ) " +
                "AND ${ENTITY_SET_ID.name} = ANY( ? ) " +
                "AND ${PARTITION.name} = ? " +
                "AND ${VERSION.name} > 0 " +
                "AND ${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}' "

    private val deleteSql =
            "DELETE FROM ${DATA.name} " +
            "WHERE ${ID.name} = ? " +
                    "AND ${ORIGIN_ID.name} = ? " +
                    "AND ${PARTITION.name} = ? " +
                    "AND ${PROPERTY_TYPE_ID.name} = ANY( ? )"
    // @formatter:on
}