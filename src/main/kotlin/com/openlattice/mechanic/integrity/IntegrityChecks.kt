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

package com.openlattice.mechanic.integrity

import com.google.common.base.Stopwatch
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 *
 */
private val logger = LoggerFactory.getLogger(IntegrityChecks::class.java)

@SuppressFBWarnings(value = ["SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"])
class IntegrityChecks(private val toolbox: Toolbox) : Check {
    override fun check(): Boolean {
        logger.info("Integrity checks aren't fully implemented.")
        toolbox.entitySets.filter {
            logger.info("Checking entity set {}", it.value.name)
            !hasIdIntegrity(it.key)
        }.toMap()

        return true
    }

    fun ensureEntityKeyIdsSynchronized() {
        val latches: MutableList<CountDownLatch> = mutableListOf()
        toolbox.entitySets.forEach {
            val entitySetId = it.key
            val entitySet = it.value
            val entitySetName = entitySet.name
            val esTableName = quote(DataTables.entityTableName(entitySetId))

            //Ensure that any required entity set tables exist
//            pgEdmManager.createEntitySet(
//                    it.value, entityTypes[it.value.entityTypeId]?.properties?.map { propertyTypes[it] })

            //Remove any entity key ids from entity_key_ids that aren't connected to an actual entity.
            val sql = "DELETE from entity_key_ids " +
                    "WHERE entity_set_id =  '$entitySetId' AND id NOT IN (SELECT id from $esTableName)"
            val latch = CountDownLatch(1)
            latches.add(latch)
            toolbox.executor.execute {
                toolbox.hds.connection.use {
                    val w = Stopwatch.createStarted()
                    it.createStatement().use {
                        logger.info(
                                "Deleting {} entity key ids not connected to {} in {} ms ",
                                it.executeUpdate(sql),
                                entitySetName,
                                w.elapsed(TimeUnit.MILLISECONDS)
                        )
                    }

                    //Remove any entity key ids from property types not connected to an actual entity.

                    val ptStmt = it.createStatement()
                    w.reset()
                    w.start()
                    ptStmt.use {
                        toolbox.propertyTypes.forEach {
                            val ptTableName = quote(DataTables.propertyTableName(it.key))
                            val ptSql = "DELETE from $ptTableName " +
                                    "WHERE entity_set_id = '$entitySetId' AND id in (select id from " +
                                    "(select entity_key_ids.entity_set_id, entity_key_ids.id, $esTableName.id as es_id from entity_key_ids LEFT JOIN $esTableName USING(id) " +
                                    "WHERE entity_key_ids.entity_set_id = 'addfe773-ce01-423c-aadf-9cd1e122c881' AND $esTableName.id is null) as sq)"
                            val ptSql2 = "DELETE FROM entity_key_ids " +
                                    "WHERE id in (select id from " +
                                    "(select entity_key_ids.entity_set_id, entity_key_ids.id, $esTableName.id as es_id from entity_key_ids LEFT JOIN $esTableName USING(id) " +
                                    "WHERE entity_key_ids.entity_set_id = 'addfe773-ce01-423c-aadf-9cd1e122c881' AND $esTableName.id is null) as sq)"
                            val ptSql3 = "DELETE from id_migration " +
                                    "WHERE entity_set_id = '$entitySetId' AND id in (select id from " +
                                    "(select entity_key_ids.entity_set_id, entity_key_ids.id, $esTableName.id as es_id from entity_key_ids LEFT JOIN $esTableName USING(id) " +
                                    "WHERE entity_key_ids.entity_set_id = 'addfe773-ce01-423c-aadf-9cd1e122c881' AND $esTableName.id is null) as sq)"

                            //"DELETE from $ptTableName " +
                            //"WHERE entity_set_id = '$entitySetId' AND id NOT IN (SELECT id from $esTableName)"

//                            pgEdmManager.createPropertyTypeTableIfNotExist(it.value)
                            logger.info(
                                    "Submitting delete for entity set{} and property type {}",
                                    entitySetName,
                                    it.value.type.fullQualifiedNameAsString
                            )
                            ptStmt.addBatch(ptSql)
                            ptStmt.addBatch(ptSql2)
                            ptStmt.addBatch(ptSql3)

                        }
                        logger.info(
                                "Deleting {} properties not connected to {} took {} ms",
                                entitySetName,
                                ptStmt.executeBatch().sum(),
                                w.elapsed(TimeUnit.MILLISECONDS)
                        )
                    }
                }
                latch.countDown()
            }
        }
        latches.forEach(CountDownLatch::await)
    }

    private fun countEntityKeysInEntityTable(entitySetId: UUID): Long {
        val esTableName = quote(DataTables.entityTableName(entitySetId))
        toolbox.hds.connection.use {
            it.createStatement().use {
                it.executeQuery("SELECT COUNT(*) FROM $esTableName").use {
                    return if (it.next()) {
                        it.getLong("count")
                    } else {
                        -2
                    }
                }
            }
        }
    }

    private fun countEntityKeysInIdTable(entitySetId: UUID): Long {
        return toolbox.hds.connection.use {
            it.createStatement().use {
                it.executeQuery("SELECT COUNT(*) FROM entity_key_ids where entity_set_id = '$entitySetId'").use {
                    return if (it.next()) {
                        it.getLong("count")
                    } else {
                        -1
                    }
                }
            }
        }
    }

    private fun hasIdIntegrity(entitySetId: UUID): Boolean {
        val idsTableCount = countEntityKeysInIdTable(entitySetId)
        val esTableCount = countEntityKeysInEntityTable(entitySetId)
        logger.info("Ids table = {}, ES table = {}", idsTableCount, esTableCount)
        return idsTableCount > esTableCount
    }

}