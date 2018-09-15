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
import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 */
private val logger = LoggerFactory.getLogger(IntegrityChecks::class.java)

class IntegrityChecks(
        private val pgEdmManager: PostgresEdmManager,
        private val hds: HikariDataSource,
        private val ptms: PropertyTypeMapstore,
        private val etms: EntityTypeMapstore,
        private val esms: EntitySetMapstore,
        private val executor: ListeningExecutorService
) {
    private val entitySets = esms.loadAllKeys().map { it to esms.load(it) }.toMap()
    private val entityTypes = etms.loadAllKeys().map { it to etms.load(it) }.toMap()
    private val propertyTypes = ptms.loadAllKeys().map { it to ptms.load(it) }.toMap()

    private val corruptEntitySets = entitySets.filter {
        logger.info("Checking entity set {}", it.value.name)
        !hasIdIntegrity(it.key)
    }.toMap()

    fun ensureEntityKeyIdsSynchronized() {
        val latches:MutableList<CountDownLatch> = mutableListOf()
        entitySets.forEach {
            val entitySetId = it.key
            val entitySet = it.value
            val entitySetName = entitySet.name
            val esTableName = quote(DataTables.entityTableName(entitySetId))

            //Ensure that any required entity set tables exist
            pgEdmManager.createEntitySet(
                    it.value, entityTypes[it.value.entityTypeId]?.properties?.map { propertyTypes[it] })

            //Remove any entity key ids from entity_key_ids that aren't connected to an actual entity.
            val sql = "DELETE from entity_key_ids " +
                    "WHERE entity_set_id =  '$entitySetId' AND id NOT IN (SELECT id from $esTableName)"
            val latch = CountDownLatch(1)
            latches.add(latch)
            executor.execute {
                hds.connection.use {
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
                        propertyTypes.forEach {
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

                            pgEdmManager.createPropertyTypeTableIfNotExist( it.value )
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

    private fun addMissingEntityDataKeys(dataKeys: PostgresIterable<EntityDataKey>): Long {
        //Add data key to processing queue
        val queued = hds.connection.use {
            val ps = it.prepareStatement("INSERT INTO id_migration (id, entity_set_id) VALUES (?,?)")
            ps.use {
                dataKeys.forEach {
                    ps.setObject(1, it.entitySetId)
                    ps.setObject(2, it.entityKeyId)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }

        //Add data key to entity_key_ids table
        val added = hds.connection.use {
            val ps = it.prepareStatement("INSERT INTO entity_key_ids (entity_set_id, entity_id, id) VALUES (?,?,?)")
            ps.use {
                dataKeys.forEach {
                    ps.setObject(1, it.entitySetId)
                    ps.setString(2, it.entityKeyId.toString())
                    ps.setObject(3, it.entityKeyId)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }
        if (queued != added) {
            logger.warn("Number queued is not equal to number added... something is going wrong.")
        }

        return queued.toLong()
    }

    private fun countEntityKeysInEntityTable(entitySetId: UUID): Long {
        val esTableName = quote(DataTables.entityTableName(entitySetId))
        hds.connection.use {
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
        return hds.connection.use {
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

    private fun getCorruptEntityKeyIdStream(entitySetId: UUID): PostgresIterable<EntityDataKey> {
        val esTableName = quote(DataTables.entityTableName(entitySetId))
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    val stmt = connection.createStatement()
                    val sql = "SELECT id FROM $esTableName " +
                            "WHERE id NOT IN (SELECT id FROM entity_key_ids)"
                    val rs = stmt.executeQuery(sql)
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EntityDataKey> {
                    val id = ResultSetAdapters.id(it)
                    EntityDataKey(entitySetId, id)
                }
        )
    }
}