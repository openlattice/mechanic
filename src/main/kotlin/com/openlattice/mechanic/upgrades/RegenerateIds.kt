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

import com.google.common.base.Preconditions.checkState
import com.google.common.base.Stopwatch
import com.google.common.collect.Lists
import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.ids.HazelcastIdGenerationService.NUM_PARTITIONS
import com.openlattice.ids.IdGeneratingEntryProcessor
import com.openlattice.ids.IdGenerationMapstore
import com.openlattice.ids.Range
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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 */
private val logger = LoggerFactory.getLogger(RegenerateIds::class.java)

class RegenerateIds(
        private val pgEdmManager: PostgresEdmManager,
        private val hds: HikariDataSource,
        private val ptms: PropertyTypeMapstore,
        private val etms: EntityTypeMapstore,
        private val esms: EntitySetMapstore,
        private val idGen: IdGenerationMapstore,
        private val executor: ListeningExecutorService
) {
    private val entitySets = esms.loadAllKeys().map { it to esms.load(it) }.toMap()
    private val entityTypes = etms.loadAllKeys().map { it to etms.load(it) }.toMap()
    private val propertyTypes = ptms.loadAllKeys().map { it to ptms.load(it) }.toMap()
    private val ranges = idGen.loadAllKeys().map { it to idGen.load(it) }.toMap().toMutableMap()
    private val rangeIndex = AtomicLong()
    private val r = Random()

    fun initRanges() {
        for (i in 0L until NUM_PARTITIONS.toLong()) {
            ranges[i] = Range(i shl 48)
        }
        idGen.storeAll(ranges)
    }

    fun assignNewEntityKeysIds() {
        hds.connection.use {
            val w = Stopwatch.createStarted()
            //The reason we allow a primary key to exist is to speed updates that mark a data key as processed.
            it
                    .createStatement()
                    .execute(
                            "create table if not exists id_migration( id uuid primary key, entity_set_id uuid, new_id uuid )"
                    )
            val rs = it.createStatement()
                    .executeQuery("select count(*) from id_migration")
            val existing = if (rs.next()) {
                rs
                        .getLong("count")
            } else {
                0
            }

            //Only do re-assignment process is starting from 0
            val queuedCount =
                    if (existing > 0) {
                        existing
                    } else {
                        it.createStatement().use {
                            it.executeUpdate(
                                    "insert into id_migration (id, entity_set_id) select id, entity_set_id from entity_key_ids" +
                                            " on conflict do nothing"
                            )
                        }.toLong()
                    }

            logger.info("Queued {} data keys for migration in {} ms", queuedCount, w.elapsed(TimeUnit.MILLISECONDS))
        }

        val insertNewId = "UPDATE id_migration SET new_id = ? WHERE id = ?"
        val counterIndex = AtomicInteger()
        val fetchSize = 500000
        val w = Stopwatch.createStarted()

        val workers = Runtime.getRuntime().availableProcessors()
        val locks: MutableList<Lock> = ArrayList(workers)
        val counters: MutableList<Int> = ArrayList(workers)
        val batchSize = fetchSize / workers

        for (i in 0 until workers) {
            locks.add(ReentrantLock())
            counters.add(0)
        }


        var dataKeys = getUnassignedEntries(fetchSize).toList()
        while (!dataKeys.isEmpty()) {
            val counter = counterIndex.getAndIncrement() % workers
            val batches = Lists.partition(dataKeys, batchSize)
            batches.forEach {
                val batch = it
                try {
                    locks[counter].lock()
                    executor.execute {
                        hds.connection.use {
                            val ps = it.prepareStatement(insertNewId)
                            batch.forEach {
                                val entityKeyId = it.entityKeyId
                                val newEntityKeyId = getNextId()
                                ps.setObject(1, newEntityKeyId)
                                ps.setObject(2, entityKeyId)
                                ps.addBatch()
                                ++counters[counter]
                            }
                            ps.executeBatch()
                        }
                        logger.info("Assigned {} ids in {} ms", counters.sum(), w.elapsed(TimeUnit.MILLISECONDS))
                    }
                } finally {
                    locks[counter].unlock()
                }
            }
            if (counterIndex.get() > 0 && counter == 0) {
                idGen.storeAll(ranges)
            }
            dataKeys = getUnassignedEntries(fetchSize).toList()
        }

//        executor.shutdown()
//        while( !executor.awaitTermination(1, TimeUnit.DAYS ) ){}

        //Since every lock must be acquired in order for for each to proceed
        locks.forEach(Lock::lock)
        idGen.storeAll(ranges)
    }

    fun updateExistingTables(): Long {
        val assignedCount = AtomicLong()
        val dataKeys = getEntityKeyIdStream()
        dataKeys.forEach {
            val entitySetId = it.entitySetId
            val entityKeyId = it.entityKeyId
            val newEntityKeyId = getNextId()

            executor.execute {
                hds.connection.use {
                    val stmt = it.createStatement()
                    stmt.use {
                        val esTableName = quote(DataTables.entityTableName(entitySetId))
                        stmt.addBatch("UPDATE $esTableName SET id = '$newEntityKeyId' WHERE id = '$entityKeyId'")
                        val entityType = entityTypes[entitySets[entitySetId]?.entityTypeId]
                        entityType?.properties?.forEach {
                            val propertyTypeTable = quote(DataTables.propertyTableName(it))
                            stmt.addBatch(
                                    "UPDATE $propertyTypeTable SET id = '$newEntityKeyId' WHERE id = '$entityKeyId'"
                            )
                        }
                        stmt.addBatch("DELETE FROM id_migration where id = $entityKeyId")
                        stmt.executeBatch()
                    }
                }
            }


        }

        return assignedCount.get()
    }

    private fun getUnassignedEntries(fetchSize: Int): PostgresIterable<EntityDataKey> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(
                            "SELECT id, entity_set_id FROM id_migration WHERE new_id IS NULL LIMIT $fetchSize"
                    )
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EntityDataKey> {
                    ResultSetAdapters.entityDataKey(it)
                }
        )
    }

    private fun getEntityKeyIdStream(): PostgresIterable<EntityDataKey> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery("SELECT id, entity_set_id FROM id_migration")
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EntityDataKey> {
                    ResultSetAdapters.entityDataKey(it)
                }
        )
    }

    private fun getNextId(): UUID {
        return ranges[rangeIndex.getAndIncrement() % NUM_PARTITIONS]!!.nextId()
    }

    private fun getNextIds(count: Long): List<UUID> {
        checkState(count < Int.MAX_VALUE)
        val remainderToBeDistributed = count % NUM_PARTITIONS
        val countPerPartition = count / NUM_PARTITIONS //0 if count < NUM_PARTITIONS
        val randomRanges = (1..remainderToBeDistributed).map { r.nextInt(NUM_PARTITIONS).toLong() }.toSet()

        val processor = IdGeneratingEntryProcessor(countPerPartition.toInt())

        val ids = if (countPerPartition > 0) {
            ranges.asSequence().flatMap { processor.getIds(it.value).asSequence() }
        } else {
            sequenceOf()
        }
        val remainingProcessor = IdGeneratingEntryProcessor(1)
        val remainingIds = randomRanges
                .asSequence()
                .map { ranges[it] }
                .flatMap { remainingProcessor.getIds(it).asSequence() }

        return (ids + remainingIds).toList()
    }

}