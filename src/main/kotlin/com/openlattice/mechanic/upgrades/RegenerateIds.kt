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
import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.data.EntityDataKey
import com.openlattice.ids.HazelcastIdGenerationService.NUM_PARTITIONS
import com.openlattice.ids.IdGeneratingEntryProcessor
import com.openlattice.ids.IdGenerationMapstore
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
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val logger = LoggerFactory.getLogger(RegenerateIds::class.java)

class RegenerateIds(
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
    private val ranges = idGen.loadAllKeys().map { it to idGen.load(it) }.toMap()
    private val r = Random()

    init {
        hds.connection.use {
            it
                    .createStatement()
                    .execute("create table if not exists id_migration( id uuid primary key, entity_set_id uuid)")
            it
                    .createStatement()
                    .execute("create index if not exists entity_set_id_ix on id_migration(entity_set_id)")

            val migratingIdsCount = it
                    .createStatement()
                    .executeQuery("select count(*) from id_migration")
                    .getLong("count")

            if (migratingIdsCount == 0L) {
                //Build list of ids to migrate.
                val w = Stopwatch.createStarted()
                val queuedCount = it.createStatement().use {
                    it.executeUpdate("insert into id_migration select id, entity_set_id from entity_key_ids")
                }
                logger.info("Queued {} data keys for migration in {} ms", queuedCount, w.elapsed(TimeUnit.MILLISECONDS))
            }
        }
    }

    private val corruptEntitySets = entitySets.filter {
        logger.info("Checking entity set {}", it.value.name)
        !hasIdIntegrity(it.key)
    }.toMap()

    init {
        corruptEntitySets
                .asSequence()
                .map { getCorruptEntityKeyIdStream(it.key) }
                .forEach { addMissingEntityDataKeys(it) }
    }

    fun assignNewEntityKeysIds(): Long {
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
                        stmt.addBatch("UPDATE $esTableName SET id = '$newEntityKeyId' WHERE id = $entityKeyId")
                        val entityType = entityTypes[entitySets[entitySetId]?.entityTypeId]
                        entityType?.properties?.forEach {
                            val propertyTypeTable = quote(DataTables.propertyTableName(it))
                            stmt.addBatch(
                                    "UPDATE $propertyTypeTable SET id = '$newEntityKeyId' WHERE id = $entityKeyId"
                            )
                        }
                        stmt.executeBatch()
                    }
                }
            }

            //Periodically flush ranges.
            if ((assignedCount.incrementAndGet() % 10000L) == 0L) {
                idGen.storeAll(ranges)
            }
        }

        return assignedCount.get()
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
                ps.executeLargeBatch().sum()
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
                ps.executeLargeBatch().sum()
            }
        }
        if (queued != added) {
            logger.warn("Number queued is not equal to number added... something is going wrong.")
        }

        return queued
    }

    private fun countEntityKeysInEntityTable(entitySetId: UUID): Long {
        val esTableName = quote(DataTables.entityTableName(entitySetId))
        hds.connection.use {
            it.createStatement().use {
                it.executeQuery("SELECT COUNT(*) FROM $esTableName").use { return it.getLong("count") }
            }
        }
    }

    private fun countEntityKeysInIdTable(entitySetId: UUID): Long {
        return hds.connection.use {
            it.createStatement().use {
                it.executeQuery("SELECT COUNT(*) FROM id_migration where entity_set_id = '$entitySetId'").use {
                    return it.getLong("count")
                }
            }
        }
    }

    private fun hasIdIntegrity(entitySetId: UUID): Boolean {
        val idsTableCount = countEntityKeysInIdTable(entitySetId)
        val esTableCount = countEntityKeysInEntityTable(entitySetId)
        logger.info("Ids table = {}, ES table = {}", idsTableCount, esTableCount)
        return idsTableCount == esTableCount
    }

    private fun getCorruptEntityKeyIdStream(entitySetId: UUID): PostgresIterable<EntityDataKey> {
        val esTableName = quote(DataTables.entityTableName(entitySetId))
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    connection.use {
                        val stmt = connection.createStatement()
                        val sql = "SELECT id FROM $esTableName " +
                                "WHERE id NOT IN (SELECT id FROM entity_key_ids)"
                        val rs = stmt.executeQuery(sql)
                        StatementHolder(connection, stmt, rs)
                    }
                },
                Function<ResultSet, EntityDataKey> {
                    val id = ResultSetAdapters.id(it)
                    EntityDataKey(entitySetId, id)
                }
        )
    }

    private fun getEntityKeyIdStream(): PostgresIterable<EntityDataKey> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    connection.use {
                        val stmt = connection.createStatement()
                        val rs = stmt.executeQuery("SELECT id, entity_set_id FROM id_migration")
                        StatementHolder(connection, stmt, rs)
                    }
                },
                Function<ResultSet, EntityDataKey> {
                    ResultSetAdapters.entityDataKey(it)
                }
        )
    }

    private fun getNextId(): UUID {
        return ranges[r.nextInt(NUM_PARTITIONS).toLong()]!!.nextId()
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