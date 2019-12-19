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

package com.openlattice.mechanic.regenerate

import com.google.common.base.Preconditions.checkState
import com.google.common.base.Stopwatch
import com.google.common.collect.Lists
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AclKeySet
import com.openlattice.authorization.mapstores.PrincipalTreesMapstore
import com.openlattice.data.EntityDataKey
import com.openlattice.ids.HazelcastIdGenerationService.Companion.NUM_PARTITIONS
import com.openlattice.ids.IdsGeneratingEntryProcessor
import com.openlattice.ids.IdGenerationMapstore
import com.openlattice.ids.Range
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 */
private val logger = LoggerFactory.getLogger(RegenerateIds::class.java)

class RegenerateIds(
        private val toolbox: Toolbox,
        private val idGen: IdGenerationMapstore,
        private val principalTrees: PrincipalTreesMapstore
) : Regeneration {
    override fun getSupportedVersion(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private val ranges = idGen.loadAllKeys()?.map { it to idGen.load(it) }?.toMap()?.toMutableMap() ?: mutableMapOf()
    private val rangeIndex = AtomicLong()
    private val r = Random()

    override fun regenerate(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


    fun testPrincipalTrees() {
        val keySet = principalTrees.loadAllKeys().toSet()
        logger.info("Key set: {}", keySet)
        val vm = principalTrees.loadAll(keySet)
        logger.info("Value map: {}", vm)
        principalTrees.storeAll(vm)
        val vm2 = principalTrees.loadAll(keySet)
        checkState(vm == vm2)

        var aclKey = AclKey(listOf(UUID.randomUUID(), UUID.randomUUID()))
        while (keySet.contains(aclKey)) {
            aclKey = AclKey(listOf(UUID.randomUUID(), UUID.randomUUID()))
        }

        val vList = listOf(
                AclKey(listOf(UUID.randomUUID(), UUID.randomUUID())),
                AclKey(listOf(UUID.randomUUID(), UUID.randomUUID())),
                AclKey(listOf(UUID.randomUUID(), UUID.randomUUID())),
                AclKey(listOf(UUID.randomUUID(), UUID.randomUUID()))
        )

        val vList2 = vList.subList(0, 3)
        val deletedItem = vList[3]
        val aclKeySet = AclKeySet(vList)
        val aclKeySet2 = AclKeySet(vList2)
        principalTrees.storeAll(mapOf(aclKey to aclKeySet))
        val actualKS1 = principalTrees.loadAll(setOf(aclKey))
        checkState(aclKeySet == actualKS1[aclKey])
        principalTrees.storeAll(mapOf(aclKey to aclKeySet2))
        val actualKS2 = principalTrees.loadAll(setOf(aclKey))
        checkState(aclKeySet2 == actualKS2[aclKey])
        checkState(!aclKeySet2.contains(deletedItem))
    }

//    fun migratePrincipalTrees() {
//        val keys = principalTreesOld.loadAllKeys().toSet()
//        logger.info("Number of trees: ${keys.size}")
//        val valueMap = principalTreesOld.loadAll(keys)
//        logger.info("Number of elements in map: ${valueMap.size}")
//        principalTrees.storeAll(valueMap)
//    }

    fun initRanges() {
        for (i in 0L until NUM_PARTITIONS.toLong()) {
            ranges[i] = Range(i shl 48)
        }
        idGen.storeAll(ranges)
    }

    fun assignNewEntityKeysIds() {
        toolbox.hds.connection.use {
            val w = Stopwatch.createStarted()
            //The reason we allow a primary key to exist is to speed updates that mark a data key as processed.
            it
                    .createStatement()
                    .execute(
                            "create table if not exists id_migration( id uuid primary key, entity_set_id uuid, new_id uuid )"
                    )
            val rs = it.createStatement()
                    .executeQuery("select count(*) from id_migration where new_id is null")
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
        val fetchSize = 3600000
        val w = Stopwatch.createStarted()

        val workers = Runtime.getRuntime().availableProcessors()
        val counters: MutableList<Int> = ArrayList(workers)
        val batchSize = fetchSize / workers
        val semaphore = Semaphore(workers)

        for (i in 0 until workers) {
            counters.add(0)
        }

        var dataKeys = getUnassignedEntries(fetchSize).toList()
        while (!dataKeys.isEmpty()) {
            val counter = counterIndex.getAndIncrement() % workers
            val batches = Lists.partition(dataKeys, batchSize)
            batches.forEach {
                val batch = it
                semaphore.acquireUninterruptibly()
                toolbox.executor.execute {
                    toolbox.hds.connection.use {
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
                    semaphore.release()
                }
            }

            if (counterIndex.get() > 0 && counter == 0) {
                idGen.storeAll(ranges)
            }

//            if (counterIndex.get() >= 3) {
//                break
//            }

            semaphore.acquireUninterruptibly(workers)
            dataKeys = getUnassignedEntries(fetchSize).toList()
            semaphore.release(workers)

            logger.info(
                    "Batch of size {} completed in {} ms, with counter index of {}!",
                    dataKeys.size,
                    w.elapsed(TimeUnit.MILLISECONDS), counterIndex.get()
            )
            //Let vacuum catch up
            Thread.sleep(60000)
        }

//        executor.shutdown()
//        while( !executor.awaitTermination(1, TimeUnit.DAYS ) ){}

        //Since every lock must be acquired in order for for each to proceed
        semaphore.acquireUninterruptibly(workers)
        idGen.storeAll(ranges)
    }

    fun updateExistingTables(): Long {
        val assignedCount = AtomicLong()
        val dataKeys = getEntityKeyIdStream()
        dataKeys.forEach {
            val entitySetId = it.entitySetId
            val entityKeyId = it.entityKeyId
            val newEntityKeyId = getNextId()

            toolbox.executor.execute {
                toolbox.hds.connection.use {
                    val stmt = it.createStatement()
                    stmt.use {
                        val esTableName = quote(DataTables.entityTableName(entitySetId))
                        stmt.addBatch("UPDATE $esTableName SET id = '$newEntityKeyId' WHERE id = '$entityKeyId'")
                        val entityType = toolbox.entityTypes[toolbox.entitySets[entitySetId]?.entityTypeId]
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
                    val connection = toolbox.hds.connection
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
                    val connection = toolbox.hds.connection
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

    fun fixOrganizationPermissions() {

    }

    fun reviveSouthDakotaPeople() {
        val semaphore = Semaphore(Runtime.getRuntime().availableProcessors())
        val entitySet = toolbox.entitySets[UUID.fromString("ed5716db-830b-41b7-9905-24fa82761ace")]!!
        val entityType = toolbox.entityTypes[entitySet.entityTypeId]!!

        entityType.properties.forEach {
            val ptt = quote(DataTables.propertyTableName(it))
            val version = System.currentTimeMillis()
            semaphore.acquire()
            toolbox.executor.execute {
                toolbox.hds.connection.use {
                    it.createStatement().use {
                        it.executeUpdate(
                                "UPDATE $ptt SET version = $version, versions = versions||$version " +
                                        "WHERE entity_set_id = '${entitySet.id}'"
                        )
                    }
                }
                semaphore.release()
            }
        }
        semaphore.acquire(Runtime.getRuntime().availableProcessors())
    }

    fun alterEntitySets() {
        val semaphore = Semaphore(Runtime.getRuntime().availableProcessors())
        toolbox.entitySets.keys.forEach {
            val esTableName = quote(DataTables.entityTableName(it))
            val entitySet = toolbox.entitySets[it]!!
            val entityType = toolbox.entityTypes[entitySet.entityTypeId]!!
//            entityType.properties.map {
//                logger.info(
//                        "Entity set = {}, Entity Type id = {}, Property type id = {}", entitySet.id, entityType.id, it
//                )
//                propertyTypes[it]
//            }

            semaphore.acquire()
            toolbox.executor.execute {
                toolbox.hds.connection.use {
                    var modified = false
                    try {
                        it.createStatement().use {
                            it.execute(
                                    "ALTER TABLE $esTableName " +
                                            "ADD COLUMN version bigint"
                            )
                        }
                        modified = true
                    } catch (e: PSQLException) {
                        logger.error("Unable to alter table $esTableName")
                    }
                    try {
                        it.createStatement().use {
                            it.execute(
                                    "ALTER TABLE $esTableName " +
                                            "ADD COLUMN versions bigint[]"
                            )
                        }
                        modified = true
                    } catch (e: PSQLException) {
                        logger.error("Unable to alter table $esTableName")

                    }
                    if (modified) {
                        val version = System.currentTimeMillis()
                        it.createStatement().use {
                            it.executeUpdate("UPDATE $esTableName SET version = $version, versions = ARRAY[$version]")
                        }
                    }
                }
                semaphore.release()
            }
        }
        semaphore.acquire(Runtime.getRuntime().availableProcessors())
    }

    fun updatePropertyTables() {
        toolbox.hds.connection.use {
            it.createStatement().use {
                val stmt = it
                toolbox.propertyTypes.keys.forEach {
                    val propertyTableName = quote(DataTables.propertyTableName(it))
                    stmt.addBatch(
                            "UPDATE $propertyTableName " +
                                    "SET id = new_id " +
                                    "FROM id_migration " +
                                    "WHERE $propertyTableName.id = id_migration.id"
                    )
                }
                stmt.executeBatch()
            }
        }
    }

    fun printPropertyTypeOder() {
        toolbox.propertyTypes.forEach { k, v -> logger.info("$k - ${v.type.fullQualifiedNameAsString}") }
    }

    fun updateEntityTables() {
        val semaphore = Semaphore(Runtime.getRuntime().availableProcessors())
        toolbox.entitySets.keys.forEach {
            val esTableName = quote(DataTables.entityTableName(it))
            val entitySet = toolbox.entitySets[it]!!
            val entityType = toolbox.entityTypes[entitySet.entityTypeId]!!
//            entityType.properties.map {
//                logger.info(
//                        "Entity set = {}, Entity Type id = {}, Property type id = {}", entitySet.id, entityType.id, it
//                )
//                propertyTypes[it]
//            }

            semaphore.acquire()
            toolbox.executor.execute {
                //                pgEdmManager.createEntitySet(entitySet, listOf())
                toolbox.hds.connection.use {
                    it.createStatement().use {
                        it.executeUpdate(
                                "UPDATE $esTableName " +
                                        "SET id = new_id " +
                                        "FROM id_migration " +
                                        "WHERE $esTableName.id = id_migration.id"
                        )
                    }
                }
                semaphore.release()
            }
        }
        semaphore.acquire(Runtime.getRuntime().availableProcessors())
    }

    fun updateEntityKeyIds() {
        toolbox.hds.connection.use {
            it.createStatement().use {
                it.executeUpdate(
                        "UPDATE entity_key_ids " +
                                "SET id = new_id " +
                                "FROM id_migration " +
                                "WHERE entity_key_ids.id = id_migration.id"
                )
            }
        }
    }

    fun updateEdgesTables() {
        toolbox.executor.execute {
            toolbox.hds.connection.use {
                it.createStatement().use {
                    it.executeUpdate(
                            "UPDATE edges " +
                                    "SET src_entity_key_id = id_migration.new_id " +
                                    "FROM id_migration " +
                                    "WHERE edges.src_entity_key_id = id_migration.id"
                    )
                }
            }
        }

        toolbox.executor.execute {
            toolbox.hds.connection.use {
                it.createStatement().use {
                    it.executeUpdate(
                            "UPDATE edges " +
                                    "SET dst_entity_key_id = id_migration.new_id " +
                                    "FROM id_migration " +
                                    "WHERE edges.dst_entity_key_id = id_migration.id"
                    )
                }
            }
        }
        toolbox.executor.execute {
            toolbox.hds.connection.use {
                it.createStatement().use {
                    it.executeUpdate(
                            "UPDATE edges " +
                                    "SET edge_entity_key_id = id_migration.new_id " +
                                    "FROM id_migration " +
                                    "WHERE edges.edge_entity_key_id = id_migration.id"
                    )
                    it.executeBatch()
                }
            }
        }
    }

    private fun getNextIds(count: Long): List<UUID> {
        checkState(count < Int.MAX_VALUE)
        val remainderToBeDistributed = count % NUM_PARTITIONS
        val countPerPartition = count / NUM_PARTITIONS //0 if count < NUM_PARTITIONS
        val randomRanges = (1..remainderToBeDistributed).map { r.nextInt(NUM_PARTITIONS).toLong() }.toSet()

        val processor = IdsGeneratingEntryProcessor(countPerPartition.toInt())

        val ids = if (countPerPartition > 0) {
            ranges.asSequence().flatMap { processor.getIds(it.value).asSequence() }
        } else {
            sequenceOf()
        }
        val remainingProcessor = IdsGeneratingEntryProcessor(1)
        val remainingIds = randomRanges
                .asSequence()
                .map { ranges[it] }
                .flatMap { remainingProcessor.getIds(it).asSequence() }

        return (ids + remainingIds).toList()
    }

}