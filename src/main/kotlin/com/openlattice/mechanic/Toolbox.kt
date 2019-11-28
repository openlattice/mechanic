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

package com.openlattice.mechanic

import com.google.common.base.Stopwatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.postgres.CitusDistributedTableDefinition
import com.openlattice.postgres.PostgresTableDefinition
import com.openlattice.postgres.PostgresTableManager
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Toolbox(
        val tableManager: PostgresTableManager,
        val hds: HikariDataSource,
        private val ptms: PropertyTypeMapstore,
        private val etms: EntityTypeMapstore,
        internal val esms: EntitySetMapstore,
        val executor: ListeningExecutorService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(Toolbox::class.java)
    }

    init {
        logger.info("Toolbox being initialized.")
    }

    val entitySets = esms.loadAll(esms.loadAllKeys().toSet()).toMap()
    val entityTypes = etms.loadAll(etms.loadAllKeys().toSet()).toMap()
    val propertyTypes = ptms.loadAll(ptms.loadAllKeys().toSet()).toMap()

    fun createTable(tableDefinition: PostgresTableDefinition) {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                logger.info("Creating the table ${tableDefinition.name}.")
                stmt.execute(tableDefinition.createTableQuery())
            }

            if (tableDefinition is CitusDistributedTableDefinition) {
                try {
                    conn.createStatement().use { stmt ->
                        logger.info("Distributing Table")
                        stmt.execute(tableDefinition.createDistributedTableQuery())
                    }
                } catch (e: Exception) {
                    logger.info("Could not distribute table: ", e)
                }
            }
        }
    }

    fun rateLimitedQuery( rate: Int, query: String, upgradeLogger: Logger ): Boolean {
        val limiter = Semaphore( rate )

        try {
            limiter.acquire()
            var insertCounter = 0
            var insertCount = 1
            val swTotal = Stopwatch.createStarted()
            hds.connection.use { conn ->
                conn.prepareStatement( query ).use { ps ->
                    val sw = Stopwatch.createStarted()
                    insertCount = ps.executeUpdate(query)
                    insertCounter += insertCount
                    upgradeLogger.info(
                            "{} rows upgraded in {} ms. Total rows upgraded so far: {} in {} ms",
                            insertCount,
                            sw.elapsed(TimeUnit.MILLISECONDS),
                            insertCounter,
                            swTotal.elapsed(TimeUnit.MILLISECONDS)
                    )
                }
            }
        } catch (e: Exception) {
            upgradeLogger.info("Something bad happened :(", e)
        } finally {
            limiter.release()
        }
        return true
    }

}