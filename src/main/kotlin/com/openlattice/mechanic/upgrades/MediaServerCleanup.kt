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

import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.aws.AwsBlobDataService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.StatementHolderSupplier
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import java.util.*

//Migration for media server


private val ENTITY_SET_ID = quote("entity_set_id")
private val ID = quote("id")
private val HASH = quote("hash")
private val NAMESPACE = quote("namespace")
private val NAME = quote("name")
private val DATA_TYPE = quote("datatype")
private val logger = LoggerFactory.getLogger(MediaServerCleanup::class.java)

class MediaServerCleanup(private val toolbox: Toolbox) : Upgrade {

    private val byteBlobDataManager: ByteBlobDataManager
    private val binaryProperties =
            BasePostgresIterable(StatementHolderSupplier(toolbox.hds, binaryPropertyTypesQuery())) { rs ->
                val id = rs.getObject(1) as UUID
                val namespace = rs.getString(2)
                val name = rs.getString(3)
                val fqn = "$namespace.$name"

                id to fqn
            }.toMap()

    init {
        val config = toolbox.configurationLoader.load(DatastoreConfiguration::class.java)
        val byteBlobDataManager = AwsBlobDataService(config, toolbox.executor)
        this.byteBlobDataManager = byteBlobDataManager
    }

    override fun upgrade(): Boolean {
        cleanupBinaryProperties()
        addMockS3Table()
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }

    private fun cleanupBinaryProperties() {

        logger.info("Swapping columns...")
        for (entry in binaryProperties) {
            try {
                logger.info("Swapping property {}", entry.value)
                val propertyTable = quote(DataTables.propertyTableName(entry.key))
                val fqn = entry.value
                swapFqnColumns(propertyTable, fqn)
            } catch (ex: PSQLException) {
                logger.info("Unable to cleanup {}.", entry, ex)
            }
        }

        logger.info("Deleting old columns...")

        for (entry in binaryProperties) {
            try {
                logger.info("Swapping property {}", entry.value)
                val propertyTable = quote(DataTables.propertyTableName(entry.key))
                val fqn = entry.value
                removeOldFqnColumn(propertyTable, fqn)
            } catch (ex: PSQLException) {
                logger.info("Unable to cleanup {}.", entry.value, ex)
            }
        }

    }

    private fun addMockS3Table() {
        val connection = toolbox.hds.connection
        val ps = connection.prepareStatement(addMockS3TableQuery())
        ps.executeUpdate()
        connection.close()
    }

    private fun swapFqnColumns(propertyTable: String, fqn: String) {
        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(
                        "ALTER TABLE $propertyTable RENAME COLUMN ${quote(fqn)} TO ${quote(fqn + "_old")}"
                )
            }
            it.createStatement().use {
                it.execute(
                        "ALTER TABLE $propertyTable RENAME COLUMN  ${quote(fqn + "_new")} to ${quote(fqn)}"
                )
            }
        }
    }

    private fun removeOldFqnColumn(propertyTable: String, fqnOld: String) {
        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(
                        removeOldFqnColumnQuery(propertyTable, fqnOld)
                )
            }
        }
    }

    //sql queries
    private fun binaryPropertyTypesQuery(): String {
        return "SELECT $ID, $NAMESPACE, $NAME FROM property_types WHERE $DATA_TYPE = 'Binary'"
    }

    private fun addMockS3TableQuery(): String {
        return "CREATE TABLE mock_s3_bucket (key text, object bytea)"
    }

    private fun addNewFqnColumnQuery(propertyTable: String, fqn: String): String {
        return "ALTER TABLE $propertyTable ADD COLUMN IF NOT EXISTS ${quote(fqn + "_new")} text"
    }

    private fun getDataForS3(propertyTable: String, fqn: String): String {
        return "SELECT $ENTITY_SET_ID, $ID, $HASH, ${quote(fqn)} from $propertyTable"
    }

    private fun storeS3Key(key: String, propertyTable: String, fqn: String): String {
        return "UPDATE $propertyTable SET ${quote(fqn + "_new")} = '$key' where $HASH = ?"
    }

    private fun removeOldFqnColumnQuery(propertyTable: String, fqn: String): String {
        return "ALTER TABLE $propertyTable DROP COLUMN ${quote(fqn + "_old")}"
    }
}

