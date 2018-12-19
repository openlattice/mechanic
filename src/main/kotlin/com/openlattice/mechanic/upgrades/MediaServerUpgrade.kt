package com.openlattice.mechanic.upgrades

import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.google.common.util.concurrent.MoreExecutors
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.kryptnostic.rhizome.configuration.amazon.AwsLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.data.storage.AwsBlobDataService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors

//Migration for media server

private var BINARY_PROPERTY_ID_TO_FQN = mutableMapOf<UUID, String>()
private val ENTITY_SET_ID = quote("entity_set_id")
private val ID = quote("id")
private val HASH = quote("hash")
private val NAMESPACE = quote("namespace")
private val NAME = quote("name")
private val DATA_TYPE = quote("datatype")
private val logger = LoggerFactory.getLogger(MediaServerUpgrade::class.java)

class MediaServerUpgrade(private val toolbox: Toolbox) : Upgrade {
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    override fun upgrade(): Boolean {
        setUp()
        getBinaryPropertyTypes()
        migrateBinaryProperties()
        addMockS3Table()
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }

    private fun setUp() {
        val awsConfig = ResourceConfigurationLoader
                .loadConfigurationFromResource("aws.yaml", AwsLaunchConfiguration::class.java)
        val s3 = newS3Client(awsConfig)
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                s3,
                awsConfig.bucket,
                awsConfig.folder,
                DatastoreConfiguration::class.java
        )
        val byteBlobDataManager = AwsBlobDataService(config, toolbox.executor)
        this.byteBlobDataManager = byteBlobDataManager
    }

    private fun getBinaryPropertyTypes() {
        val connection = toolbox.hds.connection
        val ps = connection.prepareStatement(binaryPropertyTypesQuery())
        val rs = ps.executeQuery()
        while (rs.next()) {
            val id = rs.getObject(1) as UUID
            val namespace = rs.getString(2)
            val name = rs.getString(3)
            val fqn = "$namespace.$name"
            BINARY_PROPERTY_ID_TO_FQN[id] = fqn
        }
        rs.close()
        connection.close()
    }

    private fun migrateBinaryProperties() {
        for (entry in BINARY_PROPERTY_ID_TO_FQN) {
            logger.info("Migrating property {}", entry.value)
            val propertyTable = quote(DataTables.propertyTableName(entry.key))
            val fqn = entry.value

            addNewFqnColumn(propertyTable, fqn)

            //move binary data to s3 and store s3 key
            val conn1 = toolbox.hds.connection
            val ps1 = conn1.prepareStatement(getDataForS3(propertyTable, fqn))
            val rs = ps1.executeQuery()

            while (rs.next()) {
                val data = rs.getBytes(4)
                val hash = rs.getBytes(3)
                val hashString = PostgresDataHasher.hashObjectToHex(data, EdmPrimitiveTypeKind.Binary)

                val key = rs.getString(1) + "/" + rs.getString(2) + "/" + entry.key + "/" + hashString
                byteBlobDataManager.putObject(key, data)
                val conn2 = toolbox.hds.connection
                val ps2 = conn2.prepareStatement(storeS3Key(key, propertyTable, fqn))
                ps2.setBytes(1, hash)
                ps2.executeUpdate()
                ps2.close()
                conn2.close()
            }
            rs.close()
            conn1.close()
        }
    }

    private fun newS3Client(awsConfig: AmazonLaunchConfiguration): AmazonS3 {
        val builder = AmazonS3ClientBuilder.standard()
        builder.region = Region.getRegion(awsConfig.region.or(Regions.DEFAULT_REGION)).name
        return builder.build()
    }

    private fun addMockS3Table() {
        val connection = toolbox.hds.connection
        val ps = connection.prepareStatement(addMockS3TableQuery())
        ps.executeUpdate()
        connection.close()
    }

    private fun addNewFqnColumn(propertyTable: String, fqn: String) {
        toolbox.hds.connection.use {
            it.createStatement().use { it.execute(addNewFqnColumnQuery(propertyTable, fqn)) }
        }
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
        return "ALTER TABLE $propertyTable DROP COLUMN $fqn"
    }
}

