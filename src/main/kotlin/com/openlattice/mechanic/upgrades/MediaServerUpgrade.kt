package com.openlattice.mechanic.upgrades

import com.openlattice.ResourceConfigurationLoader
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.LocalAwsBlobDataService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.quote

//Migration for media server

private val BINARY_PROPERTY_ID_TO_FQN = mapOf("90abc3fb-f0c4-45e5-b385-8cf70c06ef81" to "ol.digitalsignature",
        "881c2601-f3b9-40cd-bb9b-86e811182156" to "ol.licenseplateimage",
        "faf3c011-b386-4c51-a1bb-4bad27b4083f" to "ol.pictureback",
        "e8174f66-4b80-4f8c-bcd3-2924d5ccd6d7" to "ol.picturefront",
        "0f95049c-b668-49b1-bf32-f26835471b0a" to "ol.vehicleimage",
        "45aa6695-a7e7-46b6-96bd-782e6aa9ac13" to "publicsafety.mugshot"
        )

private val ENTITY_SET_ID = quote("entity_set_id")
private val ID = quote("id")
private val HASH = quote("hash")

class MediaServerUpgrade(private val toolbox: Toolbox) : Upgrade {
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    override fun upgrade(): Boolean {
        setUp()
        addMockS3Table()
        migrateBinaryProperties()
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }

    fun setUp() {
        val config = ResourceConfigurationLoader.loadConfiguration(DatastoreConfiguration::class.java)
        val byteBlobDataManager = LocalAwsBlobDataService(config)
        this.byteBlobDataManager = byteBlobDataManager
    }

    fun addMockS3Table() {
        val connection = toolbox.hds.connection
        val ps = connection.prepareStatement(addMockS3TableQuery())
        ps.executeUpdate()
        connection.close()
    }

    fun migrateBinaryProperties() {
        for (entry in BINARY_PROPERTY_ID_TO_FQN) {
            val propertyTable = quote("pt_".plus(entry.key))
            val fqn = quote(entry.value)
            val fqnOld = quote(entry.value.plus("_data"))

            //create new columnn to store s3 keys and rename existing fqn column for easier deletion later
            val setUpConn1 = toolbox.hds.connection
            val setUpPS1 = setUpConn1.prepareStatement(renameFqnColumn(propertyTable, fqn, fqnOld))
            setUpPS1.executeUpdate()
            setUpConn1.close()

            val setUpConn2 = toolbox.hds.connection
            val setUpPS2 = setUpConn2.prepareStatement(addNewFqnColumn(propertyTable, fqn))
            setUpPS2.executeUpdate()
            setUpConn2.close()

            //prepare statement to get relevant data for s3
            val conn1 = toolbox.hds.connection
            val ps1 = conn1.prepareStatement(getDataForS3(propertyTable, fqnOld))

            //move binary data to s3 and store s3 key
            val rs = ps1.executeQuery()
            while(rs.next()) {
                val data = rs.getBytes(4)
                val hash = rs.getBytes(3)
                val key = rs.getString(1) + "/" + rs.getString(2) + "/" + entry.key + "/" + hash
                byteBlobDataManager.putObject(key, data)
                val conn2 = toolbox.hds.connection
                val ps2 = conn2.prepareStatement(storeS3Key(key, propertyTable, fqn))
                ps2.setBytes(1, hash)
                ps2.executeUpdate()
                conn2.close()
            }
            rs.close()
            conn1.close()

            //remove old fqn column
            val cleanUpConn = toolbox.hds.connection
            val cleanUpPS = cleanUpConn.prepareStatement(removeOldFqnColumn(propertyTable, fqnOld))
            cleanUpPS.executeUpdate()
            cleanUpConn.close()
        }
    }

    fun addMockS3TableQuery() : String {
        return "CREATE TABLE mock_s3_bucket (key text, object bytea)"
    }

    fun renameFqnColumn(propertyTable: String, fqn: String, fqnOld: String) : String {
        return "ALTER TABLE $propertyTable RENAME COLUMN $fqn TO $fqnOld"
    }

    fun addNewFqnColumn(propertyTable: String, fqn: String) : String {
        return "ALTER TABLE $propertyTable ADD COLUMN $fqn text"
    }

    fun getDataForS3(propertyTable: String, fqnOld: String) : String {
        return "SELECT $ENTITY_SET_ID, $ID, $HASH, $fqnOld from $propertyTable"
    }

    fun storeS3Key(key: String, propertyTable: String, fqn: String) : String {
        return "UPDATE $propertyTable SET $fqn = '$key' where $HASH = ?"
    }

    fun removeOldFqnColumn(propertyTable: String, fqnOld: String) : String {
        return "ALTER TABLE $propertyTable DROP COLUMN $fqnOld"
    }
}

