package com.openlattice.mechanic.upgrades

import com.openlattice.ResourceConfigurationLoader
import com.openlattice.data.storage.AwsBlobDataService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.quote
import java.util.*

//Migration for media server

private var BINARY_PROPERTY_ID_TO_FQN = mutableMapOf<UUID, String>()
private val ENTITY_SET_ID = quote("entity_set_id")
private val ID = quote("id")
private val HASH = quote("hash")
private val NAMESPACE = quote("namespace")
private val NAME = quote("name")
private val DATA_TYPE = quote("datatype")

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
        val config = ResourceConfigurationLoader.loadConfiguration(DatastoreConfiguration::class.java)
        val byteBlobDataManager = AwsBlobDataService(config)
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
            val propertyTable = quote("pt_".plus(entry.key))
            val fqn = quote(entry.value)
            val fqnOld = quote(entry.value.plus("_data"))

            renameFqnColumn(propertyTable, fqn, fqnOld)
            addNewFqnColumn(propertyTable, fqn)

            //move binary data to s3 and store s3 key
            val conn1 = toolbox.hds.connection
            val ps1 = conn1.prepareStatement(getDataForS3(propertyTable, fqnOld))
            val rs = ps1.executeQuery()
            while(rs.next()) {
                val data = rs.getBytes(4)
                val hash = rs.getBytes(3)
                val hashString = Base64.getEncoder().encodeToString(hash)

                val key = rs.getString(1) + "/" + rs.getString(2) + "/" + entry.key + "/" + hashString
                byteBlobDataManager.putObject(key, data)
                val conn2 = toolbox.hds.connection
                val ps2 = conn2.prepareStatement(storeS3Key(key, propertyTable, fqn))
                ps2.setBytes(1, hash)
                ps2.executeUpdate()
                conn2.close()
            }
            rs.close()
            conn1.close()

            removeOldFqnColumn(propertyTable, fqnOld)
        }
    }

    private fun addMockS3Table() {
        val connection = toolbox.hds.connection
        val ps = connection.prepareStatement(addMockS3TableQuery())
        ps.executeUpdate()
        connection.close()
    }

    private fun renameFqnColumn(propertyTable: String, fqn: String, fqnOld: String) {
        val setUpConn1 = toolbox.hds.connection
        val setUpPS1 = setUpConn1.prepareStatement(renameFqnColumnQuery(propertyTable, fqn, fqnOld))
        setUpPS1.executeUpdate()
        setUpConn1.close()
    }

    private fun addNewFqnColumn(propertyTable: String, fqn: String) {
        val setUpConn2 = toolbox.hds.connection
        val setUpPS2 = setUpConn2.prepareStatement(addNewFqnColumnQuery(propertyTable, fqn))
        setUpPS2.executeUpdate()
        setUpConn2.close()
    }

    private fun removeOldFqnColumn(propertyTable: String, fqnOld: String) {
        val cleanUpConn = toolbox.hds.connection
        val cleanUpPS = cleanUpConn.prepareStatement(removeOldFqnColumnQuery(propertyTable, fqnOld))
        cleanUpPS.executeUpdate()
        cleanUpConn.close()
    }

    //sql queries
    private fun binaryPropertyTypesQuery() : String {
        return "SELECT $ID, $NAMESPACE, $NAME FROM property_types WHERE $DATA_TYPE = 'Binary'"
    }

    private fun addMockS3TableQuery() : String {
        return "CREATE TABLE mock_s3_bucket (key text, object bytea)"
    }

    private fun renameFqnColumnQuery(propertyTable: String, fqn: String, fqnOld: String) : String {
        return "ALTER TABLE $propertyTable RENAME COLUMN $fqn TO $fqnOld"
    }

    private fun addNewFqnColumnQuery(propertyTable: String, fqn: String) : String {
        return "ALTER TABLE $propertyTable ADD COLUMN $fqn text"
    }

    private fun getDataForS3(propertyTable: String, fqnOld: String) : String {
        return "SELECT $ENTITY_SET_ID, $ID, $HASH, $fqnOld from $propertyTable"
    }

    private fun storeS3Key(key: String, propertyTable: String, fqn: String) : String {
        return "UPDATE $propertyTable SET $fqn = '$key' where $HASH = ?"
    }

    private fun removeOldFqnColumnQuery(propertyTable: String, fqnOld: String) : String {
        return "ALTER TABLE $propertyTable DROP COLUMN $fqnOld"
    }
}

