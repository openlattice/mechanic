package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox

//Migration for media server

class MediaServerUpgrade(private val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {
        addMockS3Table()
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }

    fun addMockS3Table() {
        val connection = toolbox.hds.connection
        val ps = connection.prepareStatement(addMockS3TableQuery())
        ps.executeUpdate()
        connection.close()
    }

    fun addMockS3TableQuery() : String {
        return "CREATE TABLE mock_s3_bucket (key text, object bytea)"
    }
}

