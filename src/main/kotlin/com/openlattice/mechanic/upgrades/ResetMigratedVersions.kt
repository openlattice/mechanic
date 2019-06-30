package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import org.slf4j.LoggerFactory

class ResetMigratedVersions(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(ResetMigratedVersions::class.java)
    }

    override fun upgrade(): Boolean {

        toolbox.propertyTypes.entries.parallelStream().forEach { (propertyTypeId, propertyType) ->
            toolbox.hds.connection.use { conn ->
                conn.createStatement().use { statement ->
                    val rawTableName = propertyTableName(propertyTypeId)
                    val table = quote(rawTableName)
                    val count = statement.executeUpdate("UPDATE $table SET migrated_version = 0")
                    logger.info("Reset $count migrated_versions for $rawTableName")
                }
            }
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_14.value
    }
}
