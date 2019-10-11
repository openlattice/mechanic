package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory

class AddOriginIdToDataPrimaryKey(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddOriginIdToDataPrimaryKey::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

    override fun upgrade(): Boolean {

        val pkey = (PostgresDataTables.buildDataTableDefinition().primaryKey + ORIGIN_ID).joinToString(",") { it.name }

//        logger.info("Adding new index to data to be used for primary key.")

//        val createNewPkeyIndex = "CREATE UNIQUE INDEX CONCURRENTLY ${DATA.name}_pkey_idx ON ${DATA.name} ($pkey)"
//
//        toolbox.hds.connection.use { conn ->
//            conn.createStatement().execute( createNewPkeyIndex )
//        }

        logger.info("Finished creating index. About to drop pkey constraint and create new constraint using new index.")

        val dropPkey = "ALTER TABLE ${DATA.name} DROP CONSTRAINT ${DATA.name}_pkey1"
        val updatePkey = "ALTER TABLE ${DATA.name} ADD CONSTRAINT ${DATA.name}_pkey1 PRIMARY KEY USING INDEX ${DATA.name}_pkey_idx"

        logger.info("About to drop and recreate primary key of data.")

        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().executeUpdate( dropPkey )
            conn.createStatement().executeUpdate( updatePkey )
            conn.commit()
        }

        logger.info("Finished updating primary key.")

        return true
    }
}
