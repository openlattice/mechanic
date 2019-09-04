package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable.DATA
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class SetOriginIdToNonNullUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(Toolbox::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_09_15.value
    }

    override fun upgrade(): Boolean {

        val updateQuery = "UPDATE ${DATA.name} SET ${ORIGIN_ID.name} = ${IdConstants.EMPTY_ORIGIN_ID.id}::uuid WHERE ${ORIGIN_ID.name} = NULL"
        toolbox.rateLimitedQuery( 16, updateQuery, logger)

        val pkey = PostgresDataTables.buildDataTableDefinition().primaryKey.joinToString(",") { it.name }
        val dropPkey = "ALTER TABLE ${DATA.name} DROP CONSTRAINT ${DATA.name}_pkey"
        val updatePkey = "ALTER TABLE ${DATA.name} ADD PRIMARY KEY ($pkey)"
        val updateDefaultValue = "ALTER TABLE ${DATA.name} ALTER COLUMN ${ORIGIN_ID.name} SET DEFAULT '${IdConstants.EMPTY_ORIGIN_ID.id}'"
        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().executeUpdate( dropPkey )
            conn.createStatement().executeUpdate( updatePkey )
            conn.createStatement().executeUpdate( updateDefaultValue )
            conn.commit()
            return true
        }
    }
}
