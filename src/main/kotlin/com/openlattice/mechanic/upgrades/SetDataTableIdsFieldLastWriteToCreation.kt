package com.openlattice.mechanic.upgrades

import com.openlattice.IdConstants
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.geekbeast.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import org.slf4j.LoggerFactory

class SetDataTableIdsFieldLastWriteToCreation(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(SetDataTableIdsFieldLastWriteToCreation::class.java)
    }

    override fun upgrade(): Boolean {
        setDefaultValue()
        updateLastWriteToValueFromIds()

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_11_21.value
    }

    fun setDefaultValue() {

        logger.info("About to alter last_write default value using sql: {}", UPDATE_DEFAULT_VALUE)

        toolbox.hds.connection.use { conn ->
            conn.createStatement().executeUpdate( UPDATE_DEFAULT_VALUE )
        }

        logger.info("Finished altering last_write default value to now().")
    }

    fun updateLastWriteToValueFromIds() {

        logger.info("About to update existing last_write values in id row in data table using sql: {}", UPDATE_LAST_WRITE_SQL)

        toolbox.hds.connection.use { conn ->
            conn.createStatement().executeUpdate( UPDATE_LAST_WRITE_SQL )
        }

        logger.info("Finished updating existing values.")
    }

    fun eq(col: PostgresColumnDefinition): String {
        return "${DATA.name}.${col.name} = ${IDS.name}.${col.name} "
    }

    val UPDATE_LAST_WRITE_SQL = "UPDATE ${DATA.name} SET ${LAST_WRITE.name} = ${IDS.name}.${LAST_WRITE.name} FROM ${IDS.name} WHERE " +
            "${eq(ID)} " +
            "AND ${eq(ENTITY_SET_ID)} " +
            "AND ${eq(PARTITION)} " +
            "AND ${DATA.name}.${PROPERTY_TYPE_ID.name} = '${IdConstants.ID_ID.id}'"

    val UPDATE_DEFAULT_VALUE = "ALTER TABLE ${DATA.name} ALTER COLUMN ${LAST_WRITE.name} SET DEFAULT now()"
}