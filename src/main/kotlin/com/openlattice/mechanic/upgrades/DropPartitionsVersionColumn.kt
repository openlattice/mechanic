package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.geekbeast.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDataTables
import com.geekbeast.postgres.PostgresDatatype
import com.openlattice.postgres.PostgresTable.*
import com.geekbeast.postgres.PostgresTableDefinition
import org.slf4j.LoggerFactory

class DropPartitionsVersionColumn(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(AddOriginIdToDataPrimaryKey::class.java)
    }

    private val PARTITIONS_VERSION = PostgresColumnDefinition(
        "partitions_version",
        PostgresDatatype.INTEGER).notNull()

    override fun getSupportedVersion(): Long {
        return Version.V2019_10_03.value
    }

    override fun upgrade(): Boolean {

        removeColFromDataPkey()

        dropCol(DATA)
        dropCol(IDS)
        dropCol(E)
        dropCol(ENTITY_SETS)

        return true
    }

    private fun removeColFromDataPkey() {
        val pkey = (PostgresDataTables.buildDataTableDefinition().primaryKey - PARTITIONS_VERSION).joinToString(",") { it.name }

        logger.info("Adding new index to data to be used for primary key.")

        val createNewPkeyIndex = "CREATE UNIQUE INDEX CONCURRENTLY ${DATA.name}_pkey_idx ON ${DATA.name} ($pkey)"

        toolbox.hds.connection.use { conn ->
            conn.createStatement().execute(createNewPkeyIndex)
        }

        logger.info("Finished creating index. About to drop pkey constraint and create new constraint using new index.")

        val dropPkey = "ALTER TABLE ${DATA.name} DROP CONSTRAINT ${DATA.name}_pkey1"
        val updatePkey = "ALTER TABLE ${DATA.name} ADD CONSTRAINT ${DATA.name}_pkey1 PRIMARY KEY USING INDEX ${DATA.name}_pkey_idx"

        logger.info("About to drop and recreate primary key of data.")

        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().executeUpdate(dropPkey)
            conn.createStatement().executeUpdate(updatePkey)
            conn.commit()
        }
        logger.info("Finished updating primary key.")
    }

    private fun dropCol(table: PostgresTableDefinition) {

        val sql = getDropColSql(table)

        logger.info("About to drop partitions_version from table ${table.name} with SQL: $sql")

        toolbox.hds.connection.use { conn ->
            conn.createStatement().executeUpdate(sql)
        }

        logger.info("Finished dropping partitions_version from ${table.name}")
    }


    private fun getDropColSql(table: PostgresTableDefinition): String {
        return "ALTER TABLE ${table.name} DROP COLUMN ${PARTITIONS_VERSION.name}"
    }

}
