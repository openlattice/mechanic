package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.ResultSetAdapters
import java.sql.PreparedStatement
import java.sql.ResultSet

class AddPTTypeLastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    val BATCH_SIZE = 1 shl 10 // 2048

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_14.value
    }

    override fun upgrade(): Boolean {
        // ADD COLUMN WITH DEFAULT TO -infinity
        toolbox.hds.connection.use {conn ->
            conn.autoCommit = true
            toolbox.propertyTypes.keys.forEach {propertyTypeId ->
                conn.createStatement().use { statement ->
                    statement.execute(
                        "ALTER TABLE pt_$propertyTypeId ADD COLUMN last_migrate timestamp with time zone NOT NULL DEFAULT '-infinity'::timestamptz"
                    )
                }
            }
        }

        // Read props last_migrate < last_write
        // insert into appropriate column in new table
        toolbox.hds.connection.use {conn ->
            conn.autoCommit = false

            toolbox.propertyTypes.entries.forEach { propertyEntry ->
                val propertyId = propertyEntry.key
                // select rows to migrate
                conn.createStatement().use { stmt ->
                    stmt.fetchSize = BATCH_SIZE
                    val rs = stmt.executeQuery(
                            "SELECT * FROM pt_$propertyId WHERE last_migrate < last_write"
                    )
                    conn.prepareStatement(INSERT_SQL).use { ps ->
                        var index = 0;
                        while(index < BATCH_SIZE && rs.last()) {
                            addRow(ps, rs)
                        }
                        val numUpdates = ps.executeUpdate()
                    }
                }
                // migrate rows
                conn.commit()
            }
        }

        return true
    }

    fun addRow(ps: PreparedStatement, row: ResultSet ) {
        ps.setObject(0, ResultSetAdapters.id( row ) )
        ps.setObject(1, ResultSetAdapters.entitySetId( row ) )
    }

    val pkeyCols = PostgresDataTables.buildDataTableDefinition().primaryKey.map { it.name }

    val cols = PostgresDataTables.dataTableColumns.map{ it.name }

    val INSERT_SQL_PREFIX = "INSERT INTO data (" +
            cols.joinToString(",") +
            ") VALUES "

    val INSERT_SQL_SUFFIX = " ON CONFLICT (" +
            pkeyCols.joinToString(",")+
            ") DO UPDATE SET " +
            cols.joinToString { col -> "$col = EXCLUDED.$col" }

    val INSERT_SQL = INSERT_SQL_PREFIX +
            buildString {
                this.append("(" + cols.joinToString("?") + ") ")
            } +
            INSERT_SQL_SUFFIX

}