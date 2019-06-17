package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresDataTables

class AddPTTypeLastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        val BATCH_SIZE = 1 shl 10 // 2048

        val tableName = PostgresDataTables.buildDataTableDefinition().name

        val pkeyCols = PostgresDataTables.buildDataTableDefinition().primaryKey.map { it.name }

        val cols = PostgresDataTables.dataTableColumns.map{ it.name }

        val INSERT_SQL = "INSERT INTO $tableName (" +
                cols.joinToString(",") +
                ") VALUES (" +
                cols.joinToString{"?"} +
                ") ON CONFLICT (" +
                pkeyCols.joinToString(",")+
                ") DO UPDATE SET " +
                cols.joinToString { col -> "$col = EXCLUDED.$col" }

        var i = 0
        val otherThing = cols.joinToString{col ->
            println( "ps.setObject($i, ResultSetAdapters.id( row ) ) // $col")
            i++
            ""
        }
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

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_14.value
    }
}
