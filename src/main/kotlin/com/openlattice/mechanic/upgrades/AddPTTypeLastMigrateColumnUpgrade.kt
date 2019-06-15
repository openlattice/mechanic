package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import java.sql.ResultSet
import java.util.function.Function
import java.util.function.Supplier

class AddPTTypeLastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

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
            toolbox.propertyTypes.entries.forEach {propertyEntry ->
                val propertyId = propertyEntry.key
                // select rows to migrate
                val toMigrate = PostgresIterable(Supplier {
                    conn.createStatement().use { stmt ->
                        val rs = stmt.executeQuery(
                                "SELECT * FROM pt_$propertyId WHERE last_migrate < last_write"
                        )
                        StatementHolder(conn, stmt, rs)
                    }
                }, Function<ResultSet, Unit> {
                    conn.prepareStatement(INSERT_SQL).use{ ps ->
                        // do le settings here
                        ps.execute()
                    }
                } )
                // migrate rows
                conn.commit()
            }
        }

        return true
    }

    val cols = arrayOf("entity_set_id", "id", "partition", "property_type_id", "hash",
            "b_TEXT", "b_UUID", "b_SMALLINT", "b_INTEGER", "b_BIGINT", "b_DATE", "b_TIMESTAMPTZ", "b_DOUBLEPRECISION", "b_BOOLEAN",
            "g_TEXT", "g_UUID", "g_SMALLINT", "g_INTEGER", "g_BIGINT", "g_DATE", "g_TIMESTAMPTZ", "g_DOUBLEPRECISION", "g_BOOLEAN",
            "n_TEXT", "n_UUID", "n_SMALLINT", "n_INTEGER", "n_BIGINT", "n_DATE", "n_TIMESTAMPTZ", "n_DOUBLEPRECISION", "n_BOOLEAN"
    )

    val INSERT_SQL = "INSERT INTO data (" +
            cols.joinToString(",") +
            ") VALUES (" +
            cols.joinToString{ "?" } +
            ") ON CONFLICT DO UPDATE SET " +
            cols.joinToString { col -> "$col = EXCLUDED.$col" }
}