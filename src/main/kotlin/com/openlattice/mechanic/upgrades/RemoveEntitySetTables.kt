package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import java.lang.Exception
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class RemoveEntitySetTables(val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {
        val tables = PostgresIterable<String>(
                Supplier {
                    val conn = toolbox.hds.connection
                    val stmt = conn.createStatement()
                    val rs = stmt.executeQuery(
                            "SELECT table_name\n" +
                                    "  FROM information_schema.tables\n" +
                                    " WHERE table_schema='public'\n" +
                                    "   AND table_type='BASE TABLE' AND table_name LIKE 'es_%'"
                    )
                    StatementHolder(conn, stmt, rs)
                },
                Function<ResultSet, String> { rs ->
                    rs.getString("table_name")
                }).toSet()
        tables.parallelStream()
                .map { table -> table.removePrefix("es_") }
                .filter { table ->
                    try {
                        UUID.fromString(table).toString() == table
                    } catch (ex: Exception) {
                        false
                    }
                }
                .forEach { table ->
                    toolbox.hds.connection.use { conn ->
                        conn.createStatement().use { stmt ->
                            stmt.execute("DROP TABLE \"es_$table\"")
                        }
                    }
                }
        return true
    }


    override fun getSupportedVersion(): Long {
        return Version.V2018_09_14.value
    }

}