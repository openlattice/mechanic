package com.openlattice.mechanic.retired

import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.Upgrade
import com.openlattice.mechanic.upgrades.Version
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.StatementHolderSupplier
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class RemoveEntitySetTables(val toolbox: Toolbox) : Upgrade {
    override fun upgrade(): Boolean {
        val sql = "SELECT table_name\n" +
                "  FROM information_schema.tables\n" +
                " WHERE table_schema='public'\n" +
                "   AND table_type='BASE TABLE' AND table_name LIKE 'es_%'"

        val tables = BasePostgresIterable<String>(StatementHolderSupplier(toolbox.hds, sql)) { rs ->
            rs.getString("table_name")
        }.toSet()
        tables.parallelStream()
                .map { table -> table.removePrefix("es_") }
                .filter { table ->
                    try {
                        if (table == "ek_ids") {
                            false
                        } else {
                            UUID.fromString(table).toString() == table
                        }
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