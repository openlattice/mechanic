package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.ResultSetAdapters
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

class AddPTTypeLastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    val BATCH_SIZE = 1 shl 10 // 2048

    companion object {
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

        // Read props last_migrate < last_write
        // insert into appropriate column in new table
        toolbox.hds.connection.use {conn ->
            conn.autoCommit = false

            toolbox.propertyTypes.entries.forEach { propertyEntry ->
                val propertyId = propertyEntry.key
                // select rows to migrate
                conn.createStatement().use { stmt ->
                    stmt.fetchSize = BATCH_SIZE
                    stmt.executeQuery(
                            "SELECT * FROM pt_$propertyId WHERE last_migrate < last_write"
                    ).use { rs ->
                        while (rs.next()){
                            conn.prepareStatement(INSERT_SQL).use { ps ->
                                mapRow(ps, rs, propertyEntry.key)
                                val numUpdates = ps.executeUpdate()
                                if ( numUpdates != 1 ){
                                    // rollback
                                    conn.rollback()
                                } else {
                                    // commit
                                    conn.commit()
                                }
                            }
                        }
                    }
                }
            }
        }

        return true
    }

    fun mapRow(ps: PreparedStatement, row: ResultSet, propertyTypeId: UUID) {
        val propertyMetadata = ResultSetAdapters.propertyMetadata(row)
        ps.setObject(0, ResultSetAdapters.entitySetId( row ) )      // entity_set_id
        ps.setObject(1, ResultSetAdapters.id( row ) )               // id
        ps.setObject(2, ResultSetAdapters.id( row ) )               // partition TODO
        ps.setObject(3, propertyTypeId )                            // property_type_id
        ps.setObject(4, propertyMetadata.hash )                     // hash
        ps.setObject(5, propertyMetadata.lastWrite )                // last_write
        ps.setObject(6, ResultSetAdapters.lastLinkIndex( row ) )    // last_link_index
        ps.setObject(7, propertyMetadata.version )                  // version
        ps.setObject(8, propertyMetadata.versions )                 // versions
//      vv TODO vv
        ps.setObject(9, ResultSetAdapters.id( row ) )  // b_TEXT
        ps.setObject(10, ResultSetAdapters.id( row ) ) // b_UUID
        ps.setObject(11, ResultSetAdapters.id( row ) ) // b_TEXT
        ps.setObject(12, ResultSetAdapters.id( row ) ) // b_SMALLINT
        ps.setObject(13, ResultSetAdapters.id( row ) ) // b_INTEGER
        ps.setObject(14, ResultSetAdapters.id( row ) ) // b_BIGINT
        ps.setObject(15, ResultSetAdapters.id( row ) ) // b_BIGINT
        ps.setObject(16, ResultSetAdapters.id( row ) ) // b_DATE
        ps.setObject(17, ResultSetAdapters.id( row ) ) // b_TIMESTAMPTZ
        ps.setObject(18, ResultSetAdapters.id( row ) ) // b_DOUBLE
        ps.setObject(19, ResultSetAdapters.id( row ) ) // b_BOOLEAN
        ps.setObject(20, ResultSetAdapters.id( row ) ) // b_TEXT
        ps.setObject(21, ResultSetAdapters.id( row ) ) // g_TEXT
        ps.setObject(22, ResultSetAdapters.id( row ) ) // g_UUID
        ps.setObject(23, ResultSetAdapters.id( row ) ) // g_TEXT
        ps.setObject(24, ResultSetAdapters.id( row ) ) // g_SMALLINT
        ps.setObject(25, ResultSetAdapters.id( row ) ) // g_INTEGER
        ps.setObject(26, ResultSetAdapters.id( row ) ) // g_BIGINT
        ps.setObject(27, ResultSetAdapters.id( row ) ) // g_BIGINT
        ps.setObject(28, ResultSetAdapters.id( row ) ) // g_DATE
        ps.setObject(29, ResultSetAdapters.id( row ) ) // g_TIMESTAMPTZ
        ps.setObject(30, ResultSetAdapters.id( row ) ) // g_DOUBLE
        ps.setObject(31, ResultSetAdapters.id( row ) ) // g_BOOLEAN
        ps.setObject(32, ResultSetAdapters.id( row ) ) // g_TEXT
        ps.setObject(33, ResultSetAdapters.id( row ) ) // n_TEXT
        ps.setObject(34, ResultSetAdapters.id( row ) ) // n_UUID
        ps.setObject(35, ResultSetAdapters.id( row ) ) // n_TEXT
        ps.setObject(36, ResultSetAdapters.id( row ) ) // n_SMALLINT
        ps.setObject(37, ResultSetAdapters.id( row ) ) // n_INTEGER
        ps.setObject(38, ResultSetAdapters.id( row ) ) // n_BIGINT
        ps.setObject(39, ResultSetAdapters.id( row ) ) // n_BIGINT
        ps.setObject(40, ResultSetAdapters.id( row ) ) // n_DATE
        ps.setObject(41, ResultSetAdapters.id( row ) ) // n_TIMESTAMPTZ
        ps.setObject(42, ResultSetAdapters.id( row ) ) // n_DOUBLE
        ps.setObject(43, ResultSetAdapters.id( row ) ) // n_BOOLEAN
        ps.setObject(44, ResultSetAdapters.id( row ) ) // n_TEXT
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_14.value
    }
}