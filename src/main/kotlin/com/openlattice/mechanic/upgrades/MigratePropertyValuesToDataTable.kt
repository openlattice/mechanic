package com.openlattice.mechanic.upgrades

import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.propertyTableName
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.ResultSetAdapters
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.util.*

class MigratePropertyValuesToDataTable(private val toolbox: Toolbox) : Upgrade {

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
    }

    override fun upgrade(): Boolean {

        // Read props last_migrate < last_write
        // insert into appropriate column in new table
        toolbox.hds.connection.use { conn ->
            conn.autoCommit = false

            toolbox.propertyTypes.entries.forEach { propertyEntry ->
                val propertyTypeId = propertyEntry.key
                // select rows to migrate
                conn.createStatement().use { stmt ->
                    stmt.fetchSize = BATCH_SIZE
                    val table = quote(propertyTableName(propertyTypeId))
                    stmt.executeQuery(
                            "SELECT * FROM $table WHERE last_migrate < last_write"
                    ).use { rs ->
                        while (rs.next()) {
                            conn.prepareStatement(INSERT_SQL).use { ps ->
                                mapRow(ps, rs, propertyEntry.key, propertyEntry.value)
                                val numUpdates = ps.executeUpdate()
                                if (numUpdates != 1) {
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

    fun mapRow(ps: PreparedStatement, row: ResultSet, propTypeId: UUID, propType: PropertyType) {
        val propMetadata = ResultSetAdapters.propertyMetadata(row)
        ps.setObject(0, ResultSetAdapters.entitySetId( row ) )      // entity_set_id
        ps.setObject(1, ResultSetAdapters.id( row ) )               // id
        ps.setObject(2, ResultSetAdapters.id( row ) )               // partition TODO
        ps.setObject(3, propTypeId )                                // property_type_id
        ps.setObject(4, propMetadata.hash )                         // hash
        ps.setObject(5, propMetadata.lastWrite )                    // last_write
        ps.setObject(6, ResultSetAdapters.lastLinkIndex( row ) )    // last_link_index
        ps.setObject(7, propMetadata.version )                      // version
        ps.setObject(8, propMetadata.versions )                     // versions

        propType.datatype
        when (propType.postgresIndexType) {
            IndexType.BTREE -> mapValueFromTypeWithOffset( ps, row, propType, 0 )
            IndexType.GIN -> mapValueFromTypeWithOffset( ps, row, propType, 12 )
            IndexType.NONE -> mapValueFromTypeWithOffset( ps, row, propType, 24 )
        }

//      vv TODO: null out the un-set ones? modify the insert query to just take one col? vv
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

    private fun mapValueFromTypeWithOffset(ps: PreparedStatement, row: ResultSet, propType: PropertyType, offset: Int) {
        val ptFQN = propType.type.fullQualifiedNameAsString
        when(propType.datatype) {
            EdmPrimitiveTypeKind.String ->  ps.setObject(9 + offset, row.getObject(ptFQN) )          // _TEXT
            EdmPrimitiveTypeKind.Guid ->    ps.setObject(10 + offset, row.getObject(ptFQN) )         // _UUID
            EdmPrimitiveTypeKind.Byte ->    ps.setObject(11 + offset, row.getObject(ptFQN) )         // _TEXT
            EdmPrimitiveTypeKind.Int16 ->   ps.setObject(12 + offset, row.getObject(ptFQN) )         // _SMALLINT
            EdmPrimitiveTypeKind.Int32 ->   ps.setObject(13 + offset, row.getObject(ptFQN) )         // _INTEGER
            EdmPrimitiveTypeKind.Duration ->ps.setObject(14 + offset, row.getObject(ptFQN) )         // _BIGINT
            EdmPrimitiveTypeKind.Int64 ->   ps.setObject(15 + offset, row.getObject(ptFQN) )         // _BIGINT
            EdmPrimitiveTypeKind.Date ->    ps.setObject(16 + offset, row.getObject(ptFQN) )         // _DATE
            EdmPrimitiveTypeKind.DateTimeOffset ->  ps.setObject(17 + offset, row.getObject(ptFQN) ) // _TIMESTAMPTZ
            EdmPrimitiveTypeKind.Double ->  ps.setObject(18 + offset, row.getObject(ptFQN) )         // _DOUBLE
            EdmPrimitiveTypeKind.Boolean -> ps.setObject(19 + offset, row.getObject(ptFQN) )         // _BOOLEAN
            EdmPrimitiveTypeKind.Binary ->  ps.setObject(20 + offset, row.getObject(ptFQN) )         // _TEXT
            else -> ""
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}
