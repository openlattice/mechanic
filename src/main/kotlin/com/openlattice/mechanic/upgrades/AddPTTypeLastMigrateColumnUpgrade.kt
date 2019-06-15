package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox

class AddPTTypeLastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    val newTableName = "data"

//    val newTableCols = arrayOf( "" )
//
//    data
//    (entity_set_id UUID NOT NULL,
//    id UUID,
//    partition INTEGER,
//    property_type_id UUID NOT NULL,
//    hash BYTEA NOT NULL,
//    b_TEXT TEXT,
//    b_UUID UUID,
//    b_SMALLINT SMALLINT,
//    b_INTEGER INTEGER,
//    b_BIGINT BIGINT,
//    b_DATE DATE,
//    b_TIMESTAMPTZ TIMESTAMPTZ,
//    b_DOUBLE DOUBLE PRECISION,
//    b_BOOLEAN BOOLEAN,
//    g_TEXT TEXT,
//    g_UUID UUID,
//    g_SMALLINT SMALLINT,
//    g_INTEGER INTEGER,
//    g_BIGINT BIGINT,
//    g_DATE DATE,
//    g_TIMESTAMPTZ TIMESTAMPTZ,
//    g_DOUBLE DOUBLE PRECISION,
//    g_BOOLEAN BOOLEAN,
//    n_TEXT TEXT,
//    n_UUID UUID,
//    n_SMALLINT SMALLINT,
//    n_INTEGER INTEGER,
//    n_BIGINT BIGINT,
//    n_DATE DATE,
//    n_TIMESTAMPTZ TIMESTAMPTZ,
//    n_DOUBLE DOUBLE PRECISION,
//    n_BOOLEAN BOOLEAN)

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
                val propertyType = propertyEntry.value
                // select rows to migrate
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                        "SELECT * FROM pt_ $propertyId WHERE last_migrate < last_write"
                    ).use {
                        // migrate rows
                        conn.prepareStatement(INSERT_SQL).use { ps ->
                        }
                    }
                }
                conn.commit()
            }
        }

        return true
    }

    val INSERT_SQL = "INSERT INTO $newTableName () VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO UPDATE SET"
}