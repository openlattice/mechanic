package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresExpressionIndexDefinition
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class LastMigrateColumnUpgrade(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(LastMigrateColumnUpgrade::class.java)
    }

    override fun upgrade(): Boolean {

        // ADD COLUMN WITH DEFAULT TO -infinity
        toolbox.hds.connection.use { conn ->
            conn.createStatement().use { statement ->
                toolbox.propertyTypes.entries.parallelStream().forEach { (propertyTypeId, propertyType) ->
                    val rawTableName =propertyTableName(propertyTypeId)
                    val table = quote(rawTableName)
                    statement.execute(
                            "ALTER TABLE $table DROP COLUMN if exists last_migrate timestamp with time zone NOT NULL DEFAULT '-infinity'"
                    )
                    statement.execute(
                            "ALTER TABLE $table ADD COLUMN if not exists migrated_version bigint NOT NULL DEFAULT 0"
                    )
                    logger.info("Ensured that table pt_$propertyTypeId has migrated_version column")
                    val indexName = quote("${rawTableName}_needs_migration_idx")

            val sw = Stopwatch.createStarted()
                    statement.execute("CREATE INDEX IF NOT EXISTS $indexName ON $table (migrated_version < abs(${VERSION.name}))")
                    logger.info("Created index for row that need migration on $table in ${sw.elapsed(TimeUnit.MILLISECONDS)}ms")
                }
            }
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_14.value
    }
}
