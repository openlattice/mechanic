package com.openlattice.mechanic.upgrades


import com.openlattice.edm.type.PropertyType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import org.slf4j.LoggerFactory
import java.util.*

class MigratePropertyValuesToDataTable(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(MigratePropertyValuesToDataTable::class.java)
    }

    override fun upgrade(): Boolean {
        toolbox.hds.connection.use { conn ->
            toolbox.entityTypes
                    .getValue(UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab")).properties //Only general.person
                    .associateWith { toolbox.propertyTypes.getValue(it) }
//            toolbox.propertyTypes.entries
                    .forEach { (propertyTypeId, propertyType) ->
                        val insertSql = getInsertQuery(propertyType)
                        logger.info("Insert SQL: {}", insertSql)

                        val inserted = conn.createStatement().executeUpdate(insertSql)

                        logger.info(
                                "Migrated {} properties into DATA table of type {} ({})",
                                inserted,
                                propertyType.type.fullQualifiedNameAsString,
                                propertyTypeId
                        )
                        conn.commit()
                    }
        }
        return true
    }

    private fun getInsertQuery(propertyType: PropertyType): String {
        val col = getColumnDefinition(IndexType.NONE, propertyType.datatype)
        val insertCols = PostgresDataTables
                .dataTableMetadataColumns
                .filter { it != ORIGIN_ID}
                .joinToString(",") { it.name }
        val selectCols = listOf(
                ENTITY_SET_ID.name,
                ID_VALUE.name,
                "partitions[ 1 + (('x'||right(id::text,8))::bit(32)::int % array_length(partitions,1))] as partition",
                "'${propertyType.id}'::uuid as ${PROPERTY_TYPE_ID.name}",
                HASH.name,
                LAST_WRITE.name,
                LAST_PROPAGATE.name,
                LAST_MIGRATE.name,
                VERSION.name,
                VERSIONS.name,
                PARTITIONS_VERSION.name
        ).joinToString(",")
        val conflictSql = buildConflictSql()
        val propertyTable = quote(propertyTableName(propertyType.id))
        val propertyColumn = quote(propertyType.type.fullQualifiedNameAsString)
        val withClause = "WITH for_migration as ( UPDATE $propertyTable set migrated_version = abs(version) WHERE migrated_version < abs(version) RETURNING * ) "
        return "$withClause INSERT INTO ${DATA.name} ($insertCols,${col.name}) " +
                "SELECT $selectCols,$propertyColumn as ${col.name} " +
                "FROM for_migration INNER JOIN (select id as entity_set_id, partitions, partitions_version from ${ENTITY_SETS.name}) as entity_set_partitions USING(entity_set_id) " +
                "WHERE last_migrate = ?" +
                "ON CONFLICT (${DATA.primaryKey.joinToString(",") { it.name }} ) DO UPDATE SET $conflictSql"
    }

    private fun buildConflictSql(): String {
        //This isn't usable for repartitioning.
        return listOf(
                ENTITY_SET_ID,
                LAST_WRITE,
                LAST_PROPAGATE,
                LAST_MIGRATE,
                VERSION,
                VERSIONS,
                PARTITIONS_VERSION
        ).joinToString(",") { "${it.name} = EXCLUDED.${it.name}" }
        //"${VERSIONS.name} = ${VERSIONS.name} || EXCLUDED.${VERSIONS.name}"
        //"${VERSION.name} = CASE WHEN abs(${VERSION.name}) < EXCLUDED.${VERSION.name} THEN EXCLUDED.${VERSION.name} ELSE ${DATA.name}.${VERSION.name} END "
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}

