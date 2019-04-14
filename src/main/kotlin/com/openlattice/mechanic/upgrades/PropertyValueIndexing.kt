package com.openlattice.mechanic.upgrades

import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.*
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.concurrent.Callable

class PropertyValueIndexing(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(PropertyValueIndexing::class.java)
    }

    override fun upgrade(): Boolean {
        return createPropertyValueIndexIfNotExists()
    }

    private fun createPropertyValueIndexIfNotExists(): Boolean {
        return true
//        return toolbox.propertyTypes.values.all { propertyType ->
//            if (!DataTables.unindexedProperties.contains(propertyType.datatype.fullQualifiedName)) {
//                val idxPrefix = DataTables.propertyTableName(propertyType.id)
//
//                val valueColumn = DataTables.value(propertyType)
//                val ptd = CitusDistributedTableDefinition(DataTables.quote(idxPrefix))
//
//                val valueIndex = PostgresColumnsIndexDefinition(ptd, valueColumn)
//                        .name(DataTables.quote(idxPrefix + "_value_idx"))
//                        .ifNotExists()
//                try {
//                    toolbox.executor.submit(
//                            Callable {
//                                toolbox.hds.connection.use {
//                                    it.createStatement().execute(valueIndex.sql())
//                                }
//                            }
//                    ).get()
//                    logger.info("Created (if not exists) index for property type '{}' with value type {}",
//                            propertyType.type.fullQualifiedNameAsString, propertyType.datatype)
//                } catch (e: Exception) {
//                    logger.error(
//                            "Unable to add index for property type '{}' with value type {}${System.lineSeparator()}$e",
//                            propertyType.type.fullQualifiedNameAsString, propertyType.datatype)
//                    return@all false
//                }
//            }
//            return@all true
//        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2018_10_10.value
    }
}