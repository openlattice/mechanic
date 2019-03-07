package com.openlattice.mechanic.upgrades

import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

class EntitySetFlags(private val toolbox: Toolbox) : Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(EntitySetFlags::class.java)
    }

    private fun addFlagsColumn() {
        logger.info("About to add flags column to entity_sets")

        val updateStatement = "ALTER TABLE ${PostgresTable.ENTITY_SETS.name} ADD COLUMN IF NOT EXISTS ${PostgresColumn.ENTITY_SET_FLAGS.name} text[]"

        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(updateStatement)
            }
        }

        logger.info("Finished adding flags column to entity_sets")
    }

    private fun dropOldColumns() {
        logger.info("About to drop linking and external columns from entity_sets")

        val dropLinkingColumnStatement = "ALTER TABLE ${PostgresTable.ENTITY_SETS.name} DROP COLUMN ${PostgresColumn.LINKING.name}"
        val dropExternalColumnStatement = "ALTER TABLE ${PostgresTable.ENTITY_SETS.name} DROP COLUMN ${PostgresColumn.EXTERNAL.name}"

        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(dropLinkingColumnStatement)
            }
            it.createStatement().use {
                it.execute(dropExternalColumnStatement)
            }
        }

        logger.info("Finished dropping linking and external columns from entity_sets")

    }

    private fun setFlags() {
        logger.info("About to set LINKING, EXTERNAL, ASSOCIATION, and AUDIT flags")

        val setLinkingFlagsSql = "UPDATE ${PostgresTable.ENTITY_SETS.name} " +
                "SET ${PostgresColumn.ENTITY_SET_FLAGS.name} = ${PostgresColumn.ENTITY_SET_FLAGS.name} || '{${EntitySetFlag.LINKING}}' " +
                "WHERE ${PostgresColumn.LINKING.name} = true"

        val setExternalFlagsSql = "UPDATE ${PostgresTable.ENTITY_SETS.name} " +
                "SET ${PostgresColumn.ENTITY_SET_FLAGS.name} = ${PostgresColumn.ENTITY_SET_FLAGS.name} || '{${EntitySetFlag.EXTERNAL}}' " +
                "WHERE ${PostgresColumn.EXTERNAL.name} = true"

        val setAssociationFlagsSql = "UPDATE ${PostgresTable.ENTITY_SETS.name} " +
                "SET ${PostgresColumn.ENTITY_SET_FLAGS.name} = ${PostgresColumn.ENTITY_SET_FLAGS.name} || '{${EntitySetFlag.ASSOCIATION}}' " +
                "WHERE ${PostgresColumn.ENTITY_TYPE_ID.name} = ANY(SELECT ${PostgresColumn.ID.name} from ${PostgresTable.ASSOCIATION_TYPES.name})"

        val setAuditFlagsSql = "UPDATE ${PostgresTable.ENTITY_SETS.name} " +
                "SET ${PostgresColumn.ENTITY_SET_FLAGS.name} = ${PostgresColumn.ENTITY_SET_FLAGS.name} || '{${EntitySetFlag.AUDIT}}' " +
                "WHERE ${PostgresColumn.ID.name} = ANY(SELECT UNNEST(${PostgresColumn.AUDIT_RECORD_ENTITY_SET_IDS.name}) FROM ${PostgresTable.AUDIT_RECORD_ENTITY_SET_IDS.name})"

        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(setLinkingFlagsSql)
            }
            it.createStatement().use {
                it.execute(setExternalFlagsSql)
            }
            it.createStatement().use {
                it.execute(setAssociationFlagsSql)
            }
            it.createStatement().use {
                it.execute(setAuditFlagsSql)
            }
        }

        logger.info("Finished setting entity set flags")
    }


    override fun upgrade(): Boolean {
        addFlagsColumn()
        setFlags()
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_02_25.value
    }

}