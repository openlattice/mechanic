package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory

@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
class FixAssociationTypeCatogories(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(FixAssociationTypeCatogories::class.java)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2020_03_25.value
    }

    override fun upgrade(): Boolean {

        updateAssociationTypeCategories()
        updateEntitySetFlags()

        return true
    }

    private fun updateAssociationTypeCategories() {

        logger.info("About to update entity_types to set category to AssociationType where appropriate.")

        val sql = "UPDATE ${ENTITY_TYPES.name} " +
                "SET ${CATEGORY.name} = '${SecurableObjectType.AssociationType}' " +
                "WHERE ${ID.name} = ANY( SELECT ${ID.name} FROM ${ASSOCIATION_TYPES.name} ) " +
                "AND ${CATEGORY.name} != '${SecurableObjectType.AssociationType}'" // adding this part just to get an accurate count

        val count = toolbox.hds.connection.use { conn ->
            conn.createStatement().executeUpdate(sql)
        }

        logger.info("Updated $count association type categories.")
    }


    private fun updateEntitySetFlags() {

        logger.info("About to update entity set flags to add ASSOCIATION where appropriate.")

        val associationTypeIds = BasePostgresIterable(StatementHolderSupplier(toolbox.hds, "SELECT ${ID.name} FROM ${ASSOCIATION_TYPES.name}")) {
            ResultSetAdapters.id(it)
        }.toSet()

        val sql = "UPDATE ${ENTITY_SETS.name} " +
                "SET ${FLAGS.name} = ${FLAGS.name} || '{${EntitySetFlag.ASSOCIATION}}' " +
                "WHERE ${ENTITY_TYPE_ID.name} = ANY( '{${associationTypeIds.joinToString(",")}}' ) " +
                "AND NOT( '${EntitySetFlag.ASSOCIATION}' = ANY(${FLAGS.name}) ) "

        val count = toolbox.hds.connection.use { conn ->
            conn.createStatement().executeUpdate(sql)
        }

        logger.info("Updated $count entity set flags.")
    }

}
