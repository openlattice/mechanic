package com.openlattice.mechanic.upgrades

import com.openlattice.authorization.PrincipalType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory

@SuppressFBWarnings( value = ["SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"] )
class RemovePermissionsForNonexistentPrincipals(val toolbox: Toolbox) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(RemovePermissionsForNonexistentPrincipals::class.java)
    }

    override fun upgrade(): Boolean {
        logger.info("About to remove permissions for nonexistent principals")

        val deleteSql = "DELETE FROM ${PostgresTable.PERMISSIONS.name} WHERE NOT EXISTS (SELECT NULL FROM ${PostgresTable.PRINCIPALS.name} " +
                "WHERE ${PostgresTable.PERMISSIONS.name}.${PostgresColumn.PRINCIPAL_ID.name} = ${PostgresTable.PRINCIPALS.name}.${PostgresColumn.PRINCIPAL_ID.name}) " +
                " AND (${PostgresTable.PERMISSIONS.name}.${PostgresColumn.PRINCIPAL_TYPE.name} = '${PrincipalType.ROLE.name}' OR ${PostgresTable.PERMISSIONS.name}.${PostgresColumn.PRINCIPAL_TYPE.name} = '${PrincipalType.USER.name}')"

        toolbox.hds.connection.use {
            it.createStatement().use {
                it.execute(deleteSql)
            }
        }

        logger.info("Finished removing permissions for nonexistent principals")

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_27.value
    }

}