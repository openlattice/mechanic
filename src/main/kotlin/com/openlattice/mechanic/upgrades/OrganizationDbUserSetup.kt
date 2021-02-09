/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.mechanic.upgrades

import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresRoles
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.Schemas
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore
import java.util.*

class OrganizationDbUserSetup(
        private val organizationAssemblyMapstore: OrganizationAssemblyMapstore,
        private val assemblerConfiguration: AssemblerConfiguration,
        private val externalDatabaseConnectionManager: ExternalDatabaseConnectionManager
) : Upgrade {

    override fun upgrade(): Boolean {
        organizationAssemblyMapstore.loadAllKeys().forEach(::setupOrganizationDbUser)

        return true
    }

    private fun setupOrganizationDbUser(organizationId: UUID) {
        val organizationDbName = ExternalDatabaseConnectionManager.buildDefaultOrganizationDatabaseName(organizationId)
        val dbOrgUser = DataTables.quote(PostgresRoles.buildOrganizationUserId(organizationId))
        val connectionConfig = assemblerConfiguration.server.clone() as Properties

        externalDatabaseConnectionManager.createDataSource(organizationDbName, connectionConfig, assemblerConfiguration.ssl).use { dataSource ->
            dataSource.connection.use { connection ->
                connection.createStatement()
                        .use { statement ->
                            statement.execute("GRANT USAGE, CREATE ON SCHEMA ${Schemas.OPENLATTICE_SCHEMA} TO $dbOrgUser")
                            statement.execute("ALTER USER $dbOrgUser SET search_path TO ${Schemas.OPENLATTICE_SCHEMA}")
                        }
            }
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_08_20.value
    }
}