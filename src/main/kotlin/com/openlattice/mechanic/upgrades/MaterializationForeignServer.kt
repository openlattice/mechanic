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
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

class MaterializationForeignServer(
        private val organizationAssemblyMapstore: OrganizationAssemblyMapstore,
        private val assemblerConfiguration: AssemblerConfiguration) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(MaterializationForeignServer::class.java)
    }

    override fun upgrade(): Boolean {
        val organizationIds = organizationAssemblyMapstore.loadAllKeys()

        logger.info("Starting to update foreign server ports")
        organizationIds.forEach(::updateForeignServerPort)
        logger.info("Finished updating foreign server ports")

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_06_03.value
    }

    private fun updateForeignServerPort(organizationId: UUID) {
        val organizationDbName = PostgresDatabases.buildOrganizationDatabaseName(organizationId)
        connect(organizationDbName).use { dataSource ->
            dataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(ALTER_FOREIGN_SERVER_PORT_SQL)
                }
            }
        }

        logger.info("Organization $organizationId finished")
    }

    private fun connect(organizationDbName: String): HikariDataSource {
        val connectionConfig = assemblerConfiguration.server.clone() as Properties
        return AssemblerConnectionManager.connect(organizationDbName, connectionConfig, assemblerConfiguration.ssl)
    }

    private val ALTER_FOREIGN_SERVER_PORT_SQL = "ALTER SERVER ${AssemblerConnectionManager.PRODUCTION_SERVER} " +
                    "OPTIONS (SET port '${assemblerConfiguration.foreignPort}')"
}