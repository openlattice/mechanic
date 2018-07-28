/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.mechanic.repair

import com.openlattice.edm.PostgresEdmManager
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.zaxxer.hikari.HikariDataSource

/**
 *
 */
class Repairs(
        private val pgEdmManager: PostgresEdmManager,
        private val hds: HikariDataSource,
        private val ptms: PropertyTypeMapstore,
        private val etms: EntityTypeMapstore,
        private val esms: EntitySetMapstore ) {

    fun runRepairs( repairs: Set<String> ) {
        repairs.forEach {
            when (it) {
                "entitysets" -> repairMissingEntitySetTables()
                "propertytypes" -> repairMissingPropertyTypeTables()
            }
        }
    }

    private fun repairMissingEntitySetTables() {

    }

    fun repairMissingPropertyTypeTables() {

    }

}