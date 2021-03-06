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

package com.openlattice.mechanic.integrity

import com.google.common.base.Preconditions.checkState
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.quote
import org.slf4j.LoggerFactory
import java.sql.ResultSetMetaData

/**
 *
 */
private val logger = LoggerFactory.getLogger(EdmChecks::class.java)

class EdmChecks(private val toolbox: Toolbox) : Check {
    override fun check(): Boolean {
        checkPropertyTypesAlignWithTable()
        return true
    }

    private fun checkPropertyTypesAlignWithTable() {

        toolbox.hds.connection.use {
            val connection = it
            toolbox.propertyTypes.values.forEach pt@{
                val propertyTypeName = it.type.fullQualifiedNameAsString
                val propertyTableName = quote(DataTables.propertyTableName(it.id))
                val sql = "SELECT * FROM $propertyTableName LIMIT 1"

                connection.createStatement().use {
                    val rs = it.executeQuery(sql)
                    rs.use {
                        //Check if there are any column mistmatches
                        val maybeColumn = getColumnNames(it.metaData)
                                .filter { it.contains(".") && it != propertyTypeName }

                        when {
                            maybeColumn.isEmpty() -> return@pt
                            maybeColumn.size == 1 -> {
                                val col = maybeColumn.first()
                                logger.info("Expected column {} found column {}... renaming", propertyTypeName, col)
                                val alterSql = "ALTER TABLE $propertyTableName RENAME COLUMN ${quote(col)} TO ${quote(propertyTypeName)}"
                                connection.createStatement().use { it.execute(alterSql) }
                                checkState(
                                        connection.createStatement().executeQuery(sql).use {
                                            getColumnNames(
                                                    it.metaData
                                            ).filter { it.contains(".") && it != propertyTypeName }
                                        }.isEmpty(), "Mismatch still detected after attempting modification."
                                )
                            }
                            else -> {
                                val errMsg = "Unexpected number of columns with a '.'"
                                logger.error(errMsg)
                                throw  IllegalStateException(errMsg)
                            }
                        }

                    }
                }
            }
        }
    }

    private fun getColumnNames(metaData: ResultSetMetaData): Set<String> {
        return (1..metaData.columnCount).map { metaData.getColumnName(it) }.toSet()
    }

}