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
package com.openlattice.mechanic.integrity

import com.google.common.base.Stopwatch
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

class CheckOrphanedEntitySets(private val toolbox: Toolbox) : Check {

    companion object {
        private val logger = LoggerFactory.getLogger(CheckOrphanedEntitySets::class.java)
    }

    override fun check(): Boolean {
        logger.info("Starting to delete entries of orphaned entity sets from securable object types, permissions, " +
                "materialized entity sets and entity set property metadata tables.")
        val sw = Stopwatch.createStarted()


        // check securable objects and permissions

        var deletedEntitySetIdsInSecurableObjects = setOf<UUID>()

        val deletedPermissionCount = toolbox.hds.connection.use { conn ->
            conn.prepareStatement(deleteOrphanedPermissions).use { ps ->

                var deletedSecurableObjectCount = 0

                deletedEntitySetIdsInSecurableObjects = BasePostgresIterable(
                        StatementHolderSupplier(toolbox.hds, deleteOrphanedSecurableObjects)) { rs ->
                    val deletedAclKey = ResultSetAdapters.aclKey(rs)
                    val deletedAclKeyArr = PostgresArrays.createUuidArray(ps.connection, deletedAclKey)
                    ps.setArray(1, deletedAclKeyArr)

                    ps.addBatch()

                    deletedSecurableObjectCount++

                    deletedAclKey
                }.map { it[0] }.toSet()

                logger.info("Deleted $deletedSecurableObjectCount rows from securable object types of following " +
                        "non-existing entity sets $deletedEntitySetIdsInSecurableObjects in " +
                        "${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
                sw.reset().start()

                ps.executeBatch().sum()
            }
        }

        logger.info("Deleted $deletedPermissionCount rows from permissions of following non-existing entity sets " +
                "$deletedEntitySetIdsInSecurableObjects in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
        sw.reset().start()


        // check names

        val deletedEntitySetIdsInNames = BasePostgresIterable(
                PreparedStatementHolderSupplier(toolbox.hds, deleteOrphanedEntitySetNames) { ps ->
                    val entitySetIds = PostgresArrays.createUuidArray(
                            ps.connection, deletedEntitySetIdsInSecurableObjects
                    )
                    ps.setArray(1, entitySetIds)
                }
        ) { rs -> ResultSetAdapters.entitySetId(rs) }


        logger.info("Deleted ${deletedEntitySetIdsInNames.count()} rows of following non-existing entity sets " +
                "${deletedEntitySetIdsInNames.toSet()} from names in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
        sw.reset().start()


        // check acl keys

        val deletedEntitySetIdsInAclKeys = BasePostgresIterable(
                PreparedStatementHolderSupplier(toolbox.hds, deleteOrphanedEntitySetAclKeys) { ps ->
                    val entitySetIds = PostgresArrays.createUuidArray(
                            ps.connection, deletedEntitySetIdsInSecurableObjects
                    )
                    ps.setArray(1, entitySetIds)
                }
        ) { rs -> ResultSetAdapters.entitySetId(rs) }


        logger.info("Deleted ${deletedEntitySetIdsInAclKeys.count()} rows of following non-existing entity sets " +
                "${deletedEntitySetIdsInAclKeys.toSet()} from acl keys in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
        sw.reset().start()


        // check materialized entity sets

        val deletedEntitySetIdsInMaterializedEntitySets = BasePostgresIterable(
                StatementHolderSupplier(toolbox.hds, deleteOrphanedMaterializedEntitySets)
        ) { rs -> ResultSetAdapters.entitySetId(rs) }

        logger.info("Deleted ${deletedEntitySetIdsInMaterializedEntitySets.count()} rows of following non-existing " +
                "entity sets ${deletedEntitySetIdsInMaterializedEntitySets.toSet()} from materialized entity sets in " +
                "${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
        sw.reset().start()


        // check entity set property metadata

        val deletedEntitySetIdsInPropertyMetaData = BasePostgresIterable(
                StatementHolderSupplier(toolbox.hds, deleteOrphanedEntitySetPropertyMetaData)
        ) { rs -> ResultSetAdapters.entitySetId(rs) }


        logger.info("Deleted ${deletedEntitySetIdsInPropertyMetaData.count()} rows of following non-existing entity " +
                "sets ${deletedEntitySetIdsInPropertyMetaData.toSet()} from entity set property metadata in " +
                "${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")

        return true
    }

    // @formatter:off
    private val entitySetIds = "SELECT ${ID.name} FROM ${ENTITY_SETS.name}"

    private val deleteOrphanedSecurableObjects =
            "DELETE FROM ${SECURABLE_OBJECTS.name} " +
            "WHERE ${ACL_KEY.name}[1] NOT IN ( $entitySetIds ) " +
                "AND ( " +
                    "${SECURABLE_OBJECT_TYPE.name} = '${SecurableObjectType.EntitySet}' " +
                    "OR ${SECURABLE_OBJECT_TYPE.name} = '${SecurableObjectType.PropertyTypeInEntitySet}' " +
                ") " +
            "RETURNING ${ACL_KEY.name}"

    private val deleteOrphanedPermissions =
            "DELETE FROM ${PostgresTable.PERMISSIONS.name} " +
            "WHERE ${ACL_KEY.name} = ?"

    private val deleteOrphanedEntitySetNames =
            "DELETE FROM ${NAMES.name} " +
                    "WHERE ${SECURABLE_OBJECTID.name} = ANY( ? )"

    private val deleteOrphanedEntitySetAclKeys =
            "DELETE FROM ${ACL_KEYS.name} " +
                    "WHERE ${SECURABLE_OBJECTID.name} = ANY( ? )"

    private val deleteOrphanedMaterializedEntitySets =
            "DELETE FROM ${MATERIALIZED_ENTITY_SETS.name} " +
            "WHERE ${ENTITY_SET_ID.name} NOT IN ( $entitySetIds ) " +
            "RETURNING ${ENTITY_SET_ID.name}"

    private val deleteOrphanedEntitySetPropertyMetaData =
            "DELETE FROM ${ENTITY_SET_PROPERTY_METADATA.name} " +
            "WHERE ${ENTITY_SET_ID.name} NOT IN ( $entitySetIds ) " +
            "RETURNING ${ENTITY_SET_ID.name}"

    // @formatter:on
}