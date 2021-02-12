package com.openlattice.mechanic.upgrades

import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class SyncOrgPermissionsUpgrade(
        private val toolbox: Toolbox,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val exDbPermMan: ExternalDatabasePermissioningService,
        private val dbCreds: DbCredentialService
) : Upgrade {

    val logger: Logger = LoggerFactory.getLogger(SyncOrgPermissionsUpgrade::class.java)

    private val entitySets = toolbox.entitySets
    private val propertyTypes = toolbox.propertyTypes
    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap(toolbox.hazelcast)
    private val principalTrees = HazelcastMap.PRINCIPAL_TREES.getMap(toolbox.hazelcast)
    private val princpals = HazelcastMap.PRINCIPALS.getMap(toolbox.hazelcast)
    private val permissions = HazelcastMap.PERMISSIONS.getMap(toolbox.hazelcast)
    private val dbCredentials = HazelcastMap.DB_CREDS.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {
//        val addRolesToDbCreds = addRolesToDbCreds()
//        val assemblies = initializeAssemblyPermissions()
        val mapPTrees = mapAllPrincipalTrees()
        val createPRoles = createAssignAllPermRoles()
        if (mapPTrees && createPRoles) {
            logger.info("omg yay")
            return true
        }
        logger.error("failure :(")
//        logger.error("Sync permissions upgrade failed, final status:\n" +
//                "addRolesToDbCreds: {}\n" +
//                "updatePermissionsForAssemblies: {}\n" +
//                "mapAllPrincipalTrees: {}\n" +
//                "createAllPermRoles: {}\n", addRolesToDbCreds, assemblies, mapPTrees, createPRoles)
        return false
    }

    private fun addRolesToDbCreds(): Boolean {
        princpals.values(
                Predicates.equal<AclKey, SecurablePrincipal>(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.ROLE)
        ).forEach { rolePrincipal ->
            dbCreds.getOrCreateRoleAccount(rolePrincipal)
        }
        return true
    }

    private fun initializeAssemblyPermissions(): Boolean {
        val assembliesByOrg = entitySets.values.filter { es ->
            es.flags.contains(EntitySetFlag.TRANSPORTED)
        }.groupBy { it.organizationId }

        val etIdsToPtFqns = toolbox.entityTypes.values.associate {
            it.id to it.properties.map { pt ->
                PropertyTypeIdFqn.fromPropertyType(toolbox.propertyTypes.getValue(pt))
            }.toMutableSet()
        }

        // final shape is etid -> Set<PtIdFqn>
        assembliesByOrg.map { (orgId, entitySets) ->
            exConnMan.connectToOrg(orgId).use { hds ->
                entitySets.forEach { es ->
                    // this will create all permission roles for transporter use
                    exDbPermMan.initializeAssemblyPermissions(
                            hds,
                            es.id,
                            es.name,
                            etIdsToPtFqns.getValue(es.entityTypeId)
                    )
                }
            }
        }
        return true
    }

    private fun removeAllOldPermissionRoles() {
        val assembliesByOrg = entitySets.values.filter { es ->
            es.flags.contains(EntitySetFlag.TRANSPORTED)
        }.groupBy {
            it.organizationId
        }

        val colsByEt = transporterState.mapValues { (_, columns) ->
            columns.keys.map {
                propertyTypes.getValue(it).type
            }
        }

        assembliesByOrg.map { (orgId, entitySets) ->
            val statements = entitySets.flatMapTo(mutableSetOf()) { es ->
                colsByEt.getValue(es.entityTypeId).map { colName ->
                    "DROP ROLE ${roleName(es.name, colName.toString())}"
                }
            }
            exConnMan.connectToOrg(orgId).use { hds ->
                hds.connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        statements.forEach {
                            stmt.addBatch(it)
                        }
                        stmt.executeBatch()
                    }
                }
            }
        }
    }

    private fun roleName(entitySetName: String, columnName: String): String {
        return "${entitySetName}_$columnName"
    }

    private fun createAssignAllPermRoles(): Boolean {
        logger.info("create assign all perm roles")
        val filteredPermissionEntries = permissions.entrySet(
                Predicates.`in`<AceKey, AceValue>(PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX,
                        SecurableObjectType.PropertyTypeInEntitySet,
                        SecurableObjectType.OrganizationExternalDatabaseColumn
                )
        )
        val acls = filteredPermissionEntries.groupBy({
            it.key.aclKey
        }, { (aceKey, aceVal) ->
            val prin = aceKey.principal
            val perms = aceVal.permissions
            val exp = aceVal.expirationDate
            Ace(prin, perms, exp)
        }).map { (aclkey, aces) ->
            Acl(aclkey, aces)
        }
        exDbPermMan.executePrivilegesUpdate(Action.SET, acls)
        logger.info("successfully created + assigned all perm roles!")
        return true
    }

    private fun mapAllPrincipalTrees(): Boolean {
        logger.info("mapping all principal trees")
        val validAclKeys = dbCredentials.keys.toSet()

        principalTrees.entries.flatMap { (target, sources) ->
            sources.map { source ->
                source to target
            }
        }.filter {
            validAclKeys.contains(it.first) && validAclKeys.contains(it.second)
        }.groupBy({
            it.first
        }, {
            it.second
        }).forEach { (source, targets) ->
            exDbPermMan.addPrincipalToPrincipals(source, targets.toSet())
        }
        logger.info("successfully mapped principal trees!")
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_02_05.value
    }
}
