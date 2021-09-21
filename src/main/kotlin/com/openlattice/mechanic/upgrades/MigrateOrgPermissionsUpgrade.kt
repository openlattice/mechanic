package com.openlattice.mechanic.upgrades

import com.google.common.base.Stopwatch
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.hazelcast.query.Predicates
import com.openlattice.authorization.Ace
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.Acl
import com.openlattice.authorization.Action
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresArrays.createTextArray
import com.openlattice.postgres.PostgresArrays.createUuidArray
import com.openlattice.postgres.PostgresColumn.ACL_KEY
import com.openlattice.postgres.PostgresColumn.EXPIRATION_DATE
import com.openlattice.postgres.PostgresColumn.PERMISSIONS
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE
import com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECT_TYPE
import com.openlattice.postgres.PostgresTableDefinition
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.TimeUnit

const val SECURABLE_OBJECT_TYPE_INDEX = "securableObjectType"
const val LEGACY_PERMISSIONS_HZMAP = HazelcastMap<AceKey, AceValue>("LEGACY_PERMISSIONS")
const val LEGACY_PERMISSIONS_POSTGRES_TABLE = PostgresTableDefinition("legacy_permissions")
    .addColumns(
        ACL_KEY,
        PRINCIPAL_TYPE,
        PRINCIPAL_ID,
        PERMISSIONS,
        EXPIRATION_DATE,
        SECURABLE_OBJECT_TYPE)
    .primaryKey(ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID)

class LegacyPermissionMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<AceKey, AceValue>(
    LEGACY_PERMISSIONS_HZMAP,
    LEGACY_PERMISSIONS_POSTGRES_TABLE,
    hds
) {
    override fun bind(ps: PreparedStatement, key: AceKey, value: AceValue) {
        val permissions = createTextArray(
            ps.getConnection(),
            value.getPermissions().stream().map(Permission::name)
        )
        val expirationDate = value.getExpirationDate()
        val securableObjectType = value.getSecurableObjectType().name()

        var index = bind(ps, key, 1)

        //create
        ps.setArray(index++, permissions);
        ps.setObject(index++, expirationDate);
        ps.setString(index++, securableObjectType);

        //update
        ps.setArray(index++, permissions);
        ps.setObject(index++, expirationDate);
        ps.setString(index++, securableObjectType);
    }

    override fun bind(ps: PreparedStatement, key: AceKey, offset: Int): Int {
        var index = offset

        val p = key.getPrincipal()
        ps.setArray(index++, createUuidArray(ps.getConnection(), key.getAclKey().stream()))
        ps.setString(index++, p.getType().name());
        ps.setString(index++, p.getId())

        return index
    }

    override fun mapToKey(rs: ResultSet): AceKey {
        ResultSetAdapters.aceKey(rs);
    }

    override fun mapToValue(rs: ResultSet): AceValue {
        // I am assuming there's no NULL securableObjectTypes
        return AceValue(
            ResultSetAdapters.permissions(rs),
            ResultSetAdapters.securableObjectType(rs),
            ResultSetAdapters.expirationDate(rs)
        )
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
            // .addIndexConfig(IndexConfig(IndexType.HASH, ACL_KEY_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_TYPE_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.HASH, PERMISSIONS_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.SORTED, EXPIRATION_DATE_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, SECURABLE_OBJECT_TYPE_INDEX))
            .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): AceKey {
        return AceKey(
            AclKey(UUID.randomUUID()),
            TestDataFactory.userPrincipal()
        )
    }

    override fun generateTestValue(): AceValue {
        return TestDataFactory.aceValue()
    }
}

class MigrateOrgPermissionsUpgrade(
    toolbox: Toolbox,
    private val exDbPermMan: ExternalDatabasePermissioningService
): Upgrade {

    val logger: Logger = LoggerFactory.getLogger(MigrateOrgPermissionsUpgrade::class.java)

    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(toolbox.hazelcast)
    private val legacyPermissions = LEGACY_PERMISSIONS_HZMAP.getMap(toolbox.hazelcast)

    override fun upgrade(): Boolean {

        logger.info("starting migration")

        try {
            val timer = Stopwatch.createStarted()
            val targetOrgId: UUID? = null

            val acls = legacyPermissions.entrySet(
                Predicates.`in`(
                    SECURABLE_OBJECT_TYPE_INDEX,
                    SecurableObjectType.OrganizationExternalDatabaseColumn
                )
            ).filter { entry ->
                orgIdPredicate(entry, targetOrgId)
            }.groupBy({ it.key.aclKey }, { (aceKey, aceVal) ->
                Ace(aceKey.principal, aceVal.permissions, aceVal.expirationDate)
            })
            .map { (aclKey, aces) -> Acl(aclKey, aces) }

            // actual permission migration
            val assignSuccess = assignAllPermissions(acls)
            logger.info(
                "granting permissions took {} ms - {}",
                timer.elapsed(TimeUnit.MILLISECONDS),
                acls
            )

            if (assignSuccess) {
                return true
            }

            logger.error("migration failed - revoke {} grant {}", assignSuccess)
            return false
        } catch (e: Exception) {
            logger.error("something went wrong with the migration", e)
        }

        return true
    }

    private fun assignAllPermissions(acls: List<Acl>): Boolean {
        logger.info("Granting direct permissions")
        exDbPermMan.executePrivilegesUpdate(Action.SET, acls)
        return true
    }

    private fun orgIdPredicate(entry: MutableMap.MutableEntry<AceKey, AceValue>, orgId: UUID?): Boolean {
        try {
            val tableId = entry.key.aclKey[0]
            val securableObjectType = entry.value.securableObjectType
            return when (securableObjectType) {
                SecurableObjectType.OrganizationExternalDatabaseColumn -> {
                    externalTables[tableId]!!.organizationId == orgId || orgId == null
                }
                else -> {
                    logger.error("SecurableObjectType {} is unexpected, filtering out {}", securableObjectType, entry)
                    false
                }
            }
        }
        catch (e: Exception) {
            logger.error("something went wrong filtering permissions for {}", entry, e)
            return false
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
