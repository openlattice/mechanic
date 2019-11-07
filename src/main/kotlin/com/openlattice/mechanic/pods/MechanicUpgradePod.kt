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
package com.openlattice.mechanic.pods

import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizationQueryService
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.HazelcastAuthorizationService
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.ids.IdGenerationMapstore
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.integrity.EdmChecks
import com.openlattice.mechanic.integrity.IntegrityChecks
import com.openlattice.mechanic.retired.DropEdmVersions
import com.openlattice.mechanic.retired.DropPrincipalTree
import com.openlattice.mechanic.retired.EntitySetFlags
import com.openlattice.mechanic.upgrades.*
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.PostgresTableManager
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Profile
import javax.inject.Inject

class MechanicUpgradePod {

    companion object {
        const val INTEGRITY = "integrity"
        const val REGEN = "regen"
    }

    @Inject
    private lateinit var hikariDataSource: HikariDataSource

    @Inject
    private lateinit var tableManager: PostgresTableManager

    @Inject
    private lateinit var executor: ListeningExecutorService

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var mapstoresPod: MapstoresPod

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration


    @Bean
    fun pgEdmManager(): PostgresEdmManager {
        return PostgresEdmManager(hikariDataSource, hazelcastInstance)
    }

    @Bean
    @Profile(REGEN)
    fun regen(): RegenerateIds {
        return RegenerateIds(
                pgEdmManager(),
                hikariDataSource,
                mapstoresPod.propertyTypeMapstore() as PropertyTypeMapstore,
                mapstoresPod.entityTypeMapstore() as EntityTypeMapstore,
                mapstoresPod.entitySetMapstore() as EntitySetMapstore,
                mapstoresPod.idGenerationMapstore() as IdGenerationMapstore,
                mapstoresPod.principalTreesMapstore(),
                executor
        )
    }

    @Bean
    @Profile(INTEGRITY)
    fun integrityChecks(): IntegrityChecks {
        return IntegrityChecks(
                pgEdmManager(),
                hikariDataSource,
                mapstoresPod.propertyTypeMapstore() as PropertyTypeMapstore,
                mapstoresPod.entityTypeMapstore() as EntityTypeMapstore,
                mapstoresPod.entitySetMapstore() as EntitySetMapstore,
                executor
        )
    }

    @Bean
    @Profile(INTEGRITY)
    fun edmChecks(): EdmChecks {
        return EdmChecks(
                hikariDataSource,
                mapstoresPod.propertyTypeMapstore() as PropertyTypeMapstore,
                mapstoresPod.entityTypeMapstore() as EntityTypeMapstore,
                mapstoresPod.entitySetMapstore() as EntitySetMapstore,
                executor
        )
    }

    @Bean
    fun toolbox(): Toolbox {
        return Toolbox(
                tableManager,
                hikariDataSource,
                mapstoresPod.propertyTypeMapstore() as PropertyTypeMapstore,
                mapstoresPod.entityTypeMapstore() as EntityTypeMapstore,
                mapstoresPod.entitySetMapstore() as EntitySetMapstore,
                executor
        )
    }

    @Bean
    fun linking(): Linking {
        return Linking(toolbox())
    }

    @Bean
    fun graph(): GraphProcessing {
        return GraphProcessing(toolbox())
    }

    @Bean
    fun mediaServerUpgrade(): MediaServerUpgrade {
        return MediaServerUpgrade(toolbox())
    }

    fun mediaServerCleanup(): MediaServerCleanup {
        return MediaServerCleanup(toolbox())
    }

    @Bean
    fun readLinking(): ReadLinking {
        return ReadLinking(toolbox())
    }

    @Bean
    fun removeEntitySetTables(): RemoveEntitySetTables {
        return RemoveEntitySetTables(toolbox())
    }

    @Bean
    fun propertyValueIndexing(): PropertyValueIndexing {
        return PropertyValueIndexing(toolbox())
    }

    @Bean
    fun lastMigrateColumnUpgrade(): LastMigrateColumnUpgrade {
        return LastMigrateColumnUpgrade(toolbox())
    }

    @Bean
    fun entitySetFlags(): EntitySetFlags {
        return EntitySetFlags(toolbox())
    }

    @Bean
    fun materializedEntitySets(): MaterializedEntitySets {
        return MaterializedEntitySets(toolbox())
    }

    @Bean
    fun dropEdmVersions(): DropEdmVersions {
        return DropEdmVersions(toolbox())
    }

    @Bean
    fun dropPrincipalTree(): DropPrincipalTree {
        return DropPrincipalTree(toolbox())
    }

    @Bean
    fun materializationForeignServer(): MaterializationForeignServer {
        return MaterializationForeignServer(
                mapstoresPod.organizationAssemblies() as OrganizationAssemblyMapstore,
                assemblerConfiguration)
    }

    @Bean
    fun materializedEntitySetRefresh(): MaterializedEntitySetRefresh {
        return MaterializedEntitySetRefresh(toolbox())
    }

    @Bean
    fun organizationDbUserSetup(): OrganizationDbUserSetup {
        return OrganizationDbUserSetup(
                mapstoresPod.organizationAssemblies() as OrganizationAssemblyMapstore,
                assemblerConfiguration
        )
    }

    @Bean
    fun upgradeCreateDataTable(): CreateDataTable {
        return CreateDataTable(toolbox())
    }

    @Bean
    fun upgradeEntitySetPartitions(): UpgradeEntitySetPartitions {
        return UpgradeEntitySetPartitions(toolbox())
    }

    @Bean
    fun migratePropertyValuesToDataTable(): MigratePropertyValuesToDataTable {
        return MigratePropertyValuesToDataTable(toolbox())
    }

    @Bean
    fun upgradeEdgesTable(): UpgradeEdgesTable {
        return UpgradeEdgesTable(toolbox())
    }

    @Bean
    fun createDataTableIndexes(): CreateDataTableIndexes {
        return CreateDataTableIndexes(toolbox())
    }

    @Bean
    fun resetMigratedVersions(): ResetMigratedVersions {
        return ResetMigratedVersions(toolbox())
    }

    @Bean
    fun upgradeEntityKeyIdsTable(): UpgradeEntityKeyIdsTable {
        return UpgradeEntityKeyIdsTable(toolbox())
    }

    @Bean
    fun insertEntityKeyIdsToDataTable(): InsertEntityKeyIdsToDataTable {
        return InsertEntityKeyIdsToDataTable(toolbox())
    }

    @Bean
    fun dataExpirationUpgrade(): DataExpirationUpgrade {
        return DataExpirationUpgrade(toolbox())
    }

    @Bean
    fun setOriginIdDefaultValueUpgrade(): SetOriginIdDefaultValueUpgrade {
        return SetOriginIdDefaultValueUpgrade(toolbox())
    }

    @Bean
    fun setOriginIdToNonNullUpgrade(): SetOriginIdToNonNullUpgrade {
        return SetOriginIdToNonNullUpgrade(toolbox())
    }

    @Bean
    fun addOriginIdToDataPrimaryKey(): AddOriginIdToDataPrimaryKey {
        return AddOriginIdToDataPrimaryKey(toolbox())
    }

    @Bean
    fun insertDeletedChronicleEdgeIds(): InsertDeletedChronicleEdgeIds {
        return InsertDeletedChronicleEdgeIds(toolbox())
    }

    @Bean
    fun authorizationQueryService(): AuthorizationQueryService {
        return AuthorizationQueryService(hikariDataSource, hazelcastInstance)
    }

    @Bean
    fun aclKeyReservationService(): HazelcastAclKeyReservationService {
        return HazelcastAclKeyReservationService(hazelcastInstance)
    }

    @Bean
    fun authorizationManager(): AuthorizationManager {
        return HazelcastAuthorizationService(hazelcastInstance, authorizationQueryService(), eventBus)
    }

    @Bean
    internal fun securePrincipalsManager(): SecurePrincipalsManager {
        return HazelcastPrincipalService(hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                eventBus)
    }

    @Bean
    internal fun grantPublicSchemaAccessToOrgs(): GrantPublicSchemaAccessToOrgs {
        return GrantPublicSchemaAccessToOrgs(toolbox(), securePrincipalsManager())
    }
}