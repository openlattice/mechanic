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
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.HazelcastAuthorizationService
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.SchemaQueryService
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.mechanic.MechanicCli.Companion.UPGRADE
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.*
import com.openlattice.organizations.mapstores.OrganizationsMapstore
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import javax.inject.Inject

@Configuration
@Import(MechanicToolboxPod::class, AuditingConfigurationPod::class)
@Profile(UPGRADE)
class MechanicUpgradePod {

    @Inject
    private lateinit var hikariDataSource: HikariDataSource

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var mapstoresPod: MapstoresPod

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration

    @Inject
    private lateinit var toolbox: Toolbox

    @Inject
    private lateinit var auditingConfiguration: AuditingConfiguration

    @Bean
    fun linking(): Linking {
        return Linking(toolbox)
    }

    @Bean
    fun graph(): GraphProcessing {
        return GraphProcessing(toolbox)
    }

    @Bean
    fun mediaServerUpgrade(): MediaServerUpgrade {
        return MediaServerUpgrade(toolbox)
    }

    fun mediaServerCleanup(): MediaServerCleanup {
        return MediaServerCleanup(toolbox)
    }

    @Bean
    fun readLinking(): ReadLinking {
        return ReadLinking(toolbox)
    }

    @Bean
    fun propertyValueIndexing(): PropertyValueIndexing {
        return PropertyValueIndexing(toolbox)
    }

    @Bean
    fun lastMigrateColumnUpgrade(): LastMigrateColumnUpgrade {
        return LastMigrateColumnUpgrade(toolbox)
    }

    @Bean
    fun materializedEntitySets(): MaterializedEntitySets {
        return MaterializedEntitySets(toolbox)
    }

    @Bean
    fun materializationForeignServer(): MaterializationForeignServer {
        return MaterializationForeignServer(
                mapstoresPod.organizationAssemblies() as OrganizationAssemblyMapstore,
                assemblerConfiguration)
    }

    @Bean
    fun materializedEntitySetRefresh(): MaterializedEntitySetRefresh {
        return MaterializedEntitySetRefresh(toolbox)
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
        return CreateDataTable(toolbox)
    }

    @Bean
    fun upgradeEntitySetPartitions(): UpgradeEntitySetPartitions {
        return UpgradeEntitySetPartitions(toolbox)
    }

    @Bean
    fun migratePropertyValuesToDataTable(): MigratePropertyValuesToDataTable {
        return MigratePropertyValuesToDataTable(toolbox)
    }

    @Bean
    fun upgradeEdgesTable(): UpgradeEdgesTable {
        return UpgradeEdgesTable(toolbox)
    }

    @Bean
    fun createDataTableIndexes(): CreateDataTableIndexes {
        return CreateDataTableIndexes(toolbox)
    }

    @Bean
    fun resetMigratedVersions(): ResetMigratedVersions {
        return ResetMigratedVersions(toolbox)
    }

    @Bean
    fun upgradeEntityKeyIdsTable(): UpgradeEntityKeyIdsTable {
        return UpgradeEntityKeyIdsTable(toolbox)
    }

    @Bean
    fun insertEntityKeyIdsToDataTable(): InsertEntityKeyIdsToDataTable {
        return InsertEntityKeyIdsToDataTable(toolbox)
    }

    @Bean
    fun dataExpirationUpgrade(): DataExpirationUpgrade {
        return DataExpirationUpgrade(toolbox)
    }

    @Bean
    fun setOriginIdDefaultValueUpgrade(): SetOriginIdDefaultValueUpgrade {
        return SetOriginIdDefaultValueUpgrade(toolbox)
    }

    @Bean
    fun setOriginIdToNonNullUpgrade(): SetOriginIdToNonNullUpgrade {
        return SetOriginIdToNonNullUpgrade(toolbox)
    }

    @Bean
    fun addOriginIdToDataPrimaryKey(): AddOriginIdToDataPrimaryKey {
        return AddOriginIdToDataPrimaryKey(toolbox)
    }

    @Bean
    fun insertDeletedChronicleEdgeIds(): InsertDeletedChronicleEdgeIds {
        return InsertDeletedChronicleEdgeIds(toolbox)
    }

    @Bean
    fun aclKeyReservationService(): HazelcastAclKeyReservationService {
        return HazelcastAclKeyReservationService(hazelcastInstance)
    }

    @Bean
    fun authorizationManager(): AuthorizationManager {
        return HazelcastAuthorizationService(hazelcastInstance, eventBus)
    }

    @Bean
    fun securePrincipalsManager(): SecurePrincipalsManager {
        return HazelcastPrincipalService(hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                eventBus)
    }

    @Bean
    fun grantPublicSchemaAccessToOrgs(): GrantPublicSchemaAccessToOrgs {
        return GrantPublicSchemaAccessToOrgs(
                mapstoresPod.organizationsMapstore() as OrganizationsMapstore,
                securePrincipalsManager(),
                assemblerConfiguration)
    }

    @Bean
    fun dropPartitionsVersionColumn(): DropPartitionsVersionColumn {
        return DropPartitionsVersionColumn(toolbox)
    }

    @Bean
    fun resetEntitySetCountsMaterializedView(): ResetEntitySetCountsMaterializedView {
        return ResetEntitySetCountsMaterializedView(toolbox)
    }

    @Bean
    fun migrateOrganizationsToJsonb(): MigrateOrganizationsToJsonb {
        return MigrateOrganizationsToJsonb(toolbox)
    }

    @Bean
    fun setDataTableIdsFieldLastWriteToCreation(): SetDataTableIdsFieldLastWriteToCreation {
        return SetDataTableIdsFieldLastWriteToCreation(toolbox)
    }

    @Bean
    fun convertAppsToEntityTypeCollections(): ConvertAppsToEntityTypeCollections {
        return ConvertAppsToEntityTypeCollections(toolbox, eventBus)
    }
  
    @Bean
    fun updateAuditEntitySetPartitions(): UpdateAuditEntitySetPartitions {
        return UpdateAuditEntitySetPartitions(toolbox)
    }

    @Bean
    fun adjustNCRICDataDateTimes(): AdjustNCRICDataDateTimes {
        return AdjustNCRICDataDateTimes(toolbox)
    }

    @Bean
    fun addPartitionsToOrgsAndEntitySets(): AddPartitionsToOrgsAndEntitySets {
        return AddPartitionsToOrgsAndEntitySets(toolbox)
    }

    @Bean
    fun clearJSONOrganizationRoles() : ClearJSONOrganizationRoles {
        return ClearJSONOrganizationRoles(toolbox)
    }

    @Bean
    fun updateAppTables(): UpdateAppTables {
        return UpdateAppTables(toolbox)
    }

    @Bean
    fun updateDateTimePropertyHash(): UpdateDateTimePropertyHash {
        return UpdateDateTimePropertyHash(toolbox)
    }

    @Bean
    fun fixAssociationTypeCatogories(): FixAssociationTypeCatogories {
        return FixAssociationTypeCatogories(toolbox)
    }

    @Bean
    fun repartitionOrganizations(): RepartitionOrganizations {
        return RepartitionOrganizations(toolbox)
    }

    @Bean
    fun createMissingEntitySetsForAppConfigs(): CreateMissingEntitySetsForAppConfigs {
        return CreateMissingEntitySetsForAppConfigs(
                toolbox,
                securePrincipalsManager(),
                authorizationManager(),
                entitySetManager(),
                aclKeyReservationService()
        )
    }


    /* SETUP FOR EntitySetManager */
    @Bean
    fun partitionManager(): PartitionManager {
        return PartitionManager(hazelcastInstance, hikariDataSource)
    }

    @Bean
    fun postgresTypeManager(): PostgresTypeManager {
        return PostgresTypeManager(hikariDataSource)
    }

    @Bean
    fun schemaQueryService(): SchemaQueryService {
        return PostgresSchemaQueryService(hikariDataSource)
    }

    @Bean
    fun schemaManager(): HazelcastSchemaManager {
        return HazelcastSchemaManager(hazelcastInstance, schemaQueryService())
    }

    @Bean
    fun edmManager(): EdmManager {
        return EdmService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                postgresTypeManager(),
                schemaManager()
        )
    }


    @Bean
    fun entitySetManager(): EntitySetManager  {
        return EntitySetService(
                hazelcastInstance,
                eventBus,
                aclKeyReservationService(),
                authorizationManager(),
                partitionManager(),
                edmManager(),
                hikariDataSource,
                auditingConfiguration
        )
    }
}