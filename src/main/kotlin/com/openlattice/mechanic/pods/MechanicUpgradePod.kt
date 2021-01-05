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

import com.codahale.metrics.MetricRegistry
import com.geekbeast.hazelcast.HazelcastClientProvider
import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.Assembler
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.HazelcastAuthorizationService
import com.openlattice.data.DataGraphManager
import com.openlattice.data.DataGraphService
import com.openlattice.data.EntityKeyIdService
import com.openlattice.data.ids.PostgresEntityKeyIdService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.EntityDatastore
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.PostgresEntityDatastore
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.pods.ByteBlobServicePod
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.SchemaQueryService
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.graph.Graph
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.HazelcastLongIdService
import com.openlattice.linking.LinkingQueryService
import com.openlattice.linking.PostgresLinkingFeedbackService
import com.openlattice.linking.graph.PostgresLinkingQueryService
import com.openlattice.mechanic.MechanicCli.Companion.UPGRADE
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.*
import com.openlattice.notifications.sms.PhoneNumberService
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import com.openlattice.organizations.mapstores.OrganizationsMapstore
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import javax.inject.Inject

@Configuration
@Import(MechanicToolboxPod::class, AuditingConfigurationPod::class, ByteBlobServicePod::class)
@Profile(UPGRADE)
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
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

    @Inject
    private lateinit var externalDatabaseConnectionManager: ExternalDatabaseConnectionManager

    @Inject
    private lateinit var hazelcastClientProvider: HazelcastClientProvider

    @Inject
    private lateinit var metricRegistry: MetricRegistry

    @Inject
    private lateinit var byteBlobDataManager: ByteBlobDataManager

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
                assemblerConfiguration,
                externalDatabaseConnectionManager
        )
    }

    @Bean
    fun materializedEntitySetRefresh(): MaterializedEntitySetRefresh {
        return MaterializedEntitySetRefresh(toolbox)
    }

    @Bean
    fun organizationDbUserSetup(): OrganizationDbUserSetup {
        return OrganizationDbUserSetup(
                mapstoresPod.organizationAssemblies() as OrganizationAssemblyMapstore,
                assemblerConfiguration,
                externalDatabaseConnectionManager
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
    fun rectifyOrganizationsUpgrade(): RectifyOrganizationsUpgrade {
        val dbCredService = DbCredentialService(
                toolbox.hazelcast,
                HazelcastLongIdService(hazelcastClientProvider)
        )
        val assembler = Assembler(
                dbCredService,
                toolbox.hds,
                authorizationManager(),
                securePrincipalsManager(),
                metricRegistry,
                toolbox.hazelcast,
                eventBus
        )
        return RectifyOrganizationsUpgrade(
                toolbox,
                assembler
        )
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
    fun clearJSONOrganizationRoles(): ClearJSONOrganizationRoles {
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

    /* SETUP FOR EntitySetManager */
    @Bean
    fun partitionManager(): PartitionManager {
        return PartitionManager(hazelcastInstance, hikariDataSource)
    }

    @Bean
    fun postgresTypeManager(): PostgresTypeManager {
        return PostgresTypeManager(hikariDataSource, hazelcastInstance)
    }

    @Bean
    fun schemaQueryService(): SchemaQueryService {
        return postgresTypeManager()
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

    fun uninitializedOrganizationMetadataEntitySetsService(): OrganizationMetadataEntitySetsService {
        val service = OrganizationMetadataEntitySetsService(edmManager(), authorizationManager())
        return service
    }

    fun organizationMetadataEntitySetsService(): OrganizationMetadataEntitySetsService {
        val service = uninitializedOrganizationMetadataEntitySetsService()
        val entitySetService = uninitializedEntitySetManager(service)
        service.organizationService = uninitializedOrganizationService(service)
        service.entitySetsManager = entitySetService
        service.dataGraphManager = dataGraphManager(entitySetService)
        return service
    }

    fun uninitializedEntitySetManager(metadataService: OrganizationMetadataEntitySetsService): EntitySetManager {
        return EntitySetService(
                hazelcastInstance,
                eventBus,
                aclKeyReservationService(),
                authorizationManager(),
                partitionManager(),
                edmManager(),
                hikariDataSource,
                metadataService,
                auditingConfiguration
        )
    }

    @Bean
    fun removeLinkingDataFromDataTable(): RemoveLinkingDataFromDataTable {
        return RemoveLinkingDataFromDataTable(toolbox)
    }

    @Bean
    fun createStagingSchemaForExistingOrgs(): CreateStagingSchemaForExistingOrgs {
        return CreateStagingSchemaForExistingOrgs(toolbox, assemblerConfiguration, externalDatabaseConnectionManager)
    }

    @Bean
    fun grantAppRolesReadOnEntitySetCollections(): GrantAppRolesReadOnEntitySetCollections {
        return GrantAppRolesReadOnEntitySetCollections(toolbox, eventBus)
    }

    @Bean
    fun addDbCredUsernames(): AddDbCredUsernames {
        return AddDbCredUsernames(toolbox, assemblerConfiguration)
    }

    @Bean
    fun createAtlasUsersAndSetPermissions(): CreateAtlasUsersAndSetPermissions {
        return CreateAtlasUsersAndSetPermissions(toolbox, externalDatabaseConnectionManager)
    }

    @Bean
    fun createAndPopulateOrganizationDatabaseTable(): CreateAndPopulateOrganizationDatabaseTable {
        return CreateAndPopulateOrganizationDatabaseTable(toolbox, externalDatabaseConnectionManager)
    }

    @Bean
    fun createAllOrgMetadataEntitySets(): CreateAllOrgMetadataEntitySets {
        return CreateAllOrgMetadataEntitySets(
                toolbox,
                organizationMetadataEntitySetsService(),
                securePrincipalsManager(),
                authorizationManager()
        )
    }

    @Bean
    fun cleanOutOrgMembersAndRoles(): CleanOutOrgMembersAndRoles {
        return CleanOutOrgMembersAndRoles(toolbox, securePrincipalsManager(), authorizationManager())
    }

    @Bean
    fun grantAllOnStagingSchemaToOrgUser(): GrantAllOnStagingSchemaToOrgUser {
        return GrantAllOnStagingSchemaToOrgUser(toolbox, externalDatabaseConnectionManager)
    }

    @Bean
    fun cleanUpDeletedUsers(): CleanUpDeletedUsers {
        return CleanUpDeletedUsers(toolbox)
    }

    fun uninitializedOrganizationService(metadataService: OrganizationMetadataEntitySetsService): HazelcastOrganizationService {
        val dbCredService = DbCredentialService(
                toolbox.hazelcast,
                HazelcastLongIdService(hazelcastClientProvider)
        )
        val assembler = Assembler(
                dbCredService,
                toolbox.hds,
                authorizationManager(),
                securePrincipalsManager(),
                metricRegistry,
                toolbox.hazelcast,
                eventBus
        )

        return HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                securePrincipalsManager(),
                PhoneNumberService(hazelcastInstance),
                partitionManager(),
                assembler,
                metadataService
        )
    }

    @Bean
    fun dataQueryService(): PostgresEntityDataQueryService {
        return PostgresEntityDataQueryService(
                hikariDataSource,
                hikariDataSource,
                byteBlobDataManager,
                partitionManager()
        )
    }

    @Bean
    fun postgresLinkingFeedbackQueryService(): PostgresLinkingFeedbackService {
        return PostgresLinkingFeedbackService(hikariDataSource, hazelcastInstance)
    }

    @Bean
    fun lqs(): LinkingQueryService {
        return PostgresLinkingQueryService(hikariDataSource, partitionManager())
    }

    fun entityDatastore(entitySetManager: EntitySetManager): EntityDatastore {
        return PostgresEntityDatastore(
                dataQueryService(),
                edmManager(),
                entitySetManager,
                metricRegistry,
                eventBus,
                postgresLinkingFeedbackQueryService(),
                lqs()
        )
    }

    @Bean
    fun idGenerationService(): HazelcastIdGenerationService {
        return HazelcastIdGenerationService(hazelcastClientProvider)
    }

    @Bean
    fun idService(): EntityKeyIdService {
        return PostgresEntityKeyIdService(
                hikariDataSource,
                idGenerationService(),
                partitionManager())
    }

    @Bean
    fun jobService(): HazelcastJobService {
        return HazelcastJobService(hazelcastInstance)
    }


    fun dataGraphManager(entitySetManager: EntitySetManager): DataGraphManager {
        val graphService = Graph(hikariDataSource,
                hikariDataSource,
                entitySetManager,
                partitionManager(),
                dataQueryService(),
                idService(),
                MetricRegistry())

        return DataGraphService(
                graphService,
                idService(),
                entityDatastore(entitySetManager),
                jobService()
        )
    }

    @Bean
    fun populateOrgMetadataEntitySets(): PopulateOrgMetadataEntitySets {
        return PopulateOrgMetadataEntitySets(
                toolbox,
                organizationMetadataEntitySetsService()
        )
    }

}