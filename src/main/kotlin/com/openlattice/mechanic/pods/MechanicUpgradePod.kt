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
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.authorization.*
import com.openlattice.conductor.rpc.ConductorConfiguration
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.DataDeletionManager
import com.openlattice.data.EntityKeyIdService
import com.openlattice.data.ids.PostgresEntityKeyIdService
import com.openlattice.data.storage.*
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.data.storage.postgres.PostgresEntityDatastore
import com.openlattice.datasets.DataSetService
import com.openlattice.datastore.pods.ByteBlobServicePod
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.graph.Graph
import com.openlattice.graph.core.GraphService
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ioc.providers.LateInitProvider
import com.openlattice.jdbc.DataSourceManager
import com.openlattice.linking.LinkingQueryService
import com.openlattice.linking.PostgresLinkingFeedbackService
import com.openlattice.linking.graph.PostgresLinkingQueryService
import com.openlattice.mechanic.MechanicCli.Companion.UPGRADE
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.upgrades.DeleteOrgMetadataEntitySets
import com.openlattice.mechanic.upgrades.V3StudyMigrationUpgrade
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.scrunchie.search.ConductorElasticsearchImpl
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import mechanic.src.main.kotlin.com.openlattice.mechanic.upgrades.AddPgAuditToExistingOrgs
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import javax.annotation.PostConstruct
import javax.inject.Inject


@Configuration
@Import(MechanicToolboxPod::class, AuditingConfigurationPod::class, ByteBlobServicePod::class)
@Profile(UPGRADE)
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
class MechanicUpgradePod {

    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration

    @Inject
    private lateinit var auditingConfiguration: AuditingConfiguration

    @Inject
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    @Inject
    private lateinit var configurationLoader: ConfigurationLoader

    @Inject
    private lateinit var dataSourceManager: DataSourceManager

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var executor: ListeningExecutorService

    @Inject
    private lateinit var externalDbConnMan: ExternalDatabaseConnectionManager

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var hazelcastClientProvider: HazelcastClientProvider

    @Inject
    private lateinit var hikariDataSource: HikariDataSource

    @Inject
    private lateinit var lateInitProvider: LateInitProvider

    @Inject
    private lateinit var metricRegistry: MetricRegistry

    @Inject
    private lateinit var rhizomeConfiguration: RhizomeConfiguration

    @Inject
    private lateinit var toolbox: Toolbox

    @Bean
    fun conductorConfiguration(): ConductorConfiguration {
        return configurationLoader.logAndLoad("conductor", ConductorConfiguration::class.java)
    }

    @Bean
    fun aclKeyReservationService(): HazelcastAclKeyReservationService {
        return HazelcastAclKeyReservationService(hazelcastInstance)
    }

    @Bean
    fun jobService(): HazelcastJobService {
        return HazelcastJobService(hazelcastInstance)
    }

    @Bean
    fun elasticsearchApi(): ConductorElasticsearchApi {
        return ConductorElasticsearchImpl(conductorConfiguration().searchConfiguration)
    }

    @Bean
    fun dataSetService(): DataSetService {
        return DataSetService(hazelcastInstance, elasticsearchApi())
    }

    @Bean
    fun postgresTypeManager(): PostgresTypeManager {
        return PostgresTypeManager(hikariDataSource, hazelcastInstance)
    }

    @Bean
    fun postgresLinkingFeedbackQueryService(): PostgresLinkingFeedbackService {
        return PostgresLinkingFeedbackService(hikariDataSource, hazelcastInstance)
    }

    @Bean
    fun dataSourceResolver(): DataSourceResolver {
        dataSourceManager.registerTablesWithAllDatasources(PostgresTable.E)
        dataSourceManager.registerTablesWithAllDatasources(PostgresTable.DATA)
        return DataSourceResolver(hazelcastInstance, dataSourceManager)
    }

    @Bean
    fun principalsMapManager(): PrincipalsMapManager {
        return HazelcastPrincipalsMapManager(hazelcastInstance, aclKeyReservationService())
    }

    @Bean
    fun authorizationService(): AuthorizationManager {
        return HazelcastAuthorizationService(hazelcastInstance, eventBus, principalsMapManager())
    }

    @Bean
    fun schemaManager(): HazelcastSchemaManager {
        return HazelcastSchemaManager(hazelcastInstance, postgresTypeManager())
    }

    @Bean
    fun edmManager(): EdmManager {
        return EdmService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationService(),
                postgresTypeManager(),
                schemaManager(),
                dataSetService()
        )
    }

    @Bean
    fun entitySetService(): EntitySetManager {
        return EntitySetService(
                hazelcastInstance,
                eventBus,
                aclKeyReservationService(),
                authorizationService(),
                edmManager(),
                hikariDataSource,
                dataSetService(),
                auditingConfiguration
        )
    }

    @Bean
    fun auditRecordEntitySetsManager(): AuditRecordEntitySetsManager {
        return entitySetService().getAuditRecordEntitySetsManager()
    }

    @Bean
    fun dataQueryService(): PostgresEntityDataQueryService {
        return PostgresEntityDataQueryService(
                dataSourceResolver(),
                byteBlobDataManager
        )
    }

    @Bean
    fun idGenerationService(): HazelcastIdGenerationService {
        return HazelcastIdGenerationService(hazelcastClientProvider)
    }

    @Bean
    fun idService(): EntityKeyIdService {
        return PostgresEntityKeyIdService(
                dataSourceResolver(),
                idGenerationService()
        )
    }

    @Bean
    fun graphService(): GraphService {
        return Graph(
                dataSourceResolver(),
                entitySetService(),
                dataQueryService(),
                idService(),
                MetricRegistry()
        )
    }

    @Bean
    fun lqs(): LinkingQueryService {
        return PostgresLinkingQueryService(hikariDataSource)
    }

    @Bean
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
    fun dataDeletionService(): DataDeletionManager {
        val entitySetService = entitySetService()
        return DataDeletionService(
                entitySetService,
                authorizationService(),
                entityDatastore(entitySetService),
                graphService(),
                jobService(),
        )
    }

    @Bean
    fun deleteOrgMetadataEntitySets(): DeleteOrgMetadataEntitySets {
        return DeleteOrgMetadataEntitySets(
                toolbox,
                auditRecordEntitySetsManager(),
                dataDeletionService(),
                entitySetService(),
                jobService()
        )
    }

    @Bean
    fun addPgAuditToExistingOrgs(): AddPgAuditToExistingOrgs {
        return AddPgAuditToExistingOrgs(
            toolbox,
            externalDbConnMan,
        )
    }

    @Bean
    fun v3StudyMigration(): V3StudyMigrationUpgrade {
        return V3StudyMigrationUpgrade(
            toolbox,
            hikariDataSource,
            dataQueryService()
        )
    }

    @PostConstruct
    fun post() {
        lateInitProvider.setDataSourceResolver(dataSourceResolver())
    }
}
