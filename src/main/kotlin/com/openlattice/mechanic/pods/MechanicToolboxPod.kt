package com.openlattice.mechanic.pods

import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresTableManager
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class MechanicToolboxPod {

    @Inject
    private lateinit var hikariDataSource: HikariDataSource

    @Inject
    private lateinit var tableManager: PostgresTableManager

    @Inject
    private lateinit var executor: ListeningExecutorService

    @Inject
    private lateinit var mapstoresPod: MapstoresPod

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var configurationLoader: ConfigurationLoader


    @Bean
    fun toolbox(): Toolbox {
        return Toolbox(
                tableManager,
                hikariDataSource,
                mapstoresPod.propertyTypeMapstore() as PropertyTypeMapstore,
                mapstoresPod.entityTypeMapstore() as EntityTypeMapstore,
                mapstoresPod.entitySetMapstore() as EntitySetMapstore,
                executor,
                hazelcastInstance,
                configurationLoader
        )
    }
}