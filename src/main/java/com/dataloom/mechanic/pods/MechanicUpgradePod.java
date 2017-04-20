package com.dataloom.mechanic.pods;

import com.dataloom.mechanic.upgrades.DataTableMigrator;
import com.datastax.driver.core.Session;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class MechanicUpgradePod {
    private static final Logger logger = LoggerFactory.getLogger( MechanicUpgradePod.class );
    @Inject
    private CassandraConfiguration cassandraConfiguration;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private Session session;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private EventBus eventBus;

    @Bean
    public DataTableMigrator migrator() {
        return new DataTableMigrator( session, cassandraConfiguration.getKeyspace() , executor );
    }

    @PostConstruct
    public void upgrade() {
        logger.info( "Starting upgrade!" );

        long count = migrator().upgrade();

        logger.info( "Upgrade complete! Migrated {} rows.", count );
    }
}
