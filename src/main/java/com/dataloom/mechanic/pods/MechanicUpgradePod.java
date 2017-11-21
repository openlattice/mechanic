package com.dataloom.mechanic.pods;

import com.dataloom.mechanic.upgrades.CassandraToPostgres;
import com.dataloom.mechanic.upgrades.ManualPartitionOfDataTable;
import com.datastax.driver.core.Session;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class MechanicUpgradePod {

    @Inject
    private CassandraConfiguration cassandraConfiguration;

//    @Inject
//    private HazelcastInstance hazelcastInstance;

    @Inject
    private Session session;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private EventBus eventBus;


    @Bean
    public CassandraToPostgres ctp() {
        return new CassandraToPostgres();
    }
    //    @Bean
    //    public DataTableMigrator migrator() {
    //        return new DataTableMigrator( session, cassandraConfiguration.getKeyspace() , executor );
    //    }

    //    @Bean
    //    public EdgeTypeMigrator edgeTypeMigrator() {
    //        return new EdgeTypeMigrator( session, cassandraConfiguration.getKeyspace() );
    //    }

    @Bean
    public ManualPartitionOfDataTable mpodt() {
        return new ManualPartitionOfDataTable( session, cassandraConfiguration.getKeyspace(), executor );
    }

}
