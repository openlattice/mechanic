/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
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
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.kryptnostic.conductor.pods;

import java.io.IOException;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.authorization.AbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizationQueryService;
import com.dataloom.authorization.HazelcastAbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.HazelcastAuthorizationService;
import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.edm.properties.CassandraTypeManager;
import com.dataloom.edm.schemas.SchemaQueryService;
import com.dataloom.edm.schemas.cassandra.CassandraSchemaQueryService;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.hazelcast.HazelcastQueue;
import com.dataloom.hazelcast.serializers.QueryResultStreamSerializer;
import com.dataloom.mail.config.MailServiceRequirements;
import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.ConductorConfiguration;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.datastore.services.CassandraEntitySetManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.kindling.search.ConductorElasticsearchImpl;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.kryptnostic.rhizome.core.Cutting;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.SparkPod;
import com.kryptnostic.sparks.ConductorSparkImpl;
import com.kryptnostic.sparks.LoomCassandraConnectionFactory;

@Configuration
@Import( SparkPod.class )
public class ConductorSparkPod {
    static {
        LoomCassandraConnectionFactory.configureSparkPod();
    }

    @Inject
    private CassandraConfiguration      cassandraConfiguration;

    @Inject
    private Session                     session;

    @Inject
    private QueryResultStreamSerializer qrss;

    @Inject
    private HazelcastInstance           hazelcastInstance;

    @Inject
    private SparkSession                sparkSession;

    @Inject
    private ConfigurationService        configurationService;

    @Inject
    private EventBus                    eventBus;

    @Inject
    private Cutting                     cutting;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService( cassandraConfiguration.getKeyspace(), session, hazelcastInstance );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService(), eventBus );
    }

    @Bean
    public AbstractSecurableObjectResolveTypeService securableObjectTypes() {
        return new HazelcastAbstractSecurableObjectResolveTypeService( hazelcastInstance );
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new CassandraSchemaQueryService( DatastoreConstants.KEYSPACE, session );
    }

    @Bean
    public CassandraEntitySetManager entitySetManager() {
        return new CassandraEntitySetManager( DatastoreConstants.KEYSPACE, session, authorizationManager() );
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager( DatastoreConstants.KEYSPACE, hazelcastInstance, schemaQueryService() );
    }

    @Bean
    public CassandraTypeManager entityTypeManager() {
        return new CassandraTypeManager( DatastoreConstants.KEYSPACE, session );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public MailServiceRequirements mailServiceRequirements() {
        return () -> hazelcastInstance.getQueue( HazelcastQueue.EMAIL_SPOOL.name() );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                DatastoreConstants.KEYSPACE,
                session,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                entitySetManager(),
                entityTypeManager(),
                schemaManager() );
    }

    @Bean
    public SparkContextJavaFunctions sparkContextJavaFunctions() {
        return CassandraJavaUtil.javaFunctions( sparkSession.sparkContext() );
    }

    @Bean
    public ConductorSparkApi api() throws UnknownHostException, IOException, InterruptedException {
        cutting.addPods( CassandraPod.class );
        ConductorSparkApi api = new ConductorSparkImpl(
                DatastoreConstants.KEYSPACE,
                sparkSession,
                sparkContextJavaFunctions(),
                dataModelService(),
                hazelcastInstance,
                cutting );
        return api;
    }

    @Bean
    public ConductorElasticsearchApi elasticsearchApi() throws UnknownHostException, IOException {
        return new ConductorElasticsearchImpl(
                configurationService.getConfiguration( ConductorConfiguration.class ).getSearchConfiguration() );
    }

    @PostConstruct
    public void setSession() {
        qrss.setSession( session );
    }
}
