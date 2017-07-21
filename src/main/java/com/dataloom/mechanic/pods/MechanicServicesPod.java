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

package com.dataloom.mechanic.pods;

import com.dataloom.auditing.AuditQueryService;
import com.dataloom.auditing.HazelcastAuditLoggingService;
import com.dataloom.authorization.*;
import com.dataloom.clustering.ClusteringPartitioner;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.edm.properties.CassandraTypeManager;
import com.dataloom.edm.schemas.SchemaQueryService;
import com.dataloom.edm.schemas.cassandra.CassandraSchemaQueryService;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.linking.CassandraLinkingGraphsQueryService;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.neuron.Neuron;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.dataloom.organizations.roles.HazelcastRolesService;
import com.dataloom.organizations.roles.RolesManager;
import com.dataloom.organizations.roles.RolesQueryService;
import com.dataloom.organizations.roles.TokenExpirationTracker;
import com.dataloom.requests.*;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.services.CassandraEntitySetManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.ODataStorageService;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.pods.CassandraPod;
import digital.loom.rhizome.authentication.Auth0Pod;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

@Configuration
@Import( { CassandraPod.class, Auth0Pod.class } )
public class MechanicServicesPod {

    @Inject
    private CassandraConfiguration cassandraConfiguration;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private Session session;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private ListeningExecutorService executor;

//    @Inject
//    Neuron neuron;

    @Inject
    private EventBus eventBus;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        FullQualifedNameJacksonSerializer.registerWithMapper( mapper );
        FullQualifedNameJacksonDeserializer.registerWithMapper( mapper );
        return mapper;
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
        return new CassandraSchemaQueryService( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public CassandraEntitySetManager entitySetManager() {
        return new CassandraEntitySetManager( cassandraConfiguration.getKeyspace(), session, authorizationManager() );
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager(
                cassandraConfiguration.getKeyspace(),
                hazelcastInstance,
                schemaQueryService() );
    }

    @Bean
    public CassandraTypeManager entityTypeManager() {
        return new CassandraTypeManager( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                cassandraConfiguration.getKeyspace(),
                session,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                entitySetManager(),
                entityTypeManager(),
                schemaManager() );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public HazelcastListingService hazelcastListingService() {
        return new HazelcastListingService( hazelcastInstance );
    }

    @Bean
    public CassandraLinkingGraphsQueryService clgqs() {
        return new CassandraLinkingGraphsQueryService( cassandraConfiguration.getKeyspace(), session );
    }
    @Bean
    public HazelcastLinkingGraphs linkingGraph() {
        return new HazelcastLinkingGraphs( hazelcastInstance , clgqs() );
    }

    @Bean
    public ODataStorageService odataStorageService() {
        return new ODataStorageService(
                cassandraConfiguration.getKeyspace(),
                hazelcastInstance,
                dataModelService(),
                session );
    }

    @Bean
    public RolesQueryService rolesQueryService() {
        return new RolesQueryService( session );
    }

    @Bean
    public TokenExpirationTracker tokenTracker() {
        return new TokenExpirationTracker( hazelcastInstance );
    }

    @PostConstruct
    public void setExpiringTokenTracker() {
        Principals.setExpiringTokenTracker( tokenTracker() );
    }

    @Bean
    public RolesManager rolesService() {
        return new HazelcastRolesService(
                hazelcastInstance,
                rolesQueryService(),
                aclKeyReservationService(),
                tokenTracker(),
                userDirectoryService(),
                securableObjectTypes(),
                authorizationManager() );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                cassandraConfiguration.getKeyspace(),
                session,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                userDirectoryService(),
                rolesService() );
    }

    @Bean
    public UserDirectoryService userDirectoryService() {
        return new UserDirectoryService( auth0Configuration.getToken() );
    }

    @Bean
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager() );
    }

    @Bean
    public PermissionsRequestsQueryService permissionsRequestsQueryService() {
        return new PermissionsRequestsQueryService( session );
    }

    @Bean
    public PermissionsRequestsManager permissionsRequestsManager() {
        return new HazelcastPermissionsRequestsService(
                hazelcastInstance,
                permissionsRequestsQueryService(),
                authorizationManager() );
    }

    @Bean
    public RequestQueryService rqs() {
        return new RequestQueryService( cassandraConfiguration.getKeyspace(), session );
    }

//    @Bean
//    public HazelcastRequestsManager hazelcastRequestsManager() {
//        return new HazelcastRequestsManager( hazelcastInstance, rqs(), neuron );
//    }

    @Bean
    public AuditQueryService auditQuerySerivce() {
        return new AuditQueryService( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public HazelcastAuditLoggingService auditLoggingService() {
        return new HazelcastAuditLoggingService( hazelcastInstance, auditQuerySerivce(), eventBus );
    }

    @Bean
    public CassandraLinkingGraphsQueryService cgqs() {
        return new CassandraLinkingGraphsQueryService( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public Clusterer clusterer() {
        return new ClusteringPartitioner( cassandraConfiguration.getKeyspace(), session, cgqs(), linkingGraph() );
    }

}
