/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.dataloom.mechanic.upgrades;

import com.dataloom.authorization.AceKey;
import com.dataloom.data.mapstores.EntityKeyIdsMapstore;
import com.dataloom.data.mapstores.EntityKeysMapstore;
import com.dataloom.data.mapstores.PostgresEntityKeyIdsMapstore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.set.EntitySetPropertyKey;
import com.dataloom.edm.set.EntitySetPropertyMetadata;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.openlattice.postgres.mapstores.AclKeysMapstore;
import com.openlattice.postgres.mapstores.AssociationTypeMapstore;
import com.openlattice.postgres.mapstores.EdmVersionsMapstore;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntitySetPropertyMetadataMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.LinkedEntitySetsMapstore;
import com.openlattice.postgres.mapstores.LinkingVerticesMapstore;
import com.openlattice.postgres.mapstores.NamesMapstore;
import com.openlattice.postgres.mapstores.PropertyTypeMapstore;
import com.openlattice.postgres.mapstores.SchemasMapstore;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class CassandraToPostgres {
    private static final Logger logger = LoggerFactory.getLogger( CassandraToPostgres.class );

    @Inject private ListeningExecutorService executorService;
    @Inject private MapstoresPod           mp;
    @Inject private HikariDataSource       hds;
    @Inject private Session                session;
    @Inject
    private         CassandraConfiguration cassandraConfiguration;

    public int migratePropertyTypes() {
        PropertyTypeMapstore ptm = new PropertyTypeMapstore( HazelcastMap.PROPERTY_TYPES.name(),
                PostgresTable.PROPERTY_TYPES,
                hds );
        SelfRegisteringMapStore<UUID, PropertyType> cptm = mp.propertyTypeMapstore();
        int count = 0;
        Stopwatch w = Stopwatch.createStarted();
        for ( UUID id : cptm.loadAllKeys() ) {
            logger.info( "Migrating property type: {}", id );
            ptm.store( id, cptm.load( id ) );
            count++;
        }
        logger.info( "Migrated {} property types in {} ms", count, w.elapsed( TimeUnit.MILLISECONDS ) );
        return count;
    }

    public int migratePermissions() throws SQLException {
        Connection conn = hds.getConnection();
        conn.createStatement().execute( PostgresTable.PERMISSIONS.createTableQuery() );
        conn.close();

        PermissionMapstore ptm = new PermissionMapstore( hds );
        SelfRegisteringMapStore<AceKey, AceValue> cptm = mp.permissionMapstore();
        int count = 0;
        Stopwatch w = Stopwatch.createStarted();
        for ( AceKey id : cptm.loadAllKeys() ) {
            logger.info( "Migrating property type: {}", id );
            ptm.store( id, cptm.load( id ) );
            count++;
        }
        logger.info( "Migrated {} permissions in {} ms", count, w.elapsed( TimeUnit.MILLISECONDS ) );
        return count;
    }

    public <K, V> int simpleMigrate(
            SelfRegisteringMapStore<K, V> cMap,
            AbstractBasePostgresMapstore<K, V> pMap,
            PostgresTableDefinition table ) throws SQLException {
        Connection conn = hds.getConnection();
        conn.createStatement().execute( table.createTableQuery() );
        conn.close();

        int count = 0;
        Stopwatch w = Stopwatch.createStarted();
        List<ListenableFuture<?>> futures = new ArrayList<>(900000);
        for ( K key : cMap.loadAllKeys() ) {
            futures.add( executorService.submit( () -> pMap.store( key, cMap.load( key ) ) ) );
            count++;
        }
        futures.forEach( StreamUtil::getUninterruptibly );
        logger.info( "{}: Migrated {} values in {} ms", pMap.getMapName(), count, w.elapsed( TimeUnit.MILLISECONDS ) );
        return count;
    }

    public int migrateEntityTypes() throws SQLException {
        EntityTypeMapstore pMap = new EntityTypeMapstore( hds );
        SelfRegisteringMapStore<UUID, EntityType> cMap = mp.entityTypeMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.ENTITY_TYPES );
    }

    public int migrateEntitySets() throws SQLException {
        EntitySetMapstore pMap = new EntitySetMapstore( hds );
        SelfRegisteringMapStore<UUID, EntitySet> cMap = mp.entitySetMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.ENTITY_SETS );
    }

    public int migrateSchemas() throws SQLException {
        SchemasMapstore pMap = new SchemasMapstore( hds );
        SelfRegisteringMapStore<String, DelegatedStringSet> cMap = mp.schemaMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.SCHEMA );
    }

    public int migrateAclKeys() throws SQLException {
        AclKeysMapstore pMap = new AclKeysMapstore( hds );
        SelfRegisteringMapStore<String, UUID> cMap = mp.aclKeysMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.ACL_KEYS );
    }

    public int migrateNames() throws SQLException {
        NamesMapstore pMap = new NamesMapstore( hds );
        SelfRegisteringMapStore<UUID, String> cMap = mp.namesMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.NAMES );
    }

    public int migrateLinkedEntitySets() throws SQLException {
        LinkedEntitySetsMapstore pMap = new LinkedEntitySetsMapstore( hds );
        SelfRegisteringMapStore<UUID, DelegatedUUIDSet> cMap = mp.linkedEntitySetsMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.LINKED_ENTITY_SETS );
    }

    public int migratelinkingVertices() throws SQLException {
        LinkingVerticesMapstore pMap = new LinkingVerticesMapstore( hds );
        SelfRegisteringMapStore<LinkingVertexKey, LinkingVertex> cMap = mp.linkingVerticesMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.LINKING_VERTICES );
    }

    public int migrateAssociationTypes() throws SQLException {
        AssociationTypeMapstore pMap = new AssociationTypeMapstore( hds );
        SelfRegisteringMapStore<UUID, AssociationType> cMap = mp.edgeTypeMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.ASSOCIATION_TYPES );
    }

    public int migrateSyncIds() throws SQLException {
        PostgresTableDefinition postgresTable = PostgresTable.SYNC_IDS;
        List<PostgresColumnDefinition> postgresColumns = ImmutableList.of(
                PostgresColumn.ENTITY_SET_ID,
                PostgresColumn.SYNC_ID,
                PostgresColumn.CURRENT_SYNC_ID );
        Table cassandraTable = Table.SYNC_IDS;
        try (
                Connection conn = hds.getConnection();
                Statement createStmt = conn.createStatement();
                PreparedStatement ps = conn
                        .prepareStatement( postgresTable.insertQuery( Optional.empty(), postgresColumns ) )
        ) {

            createStmt.execute( postgresTable.createTableQuery() );

            com.datastax.driver.core.ResultSet rs = session
                    .execute( QueryBuilder.select().all()
                            .from( cassandraConfiguration.getKeyspace(), cassandraTable.name() ) );
            int count = 0;
            for ( Row row : rs ) {
                UUID entitySetId = RowAdapters.entitySetId( row );
                UUID syncId = RowAdapters.syncId( row );
                UUID currentSyncId = RowAdapters.currentSyncId( row );

                ps.setObject( 1, entitySetId );
                ps.setObject( 2, syncId );
                ps.setObject( 3, currentSyncId );
                ps.execute();
                count++;
            }
            ps.close();
            conn.close();

            return count;
        } catch ( SQLException e ) {
            logger.error( "Unable to migrate sync ids", e );
            return 0;
        }
    }

    public int migrateOrganizations() throws SQLException {
        // TODO test this

        PostgresTableDefinition postgresTable = PostgresTable.ORGANIZATIONS;
        Table cassandraTable = Table.ORGANIZATIONS;
        List<PostgresColumnDefinition> postgresColumns = ImmutableList.of(
                PostgresColumn.ID,
                PostgresColumn.NULLABLE_TITLE,
                PostgresColumn.DESCRIPTION,
                PostgresColumn.ALLOWED_EMAIL_DOMAINS,
                PostgresColumn.MEMBERS );
        try (
                Connection conn = hds.getConnection();
                Statement createStmt = conn.createStatement();
                PreparedStatement ps = conn
                        .prepareStatement( postgresTable.insertQuery( Optional.empty(), postgresColumns ) )
        ) {

            createStmt.execute( postgresTable.createTableQuery() );

            com.datastax.driver.core.ResultSet rs = session
                    .execute( QueryBuilder.select().all()
                            .from( cassandraConfiguration.getKeyspace(), cassandraTable.name() ) );
            int count = 0;
            for ( Row row : rs ) {
                UUID id = RowAdapters.id( row );
                String title = RowAdapters.title( row );
                String description = row.getString( CommonColumns.DESCRIPTION.cql() );
                Set<String> emails = row.getSet( CommonColumns.ALLOWED_EMAIL_DOMAINS.cql(), String.class );
                LinkedHashSet<String> members = RowAdapters.members( row );

                Array emailsArr = PostgresArrays.createTextArray( ps.getConnection(), emails.stream() );
                Array membersArr = PostgresArrays.createTextArray( ps.getConnection(), members.stream() );
                ps.setObject( 1, id );
                ps.setString( 2, title );
                ps.setString( 3, description );
                ps.setArray( 4, emailsArr );
                ps.setArray( 5, membersArr );

                ps.execute();
                count++;
            }
            ps.close();
            conn.close();

            return count;
        } catch ( SQLException e ) {
            logger.error( "Unable to migrate organizations", e );
            return 0;
        }
    }

    public int migrateEntitySetPropertyMetadata() throws SQLException {
        EntitySetPropertyMetadataMapstore pMap = new EntitySetPropertyMetadataMapstore( hds );
        SelfRegisteringMapStore<EntitySetPropertyKey, EntitySetPropertyMetadata> cMap = mp
                .entitySetPropertyMetadataMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.ENTITY_SET_PROPERTY_METADATA );
    }

    public int migrateEdmVersionsMapstore() throws SQLException {
        EdmVersionsMapstore pMap = new EdmVersionsMapstore( hds );
        SelfRegisteringMapStore<String, UUID> cMap = mp.edmVersionMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.EDM_VERSIONS );
    }

    public int migrateEntityKeyIds() throws SQLException {
        EntityKeysMapstore kcMap = new EntityKeysMapstore( HazelcastMap.KEYS.name(), session, Table.KEYS.getBuilder() );
        EntityKeyIdsMapstore cMap = new EntityKeyIdsMapstore( kcMap,
                HazelcastMap.IDS.name(),
                session,
                Table.IDS.getBuilder() );

        PostgresEntityKeyIdsMapstore ekIds = new PostgresEntityKeyIdsMapstore( hds );
        return simpleMigrate( cMap, ekIds, PostgresTable.IDS );
    }
}
