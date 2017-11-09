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
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.securable.SecurableObjectType;
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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.postgres.*;
import com.openlattice.postgres.mapstores.*;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class CassandraToPostgres {
    private static final Logger logger = LoggerFactory.getLogger( CassandraToPostgres.class );

    @Inject private MapstoresPod     mp;
    @Inject private HikariDataSource hds;
    @Inject private Session          session;

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
        SelfRegisteringMapStore<AceKey, DelegatedPermissionEnumSet> cptm = mp.permissionMapstore();
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
        for ( K key : cMap.loadAllKeys() ) {
            pMap.store( key, cMap.load( key ) );
            count++;
        }
        logger.info( "{}: Migrated {} values in {} ms", pMap.getMapName(), count, w.elapsed( TimeUnit.MILLISECONDS ) );
        return count;
    }

    public int migrateSecurableObjectTypes() throws SQLException {
        SecurableObjectTypeMapstore pMap = new SecurableObjectTypeMapstore( hds );
        SelfRegisteringMapStore<List<UUID>, SecurableObjectType> cMap = mp.securableObjectTypeMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.SECURABLE_OBJECTS );
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

    public int migrateRoles() throws SQLException {
        // TODO test this

        PostgresTableDefinition postgresTable = PostgresTable.ROLES;
        List<PostgresColumnDefinition> postgresColumns = ImmutableList.of(
                PostgresColumn.ID,
                PostgresColumn.ORGANIZATION_ID,
                PostgresColumn.TITLE,
                PostgresColumn.DESCRIPTION,
                PostgresColumn.PRINCIPAL_IDS );
        Table cassandraTable = Table.ROLES;
        try (
                Connection conn = hds.getConnection();
                Statement createStmt = conn.createStatement();
                PreparedStatement ps = conn
                        .prepareStatement( postgresTable.insertQuery( Optional.empty(), postgresColumns ) )
        ) {

            createStmt.execute( postgresTable.createTableQuery() );

            com.datastax.driver.core.ResultSet rs = session
                    .execute( QueryBuilder.select().all().from( cassandraTable.name() ) );
            int count = 0;
            for ( Row row : rs ) {
                UUID id = RowAdapters.id( row );
                UUID organizationId = RowAdapters.organizationId( row );
                String title = RowAdapters.title( row );
                String description = row.getString( CommonColumns.DESCRIPTION.cql() );
                Set<String> users = row.getSet( CommonColumns.PRINCIPAL_IDS.cql(), String.class );

                Array usersArr = PostgresArrays.createTextArray( ps.getConnection(), users.stream() );
                ps.setObject( 1, id );
                ps.setObject( 2, organizationId );
                ps.setString( 3, title );
                ps.setString( 4, description );
                ps.setArray( 5, usersArr );

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

    public int migrateSyncIds() throws SQLException {
        // TODO test this

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
                    .execute( QueryBuilder.select().all().from( cassandraTable.name() ) );
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
                    .execute( QueryBuilder.select().all().from( cassandraTable.name() ) );
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
                ps.setArray( 4, membersArr );

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

    public int migrateVertexIdsAfterLinking() throws SQLException {
        VertexIdsAfterLinkingMapstore pMap = new VertexIdsAfterLinkingMapstore( hds );
        SelfRegisteringMapStore<LinkingVertexKey, UUID> cMap = mp.vertexIdsAfterLinkingMapstore();
        return simpleMigrate( cMap, pMap, PostgresTable.VERTEX_IDS_AFTER_LINKING );
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
}
