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

package com.openlattice.mechanic.upgrades;

import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.data.mapstores.EntityKeyIdsMapstore;
import com.openlattice.data.mapstores.EntityKeysMapstore;
import com.openlattice.data.mapstores.PostgresEntityKeyIdsMapstore;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.openlattice.edm.mapstores.EdmVersionMapstore;
import com.openlattice.edm.schemas.mapstores.SchemaMapstore;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.linking.mapstores.LinkedEntityTypesMapstore;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.openlattice.postgres.mapstores.AclKeysMapstore;
import com.openlattice.postgres.mapstores.AssociationTypeMapstore;
import com.openlattice.postgres.mapstores.EdmVersionsMapstore;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntitySetPropertyMetadataMapstore;
import com.openlattice.postgres.mapstores.LinkedEntitySetsMapstore;
import com.openlattice.postgres.mapstores.LinkingVerticesMapstore;
import com.openlattice.postgres.mapstores.NamesMapstore;
import com.openlattice.postgres.mapstores.SchemasMapstore;
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore;
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
    @Inject private MapstoresPod             mp;
    @Inject private HikariDataSource         hds;
    @Inject private Session                  session;
    @Inject
    private         CassandraConfiguration   cassandraConfiguration;
    @Inject private PostgresTableManager     ptm;

    public int migratePropertyTypes() throws SQLException {
        com.openlattice.postgres.mapstores.PropertyTypeMapstore ptm = new com.openlattice.postgres.mapstores.PropertyTypeMapstore(
                hds );
        logger.info( PostgresTable.IDS.createTableQuery() );
        //        logger.info( PostgresTable.ENTITY_TYPES.createTableQuery() );
        com.openlattice.edm.mapstores.PropertyTypeMapstore cptm = new com.openlattice.edm.mapstores.PropertyTypeMapstore(
                session );
        return simpleMigrate( cptm, ptm, PostgresTable.PROPERTY_TYPES );
        //        int count = 0;
        //        Stopwatch w = Stopwatch.createStarted();
        //        for ( UUID id : cptm.loadAllKeys() ) {
        //            logger.info( "Migrating property type: {}", id );
        //            ptm.store( id, cptm.load( id ) );
        //            count++;
        //        }
        //        logger.info( "Migrated {} property types in {} ms", count, w.elapsed( TimeUnit.MILLISECONDS ) );
        //        return count;
    }


    public <K, V> int simpleMigrate(
            SelfRegisteringMapStore<K, V> cMap,
            AbstractBasePostgresMapstore<K, V> pMap,
            PostgresTableDefinition table ) throws SQLException {
        //Create distributed table first
        try ( Connection conn = hds.getConnection(); Statement st = conn.createStatement() ) {
            st.execute( table.createTableQuery() + distributeOn( table ) );

            //Will create indexes
            ptm.registerTables( table );

            int count = 0;
            Stopwatch w = Stopwatch.createStarted();
            List<ListenableFuture<?>> futures = new ArrayList<>( 1 << 24 );
            for ( K key : cMap.loadAllKeys() ) {
                futures.add( executorService.submit( () -> pMap.store( key, cMap.load( key ) ) ) );
                count++;
            }
            futures.forEach( StreamUtil::getUninterruptibly );
            logger.info( "{}: Migrated {} values in {} ms",
                    pMap.getMapName(),
                    count,
                    w.elapsed( TimeUnit.MILLISECONDS ) );
            return count;
        } catch ( SQLException ex ) {
            logger.error( "Failed migrating {}", table, ex );
            return 0;
        }
    }

    public int migrateSecurableObjectTypes() {
        ResultSet rs = session.execute( "select distinct acl_keys, securable_object_type from sparks.permissions;" );
        SecurableObjectTypeMapstore sotm = new SecurableObjectTypeMapstore( hds );
        int count = 0;
        for ( Row row : rs ) {
            AclKey aclKey = new AclKey( row.getList( "acl_keys", UUID.class ) );
            SecurableObjectType objectType = RowAdapters.securableObjectType( row );
            sotm.store( aclKey, objectType );
            count++;
        }
        return count;
    }

    public int migrateEntityTypes() throws SQLException {
        com.openlattice.edm.mapstores.EntityTypeMapstore cMap = new com.openlattice.edm.mapstores.EntityTypeMapstore(
                session );
        com.openlattice.postgres.mapstores.EntityTypeMapstore pMap = new com.openlattice.postgres.mapstores.EntityTypeMapstore(
                hds );
        return simpleMigrate( cMap, pMap, PostgresTable.ENTITY_TYPES );
    }

    public int migrateEntitySets() throws SQLException {
        EntitySetMapstore pMap = new EntitySetMapstore( hds );
        com.openlattice.edm.mapstores.EntitySetMapstore cMap = new com.openlattice.edm.mapstores.EntitySetMapstore(
                session );
        return simpleMigrate( cMap, pMap, PostgresTable.ENTITY_SETS );
    }

    public int migrateSchemas() throws SQLException {
        SchemasMapstore pMap = new SchemasMapstore( hds );
        SchemaMapstore cMap = new SchemaMapstore( session );
        return simpleMigrate( cMap, pMap, PostgresTable.SCHEMA );
    }

    public int migrateAclKeys() throws SQLException {
        AclKeysMapstore pMap = new AclKeysMapstore( hds );
        com.openlattice.edm.mapstores.AclKeysMapstore cMap = new com.openlattice.edm.mapstores.AclKeysMapstore( session );
        return simpleMigrate( cMap, pMap, PostgresTable.ACL_KEYS );
    }

    public int migrateNames() throws SQLException {
        NamesMapstore pMap = new NamesMapstore( hds );
        com.openlattice.edm.mapstores.NamesMapstore cMap = new com.openlattice.edm.mapstores.NamesMapstore( session );
        return simpleMigrate( cMap, pMap, PostgresTable.NAMES );
    }

    public int migrateLinkedEntitySets() throws SQLException {
        LinkedEntitySetsMapstore pMap = new LinkedEntitySetsMapstore( hds );
        com.openlattice.linking.mapstores.LinkedEntitySetsMapstore cMap = new com.openlattice.linking.mapstores.LinkedEntitySetsMapstore(
                session );
        return simpleMigrate( cMap, pMap, PostgresTable.LINKED_ENTITY_SETS );
    }

    public int migratelinkingVertices() throws SQLException {
        LinkingVerticesMapstore pMap = new LinkingVerticesMapstore( hds );
        com.openlattice.linking.mapstores.LinkingVerticesMapstore cMap = new com.openlattice.linking.mapstores.LinkingVerticesMapstore(
                session );
        return simpleMigrate( cMap, pMap, PostgresTable.LINKING_VERTICES );
    }

    public int migrateAssociationTypes() throws SQLException {
        AssociationTypeMapstore pMap = new AssociationTypeMapstore( hds );
        com.openlattice.edm.mapstores.AssociationTypeMapstore cMap = new com.openlattice.edm.mapstores.AssociationTypeMapstore(
                session );
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
        com.openlattice.edm.mapstores.EntitySetPropertyMetadataMapstore cMap = new com.openlattice.edm.mapstores.EntitySetPropertyMetadataMapstore(
                session );
        return simpleMigrate( cMap, pMap, PostgresTable.ENTITY_SET_PROPERTY_METADATA );
    }

    public int migrateEdmVersionsMapstore() throws SQLException {
        EdmVersionsMapstore pMap = new EdmVersionsMapstore( hds );
        EdmVersionMapstore cMap = new EdmVersionMapstore( session );
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

    public int migrateLinkedEntityTypes() throws SQLException {
        SelfRegisteringMapStore<UUID, DelegatedUUIDSet> let = new LinkedEntityTypesMapstore( session );
        com.openlattice.postgres.mapstores.LinkedEntityTypesMapstore pgMap =
                new com.openlattice.postgres.mapstores.LinkedEntityTypesMapstore( hds );
        return simpleMigrate( let, pgMap, PostgresTable.LINKED_ENTITY_TYPES );

    }

    public static String distributeOn( PostgresTableDefinition ptd ) {
        for ( PostgresColumnDefinition pcd : PostgresTable.HASH_ON ) {
            if ( ptd.getPrimaryKey().contains( pcd ) ) {
                return " distribute by HASH(" + pcd.getName() + ")";
            } else if ( ptd.getColumns().contains( pcd ) ) {
                logger.warn( "Distributing on non-primary key column {} for table {}", pcd, ptd );
                return " distribute by HASH(" + pcd.getName() + ")";
            } else {
                logger.warn( "Unable to find distribution column for table {}.", ptd );
                return "";
            }
        }
        return "";
    }
}
