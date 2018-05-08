/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.mechanic.upgrades;

import static com.openlattice.data.PropertyMetadata.hashObject;

import com.dataloom.mappers.ObjectMappers;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.data.mapstores.PostgresDataMapstore;
import com.openlattice.datastore.cassandra.CassandraSerDesFactory;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.PropertyTypeMapstore;
import com.openlattice.postgres.mapstores.data.DataMapstoreProxy;
import com.openlattice.postgres.mapstores.data.EntityDataMapstore;
import com.openlattice.postgres.mapstores.data.PropertyDataMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ExpandDataTables {
    private static final Logger logger                        = LoggerFactory.getLogger( ExpandDataTables.class );
    private static final UUID   complaintNumberPropertyTypeId = UUID
            .fromString( "998bc748-4f40-4f5d-98ed-91ac4dab28a1" );

    private final Map<UUID, PropertyType>  propertTypes;
    private final PostgresDataMapstore     dataMapstore;
    private final DataMapstoreProxy        dmProxy;
    private final PostgresEdmManager       pgEdmManager;
    private final PropertyTypeMapstore     ptm;
    private final EntityTypeMapstore       etm;
    private final EntitySetMapstore        esm;
    private final HikariDataSource         hds;
    private final ListeningExecutorService executorService;

    public ExpandDataTables(
            HikariDataSource hds,
            PostgresDataMapstore dataMapstore,
            DataMapstoreProxy dmProxy,
            PostgresEdmManager pgEdmManager,
            PropertyTypeMapstore ptm,
            EntityTypeMapstore etm,
            EntitySetMapstore esm,
            ListeningExecutorService executorService ) {
        this.hds = hds;
        this.dataMapstore = dataMapstore;
        this.dmProxy = dmProxy;
        this.pgEdmManager = pgEdmManager;
        this.ptm = ptm;
        this.etm = etm;
        this.esm = esm;
        this.propertTypes = ptm.loadAll( ImmutableSet.copyOf( ptm.loadAllKeys() ) );
        this.executorService = executorService;
    }

    public void migrateEdm() {
        logger.info( "Starting creation of edm tables." );
        final Collection<PropertyType> propertyTypes = propertTypes.values();
        propertyTypes.stream()
                .map( PropertyType::getId )
                .map( DataTables::propertyTableName )
                .map( DataTables::quote )
                .peek( id -> logger.info( "Deleting table {}", id ) )
                .forEach( pgEdmManager::dropTable );

        propertyTypes.stream()
                .map( PropertyType::getId )
                .peek( table -> logger.info( "Creating table for property type id {}", table ) )
                .forEach( dmProxy::getPropertyMapstore );

        for ( EntitySet es : esm.loadAll( Lists.newArrayList( esm.loadAllKeys() ) ).values() ) {
            logger.info( "Starting table creation for entity set: ", es.getName() );
            try {
                logger.info( "Deleting entity set tables for entity set {}.", es.getName() );
                pgEdmManager.deleteEntitySet( es, ImmutableSet.of() );
                logger.info( "Creating entity set tables for entity set {}.", es.getName() );
                pgEdmManager.createEntitySet( es, ImmutableSet.of() );
            } catch ( SQLException e ) {
                logger.error( "Failed to create tables for entity set {}.", es, e );
            }
            logger.info( "Finished with table creation for entity set: {}", es.getName() );
        }
        logger.info( "Finished creation of edm tables" );
    }

    public void migrate() throws InterruptedException {
        migrateEdm();
        logger.info( "Starting migration of data keys." );
        final AtomicLong migratedCount = new AtomicLong( 0 );
        final List<OldPostgresData> oldData = new ArrayList<>( 24000000 );
        final List<UUID> syncIds = new ArrayList<>( 400 );

        final String syncIdQuery = "SELECT DISTINCT current_sync_id FROM sync_ids";

        try ( final Connection connection = hds.getConnection();
                final Statement stmt = connection.createStatement();
                final ResultSet rs = stmt.executeQuery( syncIdQuery ) ) {
            while ( rs.next() ) {
                syncIds.add( (UUID) rs.getObject( "current_sync_id" ) );
            }
        } catch ( SQLException e ) {
            logger.error( "Unable to query for sync ids", e );
        }

        final String sqlQuery = "select * from data where syncid = ?";

        logger.info( "Starting load of data into memory." );

        Stopwatch w = Stopwatch.createStarted();

        syncIds.parallelStream().forEach( syncId -> {
            try ( final Connection connection = hds.getConnection();
                    final PreparedStatement ps = connection.prepareStatement( sqlQuery ) ) {
                ps.setObject( 1, syncId );
                try ( ResultSet rs = ps.executeQuery() ) {
                    while ( rs.next() ) {
                        oldData.add( new OldPostgresData(
                                ResultSetAdapters.dataKey( rs ),
                                rs.getBytes( "property_buffer" ) ) );
                    }
                }
            } catch ( SQLException e ) {
                logger.error( "Unable to query for data for syncid {}", syncId );
            }
        } );

        logger.info( "Finished load of data into memory in {} ms", w.elapsed( TimeUnit.MILLISECONDS ) );

        w.reset();

        logger.info( "Starting migration of {} keys.", oldData.size() );
        w.start();
        oldData.parallelStream().forEach( oldDatum -> {
            final DataKey dataKey = oldDatum.getDataKey();

            if ( ( migratedCount.incrementAndGet() % 10000 ) == 0 ) {
                logger.info( "Migrated {} keys.", migratedCount );
            }
            final EntityDataMapstore entityDataMapstore = dmProxy
                    .getMapstore( dataKey.getEntitySetId() );
            final PropertyDataMapstore pdm = dmProxy
                    .getPropertyMapstore( dataKey.getPropertyTypeId() );
            final UUID entitySetId = dataKey.getEntitySetId();
            final UUID entityKeyId = dataKey.getId();
            entityDataMapstore
                    .store( entityKeyId,
                            EntityDataMetadata.newEntityDataMetadata( OffsetDateTime.now() ) );
            ByteBuffer buffer = ByteBuffer.wrap( oldDatum.getData() );
            final Object obj;
            try {
                if ( dataKey.getPropertyTypeId().equals( complaintNumberPropertyTypeId )
                        && buffer.array().length == 8 ) {
                    logger.info(
                            "Detected complaint number that is a long-- switching to alternate deserialization" );
                    obj = CassandraSerDesFactory.deserializeValue( ObjectMappers.getJsonMapper(),
                            buffer,
                            EdmPrimitiveTypeKind.Int64,
                            dataKey.getEntityId() ).toString();
                } else {
                    obj = CassandraSerDesFactory.deserializeValue( ObjectMappers.getJsonMapper(),
                            buffer,
                            ptm.load( dataKey.getPropertyTypeId() ).getDatatype(),
                            dataKey.getEntityId() );
                }
                if ( obj != null ) {
                    PropertyMetadata pm = PropertyMetadata
                            .newPropertyMetadata( hashObject( obj ), OffsetDateTime.now() );
                    pdm.store( new EntityDataKey( entitySetId, entityKeyId ), ImmutableMap.of( obj, pm ) );
                }
            } catch ( Exception edp ) {
                logger.error(
                        "Unable to process entity with id {} in entity set {} with property type {} and value {}",
                        entityKeyId,
                        entitySetId,
                        dataKey.getPropertyTypeId(),
                        new String( buffer.array(), Charsets.UTF_8 ),
                        edp );
            }
        } );
        logger.info( "Successfully migrated {} keys in {} seconds.",
                migratedCount.get(),
                w.elapsed( TimeUnit.SECONDS ) );
    }

    static class OldPostgresData {
        private final DataKey dataKey;
        private final byte[]  data;

        OldPostgresData( DataKey dataKey, byte[] data ) {
            this.dataKey = dataKey;
            this.data = data;
        }

        public DataKey getDataKey() {
            return dataKey;
        }

        public byte[] getData() {
            return data;
        }
    }
}
