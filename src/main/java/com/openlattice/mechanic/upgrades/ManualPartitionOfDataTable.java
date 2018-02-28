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
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ManualPartitionOfDataTable {
    private static final Logger         logger = LoggerFactory.getLogger( ManualPartitionOfDataTable.class );
    private static final Set<WriteInfo> data   = Sets.newConcurrentHashSet();
    private static final HashFunction   hf     = Hashing.murmur3_128();

    private static final LoadingCache<UUID, AtomicInteger> PARTTIONS = CacheBuilder.newBuilder()
            .expireAfterAccess( 1, TimeUnit.MINUTES ).build( new CacheLoader<UUID, AtomicInteger>() {
                @Override public AtomicInteger load( UUID key ) throws Exception {
                    return new AtomicInteger();
                }
            } );

    private static final AtomicLong counter           = new AtomicLong();
    public static        byte[]     PARTITION_INDEXES = new byte[ 256 ];

    static {
        for ( int i = 0; i < PARTITION_INDEXES.length; ++i ) {
            PARTITION_INDEXES[ i ] = (byte) i;
        }
    }

    private final Session           session;
    private final String            keyspace;
    private final Select            readCurrentDataTableRow;
    private       PreparedStatement writeCurrentDataTableRow;

    public ManualPartitionOfDataTable(
            Session session,
            String keyspace,
            ListeningExecutorService executor ) {
        this.session = session;
        this.keyspace = keyspace;
        this.readCurrentDataTableRow = readQuery( session, keyspace );
    }

    public long migrate() {
        logger.info( "Starting ELT of data in Cassandra" );

        StreamUtil
                .stream( session.execute( readCurrentDataTableRow ) )
                .parallel()
                .map( ManualPartitionOfDataTable::fromRow )
                .peek( info -> {counter.getAndIncrement();} )
                .forEach( data::add );

        logger.info( "Completed ELT of {} records from Cassandra" , counter.get() );

        session.execute( "drop table sparks.data" );

        logger.info( "Dropped data table from Cassandra" );

        session.execute( Table.DATA.getBuilder().buildCreateTableQuery() );
        writeCurrentDataTableRow = writeQuery( session, keyspace );
        logger.info( "Created new data table" );

        logger.info( "Flushing in memory data to Cassandra." );

        data
                .parallelStream()
                .map( this::bind )
                .map( session::executeAsync )
                .forEach( ResultSetFuture::getUninterruptibly );
        logger.info( "Finished flushing transformed data from memory" );
        return data.size();
    }

    private BoundStatement bind( WriteInfo info ) {
        return writeCurrentDataTableRow.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), info.getEntitySetId() )
                .setByte( CommonColumns.PARTITION_INDEX.cql(), info.getPartitionIndex() )
                .setUUID( CommonColumns.SYNCID.cql(), info.getSyncId() )
                .setString( CommonColumns.ENTITYID.cql(), info.getEntityId() )
                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), info.getPropertyTypeId() )
                .setBytes( CommonColumns.PROPERTY_BUFFER.cql(), info.getPropertyBuffer() )
                .setBytes( CommonColumns.PROPERTY_VALUE.cql(), info.getPropertyValue() );

    }

    private static Select readQuery( Session session, String keyspace ) {
        return QueryBuilder.select().all().from( keyspace, Table.DATA.getName() );
    }

    private static PreparedStatement writeQuery( Session session, String keyspace ) {
        return session.prepare( Table.DATA.getBuilder().buildStoreQuery() );
    }

    private static WriteInfo fromRow( Row row ) {
        UUID entitySetId = RowAdapters.entitySetId( row );
        UUID syncId = RowAdapters.syncId( row );
        byte partitionIndex = (byte) PARTTIONS.getUnchecked( entitySetId ).getAndIncrement();
        String entityId = RowAdapters.entityId( row );
        UUID propertyTypeId = RowAdapters.propertyTypeId( row );
        ByteBuffer propertyBuffer = row.getBytes( CommonColumns.PROPERTY_BUFFER.cql() );
        ByteBuffer propertyValue = row.getBytes( CommonColumns.PROPERTY_VALUE.cql() );

        if ( propertyBuffer == null ) {
            propertyBuffer = propertyValue;
            propertyValue = ByteBuffer.wrap( hf.hashBytes( propertyValue.array() ).asBytes() );
        }

        return new WriteInfo( entitySetId,
                syncId,
                partitionIndex,
                entityId,
                propertyTypeId,
                propertyValue,
                propertyBuffer );

    }

    private static class WriteInfo {
        private final UUID       entitySetId;
        private final UUID       syncId;
        private final byte       partitionIndex;
        private final String     entityId;
        private final UUID       propertyTypeId;
        private final ByteBuffer propertyValue;
        private final ByteBuffer propertyBuffer;

        public WriteInfo(
                UUID entitySetId,
                UUID syncId,
                byte partitionIndex,
                String entityId,
                UUID propertyTypeId,
                ByteBuffer propertyValue, ByteBuffer propertyBuffer ) {
            this.entitySetId = entitySetId;
            this.syncId = syncId;
            this.partitionIndex = partitionIndex;
            this.entityId = entityId;
            this.propertyTypeId = propertyTypeId;
            this.propertyValue = propertyValue;
            this.propertyBuffer = propertyBuffer;
        }

        public UUID getEntitySetId() {
            return entitySetId;
        }

        public UUID getSyncId() {
            return syncId;
        }

        public byte getPartitionIndex() {
            return partitionIndex;
        }

        public String getEntityId() {
            return entityId;
        }

        public UUID getPropertyTypeId() {
            return propertyTypeId;
        }

        public ByteBuffer getPropertyValue() {
            return propertyValue;
        }

        public ByteBuffer getPropertyBuffer() {
            return propertyBuffer;
        }

        @Override public boolean equals( Object o ) {
            if ( this == o ) { return true; }
            if ( !( o instanceof WriteInfo ) ) { return false; }

            WriteInfo writeInfo = (WriteInfo) o;

            if ( partitionIndex != writeInfo.partitionIndex ) { return false; }
            if ( entitySetId != null ? !entitySetId.equals( writeInfo.entitySetId ) : writeInfo.entitySetId != null ) {
                return false;
            }
            if ( syncId != null ? !syncId.equals( writeInfo.syncId ) : writeInfo.syncId != null ) { return false; }
            if ( entityId != null ? !entityId.equals( writeInfo.entityId ) : writeInfo.entityId != null ) {
                return false;
            }
            if ( propertyTypeId != null
                    ? !propertyTypeId.equals( writeInfo.propertyTypeId )
                    : writeInfo.propertyTypeId != null ) { return false; }
            return propertyValue != null
                    ? propertyValue.equals( writeInfo.propertyValue )
                    : writeInfo.propertyValue == null;
        }

        @Override public int hashCode() {
            int result = entitySetId != null ? entitySetId.hashCode() : 0;
            result = 31 * result + ( syncId != null ? syncId.hashCode() : 0 );
            result = 31 * result + (int) partitionIndex;
            result = 31 * result + ( entityId != null ? entityId.hashCode() : 0 );
            result = 31 * result + ( propertyTypeId != null ? propertyTypeId.hashCode() : 0 );
            result = 31 * result + ( propertyValue != null ? propertyValue.hashCode() : 0 );
            return result;
        }
    }
}
