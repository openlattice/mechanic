package com.dataloom.mechanic.upgrades;

import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import io.netty.util.internal.ConcurrentSet;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DataTableMigrator {
    private static final UUID SYNC_ID = new UUID( 0, 0 );
    private final Session                  session;
    private final String                   keyspace;
    private final PreparedStatement        readCurrentDataTableRow;
    private final PreparedStatement        writeCurrentDataTableRow;
    private final ListeningExecutorService executor;

    public DataTableMigrator( Session session, String keyspace, ListeningExecutorService executor ) {
        this.session = session;
        this.keyspace = keyspace;
        this.executor = executor;

        session.execute( Table.DATA.getBuilder().buildCreateTableQuery() );

        readCurrentDataTableRow = session
                .prepare( QueryBuilder.select().all().from( keyspace, "olddata" )
                        .where( CommonColumns.ENTITYID.eq() )
                        .and( CommonColumns.PROPERTY_TYPE_ID.eq() )
                        .and( CommonColumns.PROPERTY_VALUE.eq() ) );

        writeCurrentDataTableRow = session
                .prepare( QueryBuilder.insertInto( keyspace, "data" )
                        .value( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.ENTITY_SET_ID.bindMarker() )
                        .value( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() )
                        .value( CommonColumns.PROPERTY_TYPE_ID.cql(), CommonColumns.PROPERTY_TYPE_ID.bindMarker() )
                        .value( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() )
                        .value( CommonColumns.PROPERTY_VALUE.cql(), CommonColumns.PROPERTY_VALUE.bindMarker() ) );
    }

    public long upgrade() {
        StreamUtil.stream( session.execute( "select distinct id from sparks.entity_sets" ) )
                .map( RowAdapters::id )
                .forEach( id -> QueryBuilder.insertInto( keyspace, "sync_ids" )
                        .value( CommonColumns.ENTITY_SET_ID.cql(), id )
                        .value( CommonColumns.SYNCID.cql(), SYNC_ID )
                        .value( CommonColumns.CURRENT_SYNC_ID.cql(), SYNC_ID ) );

        return StreamUtil.stream( session.execute( readCurrentEntityIdLookupTableQuery() ) )
                .parallel()
                .map( this::toDataTableQuery )
                .map( ResultSetFuture::getUninterruptibly )
                .count();
    }

    private ResultSetFuture writeRowToNewDataTable( UUID entitySetId, UUID syncId, String entityId, Row r ) {
        BoundStatement bs = writeCurrentDataTableRow.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), RowAdapters.propertyTypeId( r ) )
                .setUUID( CommonColumns.SYNCID.cql(), syncId )
                .setBytes( CommonColumns.PROPERTY_VALUE.cql(), r.getBytes( CommonColumns.PROPERTY_VALUE.cql() ) );
        return session.executeAsync( bs );
    }

    private ResultSetFuture toDataTableQuery( Row r ) {
        final UUID entitySetId = RowAdapters.entitySetId( r );
        final UUID syncId = RowAdapters.syncId( r );
        final String entityId = RowAdapters.entityId( r );
        ResultSetFuture dataRow = session.executeAsync( readCurrentDataTableRow.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setUUID( CommonColumns.SYNCID.cql(), syncId )
                .setString( CommonColumns.ENTITYID.cql(), entityId ) );
        return (ResultSetFuture) Futures
                .transformAsync(
                        dataRow, rs -> writeRowToNewDataTable( entitySetId, syncId, entityId, rs.one() ),
                        executor );

    }

    private Select readCurrentEntityIdLookupTableQuery() {
        return QueryBuilder.select().all().from( keyspace, "entity_id_lookup" );
    }

}
