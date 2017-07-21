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

package com.dataloom.mechanic.benchmark;

import static com.kryptnostic.datastore.cassandra.CommonColumns.SYNCID;

import com.dataloom.data.DatasourceManager;
import com.dataloom.data.mapstores.DataMapstore;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ReadBench {
    private static Logger logger = LoggerFactory.getLogger( ReadBench.class );
    private final CassandraEntityDatastore ceds;
    private final DatasourceManager        dm;
    private final Session                  session;
    private final PreparedStatement        readEntityKeysForEntitySetQuery;

    public ReadBench( Session session, CassandraEntityDatastore ceds, DatasourceManager dm ) {
        this.session = session;
        this.ceds = ceds;
        this.dm = dm;
        this.readEntityKeysForEntitySetQuery = DataMapstore.prepareReadEntityKeysForEntitySetQuery( session );
    }

    public void benchmark() {
        Stopwatch w = Stopwatch.createStarted();
        long total = StreamUtil.stream( session.execute( DataMapstore.currentSyncs() ) )
                //.limit(1);
                .parallel()
                .map( this::getEntityKeys )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( RowAdapters::entityKeyFromData )
                .unordered()
                .distinct()
                .count();
        long elapsed = w.elapsed( TimeUnit.MILLISECONDS );
        logger.info( "Read {} rows in {} ms", total, elapsed );
    }

    public ResultSetFuture getEntityKeys( Row row ) {
        final UUID entitySetId = RowAdapters.entitySetId( row );
        final UUID syncId = RowAdapters.currentSyncId( row );
        return session.executeAsync( readEntityKeysForEntitySetQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setUUID( SYNCID.cql(), syncId ) );
    }

}
