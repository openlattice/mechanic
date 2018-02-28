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

package com.openlattice.mechanic.benchmark;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ReadBench {
//    private static Logger logger = LoggerFactory.getLogger( ReadBench.class );
//    private final HazelcastEntityDatastore ceds;
//    private final DatasourceManager        dm;
//    private final Session                  session;
//    private final PreparedStatement        readEntityKeysForEntitySetQuery;
//    private final PreparedStatement        readAll;
//
//    public ReadBench( Session session, HazelcastEntityDatastore ceds, DatasourceManager dm ) {
//        this.session = session;
//        this.ceds = ceds;
//        this.dm = dm;
//        this.readEntityKeysForEntitySetQuery = DataMapstore.prepareReadEntityKeysForEntitySetQuery( session );
//        this.readAll = session.prepare( "select * from data where entity_set_id = ? and syncid = ? allow filtering" );
//    }
//
//    public void benchmark() {
//        Stopwatch w = Stopwatch.createStarted();
//        long total = StreamUtil.stream( session.execute( DataMapstore.currentSyncs() ) )
//                //.limit(1);
//                .parallel()
//                .map( this::getEntityKeys )
//                .map( ResultSetFuture::getUninterruptibly )
//                .flatMap( StreamUtil::stream )
//                .parallel()
//                .map( RowAdapters::entityKeyFromData )
//                .unordered()
//                .distinct()
//                .count();
//        long elapsed = w.elapsed( TimeUnit.MILLISECONDS );
//        logger.info( "Read {} rows in {} ms", total, elapsed );
//    }
//
//    public ResultSetFuture getAllEntityKeys(Row row) {
//        final UUID entitySetId = RowAdapters.entitySetId( row );
//        final UUID syncId = RowAdapters.currentSyncId( row );
//        return session.executeAsync( readAll.bind()
//                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
//                .setUUID( SYNCID.cql(), syncId ) );
//    }
//
//    public ResultSetFuture getEntityKeys( Row row ) {
//        final UUID entitySetId = RowAdapters.entitySetId( row );
//        final UUID syncId = RowAdapters.currentSyncId( row );
//        return session.executeAsync( readEntityKeysForEntitySetQuery.bind()
//                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
//                .setUUID( SYNCID.cql(), syncId ) );
//    }

}
