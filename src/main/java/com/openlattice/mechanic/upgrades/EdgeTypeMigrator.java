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
 */

package com.openlattice.mechanic.upgrades;

import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class EdgeTypeMigrator {
    private final Session           session;
    private final Statement         readEntityTypes;
    private final PreparedStatement writeEntityTypes;

    public EdgeTypeMigrator(
            Session session,
            String keyspace ) {
        this.session = session;
        this.readEntityTypes =
                QueryBuilder.select( "id", "namespace", "name", "category" )
                        .from( keyspace, "entity_types" )
                        .allowFiltering()
                        .where(
                                QueryBuilder.eq( CommonColumns.CATEGORY.cql(), "EdgeType" ) );
        this.writeEntityTypes = session
                .prepare( QueryBuilder.update( keyspace, "entity_types" )
                        .where( CommonColumns.ID.eq() )
                        .and( CommonColumns.NAMESPACE.eq() )
                        .and( CommonColumns.NAME.eq() )
                        .with( QueryBuilder
                                .set( CommonColumns.CATEGORY.cql(), "AssociationType" ) ) );
    }

    public long upgrade() {
        return StreamUtil.stream( session.execute( readEntityTypes ) )
                .parallel()
                .map( row -> session.executeAsync( writeEntityTypes.bind()
                        .setUUID( CommonColumns.ID.cql(), RowAdapters.id( row ) )
                        .setString( CommonColumns.NAMESPACE.cql(), RowAdapters.namespace( row ) )
                        .setString( CommonColumns.NAME.cql(), RowAdapters.name( row ) ) ) )
                .map( ResultSetFuture::getUninterruptibly )
                .count();
    }
}
