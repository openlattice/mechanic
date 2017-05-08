package com.dataloom.mechanic.upgrades;

import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

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
