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
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.google.common.base.Stopwatch;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.mapstores.PropertyTypeMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
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

    @Inject MapstoresPod     mp;
    @Inject HikariDataSource hds;

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

        PermissionMapstore ptm = new PermissionMapstore( HazelcastMap.PERMISSIONS.name(),
                PostgresTable.PERMISSIONS,
                hds );
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

}
