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

import com.dataloom.mappers.ObjectMappers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.data.mapstores.PostgresDataMapstore;
import com.openlattice.datastore.cassandra.CassandraSerDesFactory;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.PropertyTypeMapstore;
import com.openlattice.postgres.mapstores.data.DataMapstoreProxy;
import com.openlattice.postgres.mapstores.data.EntityDataMapstore;
import com.openlattice.postgres.mapstores.data.PropertyDataMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ExpandDataTables {
    private static final Logger logger = LoggerFactory.getLogger( ExpandDataTables.class );
    private final PostgresDataMapstore dataMapstore;
    private final DataMapstoreProxy    dmProxy;
    private final PostgresEdmManager   pgEdmManager;
    private final PropertyTypeMapstore ptm;
    private final EntityTypeMapstore   etm;
    private final EntitySetMapstore    esm;

    public ExpandDataTables(
            PostgresDataMapstore dataMapstore,
            DataMapstoreProxy dmProxy,
            PostgresEdmManager pgEdmManager,
            PropertyTypeMapstore ptm, EntityTypeMapstore etm, EntitySetMapstore esm ) {
        this.dataMapstore = dataMapstore;
        this.dmProxy = dmProxy;
        this.pgEdmManager = pgEdmManager;
        this.ptm = ptm;
        this.etm = etm;
        this.esm = esm;
    }

    public void migrateEdm() {
        logger.info( "Starting creation of edm tables." );
        for ( EntitySet es : esm.loadAll( Lists.newArrayList( esm.loadAllKeys() ) ).values() ) {
            logger.info( "Starting table creation for entity set: ", es.getName() );
            EntityType et = etm.load( es.getEntityTypeId() );
            final Collection<PropertyType> propertyTypes = ptm.loadAll( et.getProperties() ).values();
            try {
                logger.info( "Deleting entity set tables for entity set {}.", es.getName() );
                pgEdmManager.deleteEntitySet( es, propertyTypes );
                logger.info( "Creating entity set tables for entity set {}.", es.getName() );
                pgEdmManager.createEntitySet( es, propertyTypes );
            } catch ( SQLException e ) {
                logger.error( "Failed to create tables for entity set {}.", es, e );
            }
            logger.info( "Finished with table creation for entity set: {}", es.getName() );
        }
        logger.info("Finished creation of edm tables");
    }

    public void migrate() {
        migrateEdm();
        PropertyMetadata pm = PropertyMetadata.newPropertyMetadata( OffsetDateTime.now() );
        logger.info( "Starting migration of data keys." );
        long count = 0;
        for ( DataKey dataKey : dataMapstore.loadAllKeys() ) {
            if ( ( ( ++count ) % 10000 ) == 0 ) {
                logger.info( "Migrated {} keys.", count );
            }
            EntityDataMapstore entityDataMapstore = dmProxy.getMapstore( dataKey.getEntitySetId() );
            PropertyDataMapstore pdm = dmProxy
                    .getPropertyMapstore( dataKey.getEntitySetId(), dataKey.getPropertyTypeId() );
            UUID entityKeyId = dataKey.getId();
            entityDataMapstore.store( entityKeyId, EntityDataMetadata.newEntityDataMetadata( OffsetDateTime.now() ) );
            ByteBuffer buffer = dataMapstore.load( dataKey );
            Object obj = CassandraSerDesFactory.deserializeValue( ObjectMappers.getJsonMapper(),
                    buffer,
                    ptm.load( dataKey.getPropertyTypeId() ).getDatatype(),
                    dataKey.getEntityId() );

            pdm.store( entityKeyId, ImmutableMap.of( obj, pm ) );
        }
        logger.info( "Finish migration of data keys." );
    }
}
