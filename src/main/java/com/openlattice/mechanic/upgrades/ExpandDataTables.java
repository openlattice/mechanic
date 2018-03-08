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
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.data.mapstores.DataMapstore;
import com.openlattice.datastore.cassandra.CassandraSerDesFactory;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.mapstores.data.DataMapstoreProxy;
import com.openlattice.postgres.mapstores.data.EntityDataMapstore;
import com.openlattice.postgres.mapstores.data.PropertyDataMapstore;
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
    private final DataMapstore       dataMapstore;
    private final DataMapstoreProxy  dmProxy;
    private final PostgresEdmManager pgEdmManager;
    private final EdmService         edm;

    public ExpandDataTables(
            DataMapstore dataMapstore,
            DataMapstoreProxy dmProxy, PostgresEdmManager pgEdmManager,
            EdmService edm ) {
        this.dataMapstore = dataMapstore;
        this.dmProxy = dmProxy;
        this.pgEdmManager = pgEdmManager;
        this.edm = edm;
    }

    public void clearTables() {

    }

    public void migrateEdm() throws SQLException {
        logger.info( "Starting creation of edm tables."
        for ( EntitySet es : edm.getEntitySets() ) {
            EntityType et = edm.getEntityType( es.getEntityTypeId() );
            final Collection<PropertyType> propertyTypes = edm.getPropertyTypes( et.getProperties() );
            pgEdmManager.createEntitySet( es, propertyTypes );
        }
    }

    public void migrate() {
        for ( DataKey dataKey : dataMapstore.loadAllKeys() ) {
            EntityDataMapstore entityDataMapstore = dmProxy.getMapstore( dataKey.getEntitySetId() );
            PropertyDataMapstore pdm = dmProxy
                    .getPropertyMapstore( dataKey.getEntitySetId(), dataKey.getPropertyTypeId() );
            UUID entityKeyId = dataKey.getId();
            entityDataMapstore.store( entityKeyId, EntityDataMetadata.newEntityDataMetadata( OffsetDateTime.now() ) );
            ByteBuffer buffer = dataMapstore.load( dataKey );
            Object obj = CassandraSerDesFactory.deserializeValue( ObjectMappers.getJsonMapper(),
                    buffer,
                    edm.getPropertyType( dataKey.getPropertyTypeId() ).getDatatype(),
                    dataKey.getEntityId() );
            pdm.store( entityKeyId, ImmutableMap.of() );
        }
    }
}
