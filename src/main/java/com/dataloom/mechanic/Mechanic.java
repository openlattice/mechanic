/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
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
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.mechanic;

import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.mechanic.pods.CassandraTablesPod;
import com.dataloom.mechanic.pods.MechanicUpgradePod;
import com.dataloom.mechanic.upgrades.CassandraToPostgres;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.postgres.PostgresPod;
import digital.loom.rhizome.authentication.Auth0Pod;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class
Mechanic extends RhizomeApplicationServer {
    public static final  Class<?>[] rhizomePods   = new Class<?>[] {
            CassandraPod.class//,
            //            RegistryBasedHazelcastInstanceConfigurationPod.class
    };
    public static final  Class<?>[] conductorPods = new Class<?>[] {
            TypeCodecsPod.class,
            CassandraPod.class,
            MapstoresPod.class,
            //            SharedStreamSerializersPod.class,
            //            MechanicServicesPod.class,
            CassandraTablesPod.class,
            MechanicUpgradePod.class,
            Auth0Pod.class,
            JdbcPod.class,
            PostgresPod.class
    };
    private static final Logger     logger        = LoggerFactory.getLogger( Mechanic.class );

    public Mechanic() {
        super( RhizomeUtils.Pods.concatenate( RhizomeApplicationServer.DEFAULT_PODS, rhizomePods, conductorPods ) );
    }

    @Override
    public void sprout( String... activeProfiles ) {
        super.sprout( activeProfiles );
    }

    public static void main( String[] args ) throws InterruptedException, ExecutionException, SQLException {
        Mechanic mechanic = new Mechanic();
        mechanic.sprout( args );

        logger.info( "Starting upgrade!" );
        CassandraToPostgres cassandraToPostgres = mechanic.getContext().getBean( CassandraToPostgres.class );

        //CORE
        logger.info( "Migrated {} property types", cassandraToPostgres.migratePropertyTypes() );
        logger.info( "Migrated {} acl keys.", cassandraToPostgres.migrateAclKeys() );
        logger.info( "Migrated {} entity types", cassandraToPostgres.migrateEntityTypes() );
        logger.info( "Migrated {} names", cassandraToPostgres.migrateNames() );
        logger.info( "Migrated {} association types", cassandraToPostgres.migrateAssociationTypes() );
        logger.info( "Migrated {} entity set property metadata",
                cassandraToPostgres.migrateEntitySetPropertyMetadata() );
        logger.info( "Migrated {} linked entity types", cassandraToPostgres.migratePermissions() );
        //        logger.info( "Migrated {} entity key ids", cassandraToPostgres.migrateEntityKeyIds() );
        logger.info( "Migrated {} entity sets", cassandraToPostgres.migrateEntitySets() );
        logger.info( "Migrated {} sync ids", cassandraToPostgres.migrateSyncIds() );
        logger.info( "Migrated {} organizations", cassandraToPostgres.migrateOrganizations() );
        logger.info( "Migrated {} securable object types", cassandraToPostgres.migrateSecurableObjectTypes() );
        //Secondary
        logger.info( "Migrated {} schemas", cassandraToPostgres.migrateSchemas() );
        logger.info( "Migrated {} linked entity sets", cassandraToPostgres.migrateLinkedEntitySets() );
        logger.info( "Migrated {} linking vertices", cassandraToPostgres.migratelinkingVertices() );
        //        logger.info( "Migrated {} edm versions mapstore", cassandraToPostgres.migrateEdmVersionsMapstore() );
        logger.info( "Migrated {} linked entity types", cassandraToPostgres.migrateLinkedEntityTypes() );
        //cassandraToPostgres.migratePermissions();
        //long count = mechanic.getContext().getBean( ManualPartitionOfDataTable.class ).migrate();;
        //ReadBench readBench = mechanic.getContext().getBean( ReadBench.class );
        //readBench.benchmark();

        logger.info( "Upgrade complete!" );

        mechanic.plowUnder();
    }

}
