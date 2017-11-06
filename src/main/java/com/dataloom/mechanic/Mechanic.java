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
import com.dataloom.hazelcast.pods.SharedStreamSerializersPod;
import com.dataloom.mechanic.benchmark.ReadBench;
import com.dataloom.mechanic.pods.CassandraTablesPod;
import com.dataloom.mechanic.pods.MechanicServicesPod;
import com.dataloom.mechanic.pods.MechanicUpgradePod;
import com.dataloom.mechanic.upgrades.CassandraToPostgres;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.postgres.PostgresPod;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class Mechanic extends RhizomeApplicationServer {
    private static final Logger     logger      = LoggerFactory.getLogger( Mechanic.class );
    public static final  Class<?>[] rhizomePods = new Class<?>[] {
            CassandraPod.class,
            RegistryBasedHazelcastInstanceConfigurationPod.class };

    public static final Class<?>[] conductorPods = new Class<?>[] {
            TypeCodecsPod.class,
            CassandraPod.class,
            MapstoresPod.class,
            SharedStreamSerializersPod.class,
            MechanicServicesPod.class,
            CassandraTablesPod.class,
            MechanicUpgradePod.class,
            JdbcPod.class,
            PostgresPod.class
    };

    public Mechanic() {
        super( RhizomeUtils.Pods.concatenate( RhizomeApplicationServer.DEFAULT_PODS, rhizomePods, conductorPods ) );
    }

    public static void main( String[] args ) throws InterruptedException, ExecutionException, SQLException {
        Mechanic mechanic = new Mechanic();
        mechanic.sprout( args );

        logger.info( "Starting upgrade!" );
        CassandraToPostgres cassandraToPostgres = mechanic.getContext().getBean( CassandraToPostgres.class );
        int count = cassandraToPostgres.migratePropertyTypes();;
        //cassandraToPostgres.migratePermissions();
        //long count = mechanic.getContext().getBean( ManualPartitionOfDataTable.class ).migrate();;
        //ReadBench readBench = mechanic.getContext().getBean( ReadBench.class );
        //readBench.benchmark();

        logger.info( "Upgrade complete! Migrated {} rows.", count );
        mechanic.plowUnder();
    }

    @Override
    public void sprout( String... activeProfiles ) {
        super.sprout( activeProfiles );
    }

}
