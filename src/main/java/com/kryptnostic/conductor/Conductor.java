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

package com.kryptnostic.conductor;

import java.util.concurrent.ExecutionException;

import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.hazelcast.pods.SharedStreamSerializersPod;
import com.dataloom.mail.pods.MailServicePod;
import com.dataloom.mail.services.MailService;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.conductor.pods.ConductorSparkPod;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;

/**
 * This class will not run unless ./gradlew :kindling:clean :kindling:build :kindling:shadow --daemon has been run in
 * super project. You must also download Spark 1.6.2 w/ Hadoop and have a master and slave running locally. Finally you
 * must make sure to update {@link ConductorSparkPod} with your spark master URL.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class Conductor extends RhizomeApplicationServer {
    public static final Class<?>[] rhizomePods   = new Class<?>[] {
            CassandraPod.class,
            RegistryBasedHazelcastInstanceConfigurationPod.class };

    public static final Class<?>[] conductorPods = new Class<?>[] {
            ConductorSparkPod.class,
            TypeCodecsPod.class,
            SharedStreamSerializersPod.class,
            PlasmaCoupling.class,
            MailServicePod.class,
            CassandraPod.class,
            CassandraTablesPod.class,
            MapstoresPod.class
    };

    public Conductor() {
        super( Pods.concatenate( RhizomeApplicationServer.DEFAULT_PODS, rhizomePods, conductorPods ) );
    }

    public static void main( String[] args ) throws InterruptedException, ExecutionException {
        new Conductor().sprout( args );
    }

    @Override
    public void sprout( String... activeProfiles ) {
        super.sprout( activeProfiles );
        getContext().getBean( MailService.class ).processEmailRequestsQueue();
    }
}
