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

package com.openlattice.mechanic;

import static com.openlattice.mechanic.pods.MechanicUpgradePod.INTEGRITY;
import static com.openlattice.mechanic.pods.MechanicUpgradePod.REGEN;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles;
import com.kryptnostic.rhizome.core.Rhizome;
import com.kryptnostic.rhizome.pods.AsyncPod;
import com.kryptnostic.rhizome.pods.ConfigurationPod;
import com.kryptnostic.rhizome.startup.Requirement;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.mechanic.integrity.IntegrityChecks;
import com.openlattice.mechanic.pods.MechanicUpgradePod;
import com.openlattice.mechanic.upgrades.RegenerateIds;
import com.openlattice.postgres.PostgresPod;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Mechanic {
    private static final Class<?>[]                         mechanicPods = new Class<?>[] {
            Auth0Pod.class,
            JdbcPod.class,
            PostgresPod.class,
            MapstoresPod.class,
            MechanicUpgradePod.class,
            AsyncPod.class,
            ConfigurationPod.class
    };
    private static final Logger                             logger       = LoggerFactory.getLogger( Mechanic.class );
    private final        AnnotationConfigApplicationContext context      = new AnnotationConfigApplicationContext();

    public Mechanic() {
        this.context.register( mechanicPods );
    }

    public void sprout( String... activeProfiles ) {
        boolean awsProfile = false;
        boolean localProfile = false;
        for ( String profile : activeProfiles ) {
            if ( StringUtils.equals( Profiles.AWS_CONFIGURATION_PROFILE, profile ) ) {
                awsProfile = true;
            }

            if ( StringUtils.equals( Profiles.LOCAL_CONFIGURATION_PROFILE, profile ) ) {
                localProfile = true;
            }

            context.getEnvironment().addActiveProfile( profile );
        }

        if ( !awsProfile && !localProfile ) {
            context.getEnvironment().addActiveProfile( Profiles.LOCAL_CONFIGURATION_PROFILE );
        }

        /*if ( additionalPods.size() > 0 ) {
            context.register( additionalPods.toArray( new Class<?>[] {} ) );
        }*/
        context.refresh();

        if ( context.isRunning() && startupRequirementsSatisfied( context ) ) {
            Rhizome.showBanner();
        }
    }

    public static void main( String[] args ) throws InterruptedException, ExecutionException, SQLException {
        Mechanic mechanic = new Mechanic();
        mechanic.sprout( args );
        //        Console console = System.console();
        //
        //        if ( console == null ) {
        //             logger.error("Cannot be run in non-interactive mode.");
        //        }

        Stopwatch w = Stopwatch.createStarted();

        if ( Stream.of( mechanic.context.getEnvironment().getActiveProfiles() )
                .anyMatch( profile -> profile.equals( INTEGRITY ) ) ) {
            IntegrityChecks checks = mechanic.context.getBean( IntegrityChecks.class );
            checks.ensureEntityKeyIdsSynchronized();
        }

        if ( Stream.of( mechanic.context.getEnvironment().getActiveProfiles() )
                .anyMatch( profile -> profile.equals( REGEN ) ) ) {
            RegenerateIds regen = mechanic.context.getBean( RegenerateIds.class );

            if ( Stream.of( args ).anyMatch( arg -> arg.equals( "idgen" ) ) ) {
                regen.initRanges();
            }

            regen.assignNewEntityKeysIds();

            ListeningExecutorService executor = mechanic.context.getBean( ListeningExecutorService.class );

            if ( Stream.of( args ).anyMatch( arg -> arg.equals( "upgrade" ) ) ) {
                CountDownLatch latch = new CountDownLatch( 3 );
                executor.execute( () -> {
                    logger.info( "Upgrade edge tables." );
                    regen.updateEdgesTables();
                    latch.countDown();
                } );
                executor.execute( () -> {
                    logger.info( "Upgrade entity tables." );
                    regen.updateEntityTables();
                    latch.countDown();
                } );
                executor.execute( () -> {
                    logger.info( "Upgrading property tables." );
                    regen.updatePropertyTables();
                    latch.countDown();
                } );
                latch.await();
            }
        }
        //        long assigned = regen.assignNewEntityKeysIds();
        //expander.migrate();

        //        CassandraToPostgres cassandraToPostgres = mechanic.getContext().getBean( CassandraToPostgres.class );

        //CORE
        //        logger.info( "Migrated {} property types", cassandraToPostgres.migratePropertyTypes() );
        //        logger.info( "Migrated {} acl keys.", cassandraToPostgres.migrateAclKeys() );
        //        logger.info( "Migrated {} entity types", cassandraToPostgres.migrateEntityTypes() );
        //        logger.info( "Migrated {} names", cassandraToPostgres.migrateNames() );
        //        logger.info( "Migrated {} association types", cassandraToPostgres.migrateAssociationTypes() );
        //        logger.info( "Migrated {} entity set property metadata",
        //                cassandraToPostgres.migrateEntitySetPropertyMetadata() );
        //        logger.info( "Migrated {} linked entity types", cassandraToPostgres.migratePermissions() );
        //        logger.info( "Migrated {} entity key ids", cassandraToPostgres.migrateEntityKeyIds() );
        //        logger.info( "Migrated {} entity sets", cassandraToPostgres.migrateEntitySets() );
        //        logger.info( "Migrated {} sync ids", cassandraToPostgres.migrateSyncIds() );
        //        logger.info( "Migrated {} organizations", cassandraToPostgres.migrateOrganizations() );
        //        logger.info( "Migrated {} securable object types", cassandraToPostgres.migrateSecurableObjectTypes() );
        //Secondary
        //        logger.info( "Migrated {} schemas", cassandraToPostgres.migrateSchemas() );
        //        logger.info( "Migrated {} linked entity sets", cassandraToPostgres.migrateLinkedEntitySets() );
        //        logger.info( "Migrated {} linking vertices", cassandraToPostgres.migratelinkingVertices() );
        //        logger.info( "Migrated {} edm versions mapstore", cassandraToPostgres.migrateEdmVersionsMapstore() );
        //        logger.info( "Migrated {} linked entity types", cassandraToPostgres.migrateLinkedEntityTypes() );
        //cassandraToPostgres.migratePermissions();
        //long count = mechanic.getContext().getBean( ManualPartitionOfDataTable.class ).migrate();;
        //ReadBench readBench = mechanic.getContext().getBean( ReadBench.class );
        //readBench.benchmark();
        //        logger.info( "Assigned {} new ids in {} ms.", assigned, w.elapsed( TimeUnit.MILLISECONDS ) );
        logger.info( "Upgrade complete!" );
        mechanic.context.close();
        System.exit( 0 );
    }

    public static boolean startupRequirementsSatisfied( AnnotationConfigApplicationContext context ) {
        return context.getBeansOfType( Requirement.class )
                .values()
                .parallelStream()
                .allMatch( Requirement::isSatisfied );
    }
}
