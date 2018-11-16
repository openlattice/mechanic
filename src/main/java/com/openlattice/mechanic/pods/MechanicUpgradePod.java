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

package com.openlattice.mechanic.pods;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.ids.IdGenerationMapstore;
import com.openlattice.mechanic.Toolbox;
import com.openlattice.mechanic.integrity.EdmChecks;
import com.openlattice.mechanic.integrity.IntegrityChecks;
import com.openlattice.mechanic.upgrades.GraphProcessing;
import com.openlattice.mechanic.upgrades.Linking;

import com.openlattice.mechanic.upgrades.MediaServerUpgrade;
import com.openlattice.mechanic.upgrades.ReadLinking;
import com.openlattice.mechanic.upgrades.RegenerateIds;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.PropertyTypeMapstore;
import com.zaxxer.hikari.HikariDataSource;
import javax.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MechanicUpgradePod {
    public static final String INTEGRITY = "integrity";
    public static final String REGEN     = "regen";

    @Inject
    HikariDataSource hikariDataSource;

    @Inject
    private PostgresTableManager tableManager;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private EventBus eventBus;

    @Inject
    private MapstoresPod mapstoresPod;

    @Bean
    public Jdbi jdbi() {
        Jdbi jdbi = Jdbi.create( hikariDataSource );
        jdbi.installPlugin( new SqlObjectPlugin() );
        return jdbi;
    }

    @Bean
    public PostgresEdmManager pgEdmManager() {
        return new PostgresEdmManager( hikariDataSource, tableManager );
    }

    @Bean
    @Profile( REGEN )
    public RegenerateIds regen() {

        return new RegenerateIds(
                pgEdmManager(),
                hikariDataSource,
                (PropertyTypeMapstore) mapstoresPod.propertyTypeMapstore(),
                (EntityTypeMapstore) mapstoresPod.entityTypeMapstore(),
                (EntitySetMapstore) mapstoresPod.entitySetMapstore(),
                (IdGenerationMapstore) mapstoresPod.idGenerationMapstore(),
                //                (PrincipalTreeMapstore) mapstoresPod.aclKeySetMapstore(),
                mapstoresPod.principalTreesMapstore(),
                executor );
    }

    @Bean
    @Profile( INTEGRITY )
    public IntegrityChecks integrityChecks() {
        return new IntegrityChecks(
                pgEdmManager(),
                hikariDataSource,
                (PropertyTypeMapstore) mapstoresPod.propertyTypeMapstore(),
                (EntityTypeMapstore) mapstoresPod.entityTypeMapstore(),
                (EntitySetMapstore) mapstoresPod.entitySetMapstore(),
                executor );
    }

    @Bean
    @Profile( INTEGRITY )
    public EdmChecks edmChecks() {
        return new EdmChecks(
                pgEdmManager(),
                hikariDataSource,
                (PropertyTypeMapstore) mapstoresPod.propertyTypeMapstore(),
                (EntityTypeMapstore) mapstoresPod.entityTypeMapstore(),
                (EntitySetMapstore) mapstoresPod.entitySetMapstore(),
                executor );
    }

    @Bean Toolbox toolbox() {
        return new Toolbox(
                tableManager,
                pgEdmManager(),
                hikariDataSource,
                (PropertyTypeMapstore) mapstoresPod.propertyTypeMapstore(),
                (EntityTypeMapstore) mapstoresPod.entityTypeMapstore(),
                (EntitySetMapstore) mapstoresPod.entitySetMapstore(),
                executor );
    }

    @Bean
    Linking linking() {
        return new Linking( toolbox() );
    }

    @Bean GraphProcessing graph() {
        return new GraphProcessing( toolbox() );
    }

    @Bean MediaServerUpgrade mediaServerUpgrade() { return new MediaServerUpgrade( toolbox() );}

    @Bean
    ReadLinking readLinking() {
        return new ReadLinking( toolbox() );
    }

}
