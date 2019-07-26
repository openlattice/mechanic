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
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.ids.IdGenerationMapstore;
import com.openlattice.mechanic.Toolbox;
import com.openlattice.mechanic.integrity.EdmChecks;
import com.openlattice.mechanic.integrity.IntegrityChecks;
import com.openlattice.mechanic.retired.DropEdmVersions;
import com.openlattice.mechanic.retired.DropPrincipalTree;
import com.openlattice.mechanic.retired.EntitySetFlags;
import com.openlattice.mechanic.upgrades.*;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore;
import com.openlattice.postgres.mapstores.PropertyTypeMapstore;
import com.zaxxer.hikari.HikariDataSource;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
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

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private AssemblerConfiguration assemblerConfiguration;

    @Bean
    public PostgresEdmManager pgEdmManager() {
        return new PostgresEdmManager( hikariDataSource, hazelcastInstance );
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
                hikariDataSource,
                (PropertyTypeMapstore) mapstoresPod.propertyTypeMapstore(),
                (EntityTypeMapstore) mapstoresPod.entityTypeMapstore(),
                (EntitySetMapstore) mapstoresPod.entitySetMapstore(),
                executor );
    }

    @Bean
    public Toolbox toolbox() {
        return new Toolbox(
                tableManager,
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

    @Bean MediaServerUpgrade mediaServerUpgrade() {
        return new MediaServerUpgrade( toolbox() );
    }

    MediaServerCleanup mediaServerCleanup() {
        return new MediaServerCleanup( toolbox() );
    }

    @Bean
    ReadLinking readLinking() {
        return new ReadLinking( toolbox() );
    }

    @Bean
    RemoveEntitySetTables removeEntitySetTables() {
        return new RemoveEntitySetTables( toolbox() );
    }

    @Bean
    PropertyValueIndexing propertyValueIndexing() {
        return new PropertyValueIndexing( toolbox() );
    }

    @Bean
    LastMigrateColumnUpgrade lastMigrateColumnUpgrade() {
        return new LastMigrateColumnUpgrade( toolbox() );
    }

    @Bean
    EntitySetFlags entitySetFlags() {
        return new EntitySetFlags( toolbox() );
    }

    @Bean
    MaterializedEntitySets materializedEntitySets() {
        return new MaterializedEntitySets( toolbox() );
    }

    @Bean
    DropEdmVersions dropEdmVersions() {
        return new DropEdmVersions( toolbox() );
    }

    @Bean
    DropPrincipalTree dropPrincipalTree() {
        return new DropPrincipalTree( toolbox() );
    }

    @Bean
    MaterializationForeignServer materializationForeignServer() {
        return new MaterializationForeignServer(
                (OrganizationAssemblyMapstore) mapstoresPod.organizationAssemblies(),
                assemblerConfiguration );
    }

    @Bean
    MaterializedEntitySetRefresh materializedEntitySetRefresh() {
        return new MaterializedEntitySetRefresh( toolbox() );
    }

    @Bean
    CreateDataTable upgradeCreateDataTable() {
        return new CreateDataTable( toolbox() );
    }

    @Bean
    UpgradeEntitySetPartitions upgradeEntitySetPartitions() {
        return new UpgradeEntitySetPartitions( toolbox() );
    }

    @Bean
    MigratePropertyValuesToDataTable migratePropertyValuesToDataTable() {
        return new MigratePropertyValuesToDataTable( toolbox() );
    }

    @Bean
    UpgradeEdgesTable upgradeEdgesTable() {
        return new UpgradeEdgesTable( toolbox() );
    }

    @Bean
    CreateDataTableIndexes createDataTableIndexes() {
        return new CreateDataTableIndexes( toolbox() );
    }

    @Bean
    ResetMigratedVersions resetMigratedVersions() {
        return new ResetMigratedVersions( toolbox() );
    }

    @Bean
    UpgradeEntityKeyIdsTable upgradeEntityKeyIdsTable() {
        return new UpgradeEntityKeyIdsTable( toolbox() );
    }

    @Bean
    InsertEntityKeyIdsToDataTable insertEntityKeyIdsToDataTable() {
        return new InsertEntityKeyIdsToDataTable( toolbox() );
    }
}
