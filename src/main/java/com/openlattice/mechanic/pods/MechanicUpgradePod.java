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
import com.openlattice.mechanic.upgrades.ExpandDataTables;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.PropertyTypeMapstore;
import com.openlattice.postgres.mapstores.data.DataMapstoreProxy;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import javax.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.springframework.context.annotation.Bean;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MechanicUpgradePod {

    @Inject
    HikariDataSource hikariDataSource;

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
    public ExpandDataTables edt() throws SQLException {
        return new ExpandDataTables(
                hikariDataSource,
                (DataMapstoreProxy) mapstoresPod.entityDataMapstore(),
                new PostgresEdmManager( new PostgresTableManager( hikariDataSource ), hikariDataSource ),
                (PropertyTypeMapstore) mapstoresPod.propertyTypeMapstore(),
                (EntityTypeMapstore) mapstoresPod.entityTypeMapstore(),
                (EntitySetMapstore) mapstoresPod.entitySetMapstore(), executor );
    }

}
