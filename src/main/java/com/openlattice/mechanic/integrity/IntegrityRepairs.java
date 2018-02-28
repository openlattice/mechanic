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

package com.openlattice.mechanic.integrity;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.postgres.PostgresTableManager;
import com.zaxxer.hikari.HikariDataSource;
import java.util.UUID;
import javax.inject.Inject;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IntegrityRepairs {

    @Inject private ListeningExecutorService executorService;
    @Inject private MapstoresPod             mp;
    @Inject private HikariDataSource         hds;
    @Inject private PostgresTableManager     ptm;

    private void repairSecurableObjectTypes() {
        SelfRegisteringMapStore<UUID, PropertyType> ptm = mp.propertyTypeMapstore();
//        SelfRegisteringMapStore<UUID, EntitySet> ptm = mp.entitySetMapstore();


    }
}
