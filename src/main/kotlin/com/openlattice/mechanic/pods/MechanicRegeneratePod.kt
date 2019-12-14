/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
package com.openlattice.mechanic.pods

import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.ids.IdGenerationMapstore
import com.openlattice.mechanic.MechanicCli.Companion.REGEN
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.regenerate.RegenerateIds
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import javax.inject.Inject

@Configuration
@Import(MechanicToolboxPod::class)
@Profile(REGEN)
class MechanicRegeneratePod {

    @Inject
    private lateinit var mapstoresPod: MapstoresPod

    @Inject
    private lateinit var toolbox: Toolbox

    @Bean
    fun regenerateIds(): RegenerateIds {
        return RegenerateIds(
                toolbox,
                mapstoresPod.idGenerationMapstore() as IdGenerationMapstore,
                mapstoresPod.principalTreesMapstore()
        )
    }
}