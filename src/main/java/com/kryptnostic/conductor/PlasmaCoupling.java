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

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.context.annotation.Configuration;

import com.dataloom.hazelcast.serializers.ConductorCallStreamSerializer;
import com.dataloom.hazelcast.serializers.ConductorElasticsearchCallStreamSerializer;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;

@Configuration
public class PlasmaCoupling {
    @Inject
    private ConductorSparkApi                          api;

    @Inject
    private ConductorCallStreamSerializer              ccss;

    @Inject
    private ConductorElasticsearchApi                  elasticsearchApi;

    @Inject
    private ConductorElasticsearchCallStreamSerializer cecss;

    @PostConstruct
    public void connect() {
        ccss.setConductorSparkApi( api );
        cecss.setConductorElasticsearchApi( elasticsearchApi );
    }

}
