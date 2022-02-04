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
 *
 */

package com.openlattice.mechanic

import com.google.common.base.Preconditions.checkArgument
import com.google.common.collect.Maps
import com.geekbeast.rhizome.configuration.ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE
import com.geekbeast.rhizome.configuration.ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE
import com.geekbeast.rhizome.core.Rhizome

import com.geekbeast.rhizome.core.RhizomeApplicationServer
import com.geekbeast.rhizome.hazelcast.serializers.RhizomeUtils
import com.geekbeast.rhizome.pods.MetricsPod
import com.geekbeast.rhizome.startup.Requirement
import com.openlattice.assembler.pods.AssemblerConfigurationPod
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.geekbeast.auth0.Auth0Pod
import com.openlattice.chronicle.pods.ChronicleConfigurationPod
import com.openlattice.chronicle.pods.ChronicleServerServicesPod
import com.openlattice.datastore.pods.ByteBlobServicePod
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.ioc.providers.LateInitProvidersPod
import com.geekbeast.jdbc.JdbcPod
import com.openlattice.mechanic.MechanicCli.Companion.AWS
import com.openlattice.mechanic.MechanicCli.Companion.CHECK
import com.openlattice.mechanic.MechanicCli.Companion.HELP
import com.openlattice.mechanic.MechanicCli.Companion.LOCAL
import com.openlattice.mechanic.MechanicCli.Companion.POSTGRES
import com.openlattice.mechanic.MechanicCli.Companion.REINDEX
import com.openlattice.mechanic.MechanicCli.Companion.UPGRADE
import com.openlattice.mechanic.integrity.Check
import com.openlattice.mechanic.pods.MechanicChronicleByteBlobServicePod
import com.openlattice.mechanic.pods.MechanicChronicleMapstoresPod
import com.openlattice.mechanic.pods.MechanicChronicleSharedStreamSerializersPod
import com.openlattice.mechanic.pods.MechanicIntegrityPod
import com.openlattice.mechanic.pods.MechanicRetireePod
import com.openlattice.mechanic.pods.MechanicUpgradePod
import com.openlattice.mechanic.reindex.Reindexer
import com.openlattice.mechanic.upgrades.Upgrade
import com.geekbeast.postgres.PostgresPod
import com.openlattice.postgres.pods.ExternalDatabaseConnectionManagerPod
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import kotlin.system.exitProcess


private val logger = LoggerFactory.getLogger(Mechanic::class.java)

fun main(args: Array<String>) {
    val mechanic = Mechanic()
    val cl = MechanicCli.parseCommandLine(args)


    if (cl.hasOption(HELP)) {
        MechanicCli.printHelp()
        return
    }

    checkArgument(
            cl.hasOption(AWS) xor cl.hasOption(LOCAL),
            "Local or AWS configuration profile must be specified"
    )

    checkArgument(
            cl.hasOption(AWS) xor cl.hasOption(LOCAL),
            "Local or AWS configuration profile must be specified"
    )

    val ars = mutableListOf<String>()

    if (cl.hasOption(AWS)) {
        ars.add(AWS_CONFIGURATION_PROFILE)
    } else if (cl.hasOption(LOCAL_CONFIGURATION_PROFILE)) {
        ars.add(LOCAL_CONFIGURATION_PROFILE)
        ars.add("medialocal")
    }

    if (cl.hasOption(POSTGRES)) {
        ars.add(PostgresPod.PROFILE)
    }

    if (cl.hasOption(CHECK)) {
        ars.add(CHECK)
    }

    if (cl.hasOption(UPGRADE)) {
        ars.add(UPGRADE)
    }

    mechanic.sprout(*ars.toTypedArray())

    if (cl.hasOption(CHECK)) {
        val checks = cl.getOptionValues(CHECK).toSet()
        mechanic.runChecks(checks)
    }

    if (cl.hasOption(UPGRADE)) {
        val upgrades = cl.getOptionValues(UPGRADE).toSet()
        mechanic.doUpgrade(upgrades)
    }

    if (cl.hasOption(REINDEX)) {
        mechanic.reIndex()
    }

    mechanic.close()
    exitProcess(0)

}

/**
 *
 */
class Mechanic {

    private val mechanicPods = RhizomeUtils.Pods.concatenate(
        RhizomeApplicationServer.DEFAULT_PODS,
        arrayOf(
            AssemblerConfigurationPod::class.java,
            AuditingConfigurationPod::class.java,
            Auth0Pod::class.java,
            ByteBlobServicePod::class.java,
            ChronicleConfigurationPod::class.java,
            ChronicleServerServicesPod::class.java,
            JdbcPod::class.java,
            MapstoresPod::class.java,
            MechanicChronicleByteBlobServicePod::class.java,
            MechanicChronicleMapstoresPod::class.java,
            MechanicIntegrityPod::class.java,
            MechanicRetireePod::class.java,
            MechanicUpgradePod::class.java,
            MetricsPod::class.java,
            ExternalDatabaseConnectionManagerPod::class.java,
            PostgresPod::class.java,
            SharedStreamSerializersPod::class.java,
            MechanicChronicleSharedStreamSerializersPod::class.java,
            LateInitProvidersPod::class.java
            )
    )

    private val context = AnnotationConfigApplicationContext()

    fun sprout(vararg activeProfiles: String) {
        var awsProfile = false
        var localProfile = false
        for (profile in activeProfiles) {
            if (StringUtils.equals(AWS_CONFIGURATION_PROFILE, profile)) {
                awsProfile = true
            }

            if (StringUtils.equals(LOCAL_CONFIGURATION_PROFILE, profile)) {
                localProfile = true
            }

            context.environment.addActiveProfile(profile)
        }

        if (!awsProfile && !localProfile) {
            context.environment.addActiveProfile(LOCAL_CONFIGURATION_PROFILE)
        }

        context.register(*mechanicPods)
        context.refresh()

        if (context.isRunning && startupRequirementsSatisfied(context)) {
            Rhizome.showBanner()
        }
    }

    fun runChecks(checkNames: Set<String>, checkAll: Boolean = false) {
        val checks = context.getBeansOfType(Check::class.java)
        val results = if (checkAll) {
            checks.mapValues { it.value.check() }
        } else {
            check(checks.keys.containsAll(checkNames)) { "Unable to find checks: ${checkNames - checks.keys}" }
            Maps.toMap(checkNames) { name -> checks[name]!!.check() }
        }

        logger.info("Results of running checks: {}", results)
    }

    fun reIndex() {
        context.getBean(Reindexer::class.java).runReindex()
    }

    fun doUpgrade(upgradeNames: Set<String>) {
        val upgrades = context.getBeansOfType(Upgrade::class.java)
        if (!upgrades.keys.containsAll(upgradeNames)) {
            logger.error("Unable to find upgrades: {}", upgradeNames - upgrades.keys)
        }

        upgradeNames.forEach {
            if (upgrades[it]!!.upgrade()) {
                logger.info("Successfully completed $it")
            } else {
                logger.error("Upgrade failed on $it")
            }
        }
    }

    private fun startupRequirementsSatisfied(context: AnnotationConfigApplicationContext): Boolean {
        return context.getBeansOfType(Requirement::class.java)
                .values
                .parallelStream()
                .allMatch { it.isSatisfied }
    }

    fun close() {
        context.close()
    }
}