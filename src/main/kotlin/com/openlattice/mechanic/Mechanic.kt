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
import com.google.common.base.Stopwatch
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE
import com.kryptnostic.rhizome.core.Rhizome
import com.kryptnostic.rhizome.pods.AsyncPod
import com.kryptnostic.rhizome.pods.ConfigurationPod
import com.kryptnostic.rhizome.pods.HazelcastPod
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod
import com.kryptnostic.rhizome.startup.Requirement
import com.openlattice.auth0.Auth0Pod
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.jdbc.JdbcPod
import com.openlattice.mechanic.MechanicCli.Companion.AWS
import com.openlattice.mechanic.MechanicCli.Companion.CHECK
import com.openlattice.mechanic.MechanicCli.Companion.HELP
import com.openlattice.mechanic.MechanicCli.Companion.LOCAL
import com.openlattice.mechanic.MechanicCli.Companion.POSTGRES
import com.openlattice.mechanic.MechanicCli.Companion.REINDEX
import com.openlattice.mechanic.MechanicCli.Companion.SQL
import com.openlattice.mechanic.MechanicCli.Companion.UPGRADE
import com.openlattice.mechanic.integrity.EdmChecks
import com.openlattice.mechanic.integrity.IntegrityChecks
import com.openlattice.mechanic.pods.MechanicUpgradePod
import com.openlattice.mechanic.reindex.Reindexer
import com.openlattice.mechanic.upgrades.Upgrade
import com.openlattice.postgres.PostgresPod
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext


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

    val args = mutableListOf<String>()

    if (cl.hasOption(AWS)) {
        args.add(AWS_CONFIGURATION_PROFILE)
    } else if (cl.hasOption(LOCAL_CONFIGURATION_PROFILE)) {
        args.add(LOCAL_CONFIGURATION_PROFILE)
    }

    if (cl.hasOption(POSTGRES)) {
        args.add(PostgresPod.PROFILE)
    }

    mechanic.sprout(*args.toTypedArray())


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

    if (cl.hasOption(SQL)) {

    }

    val w = Stopwatch.createStarted()

    mechanic.close()
    System.exit(0)

}

/**
 *
 */
class Mechanic {

    private val mechanicPods = arrayOf(
            Auth0Pod::class.java, JdbcPod::class.java, PostgresPod::class.java, MapstoresPod::class.java,
            MechanicUpgradePod::class.java, AsyncPod::class.java, ConfigurationPod::class.java,
            RegistryBasedHazelcastInstanceConfigurationPod::class.java, HazelcastPod::class.java,
            SharedStreamSerializersPod::class.java
    )

    private val context = AnnotationConfigApplicationContext()

    init {
        this.context.register(*mechanicPods)
    }

    fun sprout(vararg activeProfiles: String) {
        var awsProfile = false
        var localProfile = false
        for (profile in activeProfiles) {
            if (StringUtils.equals(ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE, profile)) {
                awsProfile = true
            }

            if (StringUtils.equals(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE, profile)) {
                localProfile = true
            }

            context.environment.addActiveProfile(profile)
        }

        if (!awsProfile && !localProfile) {
            context.environment.addActiveProfile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
        }

        /*if ( additionalPods.size() > 0 ) {
            context.register( additionalPods.toArray( new Class<?>[] {} ) );
        }*/
        context.refresh()

        if (context.isRunning && startupRequirementsSatisfied(context)) {
            Rhizome.showBanner()
        }
    }

    fun runChecks(checks: Set<String>) {
        val integrityChecks = context.getBean(IntegrityChecks::class.java)
        val edmChecks = context.getBean(EdmChecks::class.java)
        checks.forEach {
            when (it) {
                "integrity" -> integrityChecks.ensureEntityKeyIdsSynchronized()
                "edm" -> edmChecks.checkPropertyTypesAlignWithTable()
            }
        }
    }

    fun reIndex() {
        val reindexer = context.getBean(Reindexer::class.java)
        reindexer.runReindex()
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