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

import org.apache.commons.cli.*
import kotlin.system.exitProcess

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MechanicCli {
    companion object {
        const val HELP = "help"
        const val UPGRADE = "upgrade"
        const val CHECK = "check"
        const val REINDEX = "reindex"
        const val AWS = "aws"
        const val POSTGRES = "postgres"
        const val LOCAL = "local"
        const val REGEN = "regen"
        const val RETIRE = "retire"


        private val options = Options()
        private val clp = DefaultParser()
        private val hf = HelpFormatter()

        private val awsOption = Option.builder()
                .longOpt(AWS)
                .desc("Attempt to load configuration from AWS.")
                .hasArg(false)
                .build()

        private val localOption = Option.builder()
                .longOpt(LOCAL)
                .desc("Attempt to load configuration from AWS.")
                .hasArg(false)
                .build()

        private val checkOption = Option.builder()
                .longOpt(CHECK)
                .hasArgs()
                .argName("name")
                .desc("Run checks on the system. ")
                .valueSeparator(',')
                .build()

        private val reindexOption = Option.builder()
                .longOpt(REINDEX)
                .desc("Reindex all the data in the system.")
                .hasArg()
                .argName("es1,es2")
                .optionalArg(true)
                .valueSeparator(',')
                .build()

        private val upgradeOption = Option.builder()
                .longOpt(UPGRADE)
                .desc("Run upgrade tasks.")
                .hasArgs()
                .argName("name")
                .valueSeparator(',')
                .build()

        private val helpOption = Option.builder(HELP.first().toString())
                .longOpt(HELP)
                .desc("Print this help message.")
                .hasArg(false)
                .build()

        private val postgresOption = Option.builder()
                .longOpt(POSTGRES)
                .desc("Use postgres backend.")
                .hasArg(false)
                .build()

        private val regenOption = Option.builder()
                .longOpt(REGEN)
                .hasArgs()
                .argName("name")
                .desc("Run regeneration on the system. ")
                .valueSeparator(',')
                .build()

        private val retireOption = Option.builder()
                .longOpt(RETIRE)
                .hasArgs()
                .argName("name")
                .desc("Run retirees on the system. ")
                .valueSeparator(',')
                .build()

        init {
            options.addOption(helpOption)
            options.addOption(postgresOption)
            options.addOption(awsOption)
            options.addOption(localOption)
            options.addOption(checkOption)
            options.addOption(reindexOption)
            options.addOption(upgradeOption)
            options.addOption(regenOption)
            options.addOption(retireOption)

            options.addOptionGroup(
                    OptionGroup()
                            .addOption(awsOption)
                            .addOption(localOption)
            )
        }

        @JvmStatic
        fun parseCommandLine(args: Array<String>): CommandLine {
            try {
                return clp.parse(options, args)
            } catch (ex: AlreadySelectedException) {
                println(ex.message)
                printHelp()
                exitProcess(1)
            }
        }

        fun printHelp() {
            hf.printHelp("mechanic", options)
        }
    }

}