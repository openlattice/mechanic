package com.openlattice.mechanic.upgrades

import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 *
 * This class migrates Android system apps - apps that are not considered to have been installed by the user and
 * need to be filtered from app usage survey data
 */
class MigrateChronicleSystemApps(
    val toolbox: Toolbox,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val entitySetService: EntitySetManager,
    private val dataQueryService: PostgresEntityDataQueryService

) : Upgrade {


    companion object {
        private val logger = LoggerFactory.getLogger(MigrateChronicleSystemApps::class.java)

        private const val ENTITY_SET_NAME = "chronicle_application_dictionary"

        private val FULL_NAME_FQN = FullQualifiedName("general.fullname")
        private val RECORD_TYPE_FQN = FullQualifiedName("ol.recordtype")

        private val CREATE_SYSTEM_APPS_SQL = """
            CREATE TABLE IF NOT EXISTS public.system_apps(
                app_package_name text NOT NULL,
                PRIMARY KEY (app_package_name)
            )
        """.trimIndent()

        private val INSERT_SYSTEM_APPS_SQL = """
            INSERT INTO public.system_apps values(?) ON CONFLICT DO NOTHING
        """.trimIndent()
    }

    init {
        val hds = getDataSource()
        hds.connection.createStatement().use { statement ->
            statement.execute(CREATE_SYSTEM_APPS_SQL)
        }
    }

    private fun getDataSource(): HikariDataSource {
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["chronicle"]!!
        val hc = HikariConfig(hikariConfiguration)
        return HikariDataSource(hc)
    }

    override fun upgrade(): Boolean {
        val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
        val entitySetId = entitySets.find { it.value.name == ENTITY_SET_NAME }?.key ?: return false

        val entitiesToWrite = getEntitiesToWrite(entitySetId)
        logger.info("retrieved ${entitiesToWrite.size} system apps")

        val hds = getDataSource()
        val written = hds.connection.prepareStatement(INSERT_SYSTEM_APPS_SQL).use { ps ->
            entitiesToWrite.forEach {
                ps.setString(1, it)
                ps.addBatch()
            }
            ps.executeBatch().sum()
        }

        logger.info("wrote $written to system_apps table")
        return true
    }

    private fun getEntitiesToWrite(entitySetId: UUID): Set<String> {
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
            mapOf(entitySetId to Optional.empty()),
            entitySetService.getPropertyTypesOfEntitySets(setOf(entitySetId)),
            mapOf(),
            setOf(),
            Optional.empty(),
            false
        ).values
            .filter { it[RECORD_TYPE_FQN]?.iterator()?.next().toString() == "SYSTEM" }
            .map { it[FULL_NAME_FQN]?.iterator()?.next().toString() }.toSet()
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}