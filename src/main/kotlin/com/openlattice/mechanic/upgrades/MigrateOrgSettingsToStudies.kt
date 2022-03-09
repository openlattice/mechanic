package com.openlattice.mechanic.upgrades

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EdmConstants
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class MigrateOrgSettingsToStudies(
    val toolbox: Toolbox,
    private val entitySetsManager: EntitySetManager,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val dataQueryService: PostgresEntityDataQueryService

) : Upgrade {
    private val entitySetIdsByName: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).values.associate { it.name to it.id }

    companion object {
        private val logger = LoggerFactory.getLogger(MigrateOrgSettingsToStudies::class.java)
        private val mapper = ObjectMappers.getJsonMapper()

        // app ids
        private val DATA_COLLECTION_APP_ID = UUID.fromString("c4e6d8fd-daf9-41e7-8c59-2a12c7ee0857")
        private val SURVEY_APP_ID = UUID.fromString("bb44218b-515a-4314-b955-df2c991b2575")
        private val CHRONICLE_APP_ID = UUID.fromString("82e5504b-4dca-4600-a321-fa8e00e3b788")

        private val LEGACY_ORG_ID = UUID.fromString("7349c446-2acc-4d14-b2a9-a13be39cff93")
        private val RICE_UNIVERSITY_ORG_ID = UUID.fromString("a77a8f87-9e3f-4ae1-bfb1-c5c72f194fa8")

        private const val LEGACY_STUDY_ENTITY_SET = "chronicle_study"

        private val OL_ID_FQN = EdmConstants.ID_FQN
        private val STRING_ID_FQN = FullQualifiedName("general.stringid")

        private val appIdToComponentMapping = mapOf(
            DATA_COLLECTION_APP_ID to AppComponents.CHRONICLE_DATA_COLLECTION,
            SURVEY_APP_ID to AppComponents.CHRONICLE_SURVEYS,
            CHRONICLE_APP_ID to AppComponents.CHRONICLE
        )

        // settings keys
        private const val APP_FREQUENCY_SETTING = "appUsageFrequency"
        private const val COMPONENTS_SETTING = "components"

        // columns to insert
        private const val LEGACY_STUDY_ID = "v2_study_id"
        private const val LEGACY_STUDY_EK_ID = "v2_study_ekid"
        private const val SETTINGS = "settings"

        private const val STUDY_SETTINGS_TABLE = "study_settings"

        private val CREATE_TABLE_QUERY = """
            CREATE TABLE IF NOT EXISTS $STUDY_SETTINGS_TABLE(
               $LEGACY_STUDY_EK_ID uuid NOT NULL,
               $LEGACY_STUDY_ID uuid NOT NULL,
               $SETTINGS jsonb,
               PRIMARY KEY ($LEGACY_STUDY_EK_ID)
            )
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyEntityKeyId
         * 2) studyId
         * 3) settings
         */
        private val INSERT_INTO_SETTINGS_TABLE_QUERY = """
            INSERT INTO $STUDY_SETTINGS_TABLE ($LEGACY_STUDY_EK_ID, $LEGACY_STUDY_ID, $SETTINGS) VALUES (?, ?, ?::jsonb)
            ON CONFLICT DO NOTHING
        """.trimIndent()
    }

    init {
        getDataSource().connection.createStatement().use { stmt ->
            stmt.execute(CREATE_TABLE_QUERY)
        }
    }

    override fun upgrade(): Boolean {
        val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)

        val appIdsByOrganizationId: MutableMap<UUID, Set<UUID>> = appConfigs.keys
            .filter { it.appId == DATA_COLLECTION_APP_ID || it.appId == SURVEY_APP_ID || it.appId == CHRONICLE_APP_ID }
            .groupBy { it.organizationId }
            .mapValues { (_, configs) -> configs.map { it.appId }.toSet() }
            .toMutableMap()

        // legacy org id should have all app components
        appIdsByOrganizationId[LEGACY_ORG_ID] = appIdToComponentMapping.keys

        val studiesByOrgId: Map<UUID, List<Study>> = getStudiesByOrgId(appIdsByOrganizationId.keys, appIdsByOrganizationId)
        logger.info("Entities to write by org: ${studiesByOrgId.mapValues { it.value.size }}")

        val rowsWritten = writeEntitiesToTable(studiesByOrgId)
        logger.info("Wrote $rowsWritten study setting entities to table")
        return true
    }

    private fun writeEntitiesToTable(entities: Map<UUID, List<Study>>): Int {
        return getDataSource().connection.use { connection ->
            try {
                val wc = connection.prepareStatement(INSERT_INTO_SETTINGS_TABLE_QUERY).use { ps ->
                    entities.values.flatten().forEach {
                        ps.setObject(1, it.studyEntityKeyId)
                        ps.setObject(2, it.studyId)
                        ps.setString(3, mapper.writeValueAsString(it.settings))
                        ps.addBatch()
                    }
                    ps.executeBatch().sum()
                }
                return@use wc
            } catch (ex: Exception) {
                throw ex
            }
        }
    }

    private fun getDataSource(): HikariDataSource {
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["chronicle"]!!
        val hc = HikariConfig(hikariConfiguration)
        return HikariDataSource(hc)
    }

    private fun getStudiesByOrgId(orgIds: Set<UUID>, appIdsByOrganizationId: Map<UUID, Set<UUID>>): Map<UUID, List<Study>> {
        val studyIdEntitySetIdByOrgId = getEntitySetIds(orgIds)

        return orgIds.associateWith { orgId ->
            getStudyEntities(
                orgId,
                appIdsByOrganizationId.getValue(orgId),
                studyIdEntitySetIdByOrgId.getValue(orgId)
            )
        }
    }

    private fun getStudyEntities(orgId: UUID, appIds: Set<UUID>, entitySetId: UUID): List<Study> {
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
            mapOf(entitySetId to Optional.empty()),
            entitySetsManager.getPropertyTypesOfEntitySets(setOf(entitySetId)),
            mapOf(),
            setOf(),
            Optional.empty(),
            false
        ).values.filter { getFirstUUIDOrNull(it, STRING_ID_FQN) != null } .map { getStudyEntity(it, appIds, orgId) }.toList()
    }

    private fun getStudyEntity(entity: Map<FullQualifiedName, Set<Any>>, appIds: Set<UUID>, orgId: UUID): Study {
        val studyId = getFirstUUIDOrNull(entity, STRING_ID_FQN)
        val studyEntityKeyId = getFirstUUIDOrNull(entity, OL_ID_FQN)

        val settings: MutableMap<String, Any> = mutableMapOf()

        val appComponents =  appIds.map { appIdToComponentMapping.getValue(it) }.toMutableSet()
        if (appIds.contains(SURVEY_APP_ID) && orgId != LEGACY_ORG_ID) {
            appComponents.add(AppComponents.TIME_USE_DIARY)
        }
        settings[COMPONENTS_SETTING] = appComponents
        settings[APP_FREQUENCY_SETTING] = if (orgId == RICE_UNIVERSITY_ORG_ID) AppUsageFrequency.HOURLY else AppUsageFrequency.DAILY

        return Study(
            studyEntityKeyId = studyEntityKeyId!!,
            studyId = studyId!!,
            settings = settings
        )
    }

    private fun getEntitySetIds(orgIds: Set<UUID>): Map<UUID, UUID> {
        return orgIds.associateWith { orgId ->
            when (orgId) {
                LEGACY_ORG_ID -> LEGACY_STUDY_ENTITY_SET
                else -> "chronicle_${orgId.toString().replace("-", "")}_studies"
            }
        }.mapValues { entitySetIdsByName.getValue(it.value) }
    }

    private fun getFirstUUIDOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): UUID? {
        return when (val string = getFirstValueOrNull(entity, fqn)) {
            null -> null
            else -> UUID.fromString(string)
        }
    }

    private fun getFirstValueOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): String? {
        entity[fqn]?.iterator()?.let {
            if (it.hasNext()) return it.next().toString()
        }
        return null
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}

private enum class AppComponents {
    CHRONICLE,
    CHRONICLE_DATA_COLLECTION,
    CHRONICLE_SURVEYS,
    TIME_USE_DIARY
}

private enum class AppUsageFrequency {
    DAILY,
    HOURLY
}