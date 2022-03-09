package com.openlattice.mechanic.upgrades

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.openlattice.authorization.*
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.EdmConstants.Companion.LAST_WRITE_FQN
import com.openlattice.graph.PagedNeighborRequest
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.search.SearchService
import com.openlattice.search.requests.EntityNeighborsFilter
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class MigrateAppUsageSurveyData(
    toolbox: Toolbox,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val authorizationService: AuthorizationManager,
    private val principalService: SecurePrincipalsManager,
    private val searchService: SearchService,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val entitySetService: EntitySetManager
) : Upgrade {
    private val logger = LoggerFactory.getLogger(MigrateAppUsageSurveyData::class.java)

    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)
    private val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)
    private val entitySetIds = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).values.associateBy { it.name }

    private val studyEKIDByParticipantEKID: MutableMap<UUID, UUID> = mutableMapOf()
    private val participantIdByEKID: MutableMap<UUID, String?> = mutableMapOf()
    private val surveyDataByParticipantEKID: MutableMap<UUID, List<Map<FullQualifiedName, Set<Any?>>>> = mutableMapOf()
    private val studyIdByStudyEKID: MutableMap<UUID, UUID> = mutableMapOf()

    companion object {
        private val DATA_COLLECTION_APP_ID = UUID.fromString("c4e6d8fd-daf9-41e7-8c59-2a12c7ee0857")

        private val LEGACY_ORG_ID = UUID.fromString("7349c446-2acc-4d14-b2a9-a13be39cff93")

        private const val CHRONICLE_APP_ES_PREFIX = "chronicle_"
        private const val DATA_COLLECTION_APP_PREFIX = "chronicle_data_collection_"
        private const val USER_APPS = "userapps"
        private const val USED_BY = "usedby"
        private const val STUDIES = "studies"
        private const val PARTICIPANTS = "participants"
        private const val PARTICIPATED_IN = "participatedin"

        private const val LEGACY_USER_APPS_ES = "chronicle_user_apps"
        private const val LEGACY_STUDIES_ES = "chronicle_study"
        private const val LEGACY_PARTICIPATED_IN_ES = "chronicle_participated_in"
        private const val LEGACY_USED_BY_ES = "chronicle_used_by"

        private val OL_ID_FQN = EdmConstants.ID_FQN
        private val STRING_ID_FQN = FullQualifiedName("general.stringid")
        private val PERSON_FQN = FullQualifiedName("nc.SubjectIdentification")
        private val USER_FQN = FullQualifiedName("ol.user")
        private val FULL_NAME_FQN = FullQualifiedName("general.fullname")
        private val DATETIME_FQN = FullQualifiedName("ol.datetime")
        private val TIMEZONE_FQN = FullQualifiedName("ol.timezone")
        private val TITLE_FQN = FullQualifiedName("ol.title")

        // table column names
        private const val SUBMISSION_DATE = "submission_date"
        private const val APPLICATION_LABEL = "application_label"
        private const val APP_PACKAGE_NAME = "app_package_name"
        private const val EVENT_TIMESTAMP = "event_timestamp"
        private const val TIMEZONE = "timezone"
        private const val USERS = "users"
        private const val V2_STUDY_ID = "v2_study_id"
        private const val PARTICIPANT_ID = "participant_id"
        private const val V2_STUDY_EKID = "v2_study_ekid"

        private val FQN_TO_COLUMNS = mapOf(
            LAST_WRITE_FQN to SUBMISSION_DATE,
            TITLE_FQN to APPLICATION_LABEL,
            FULL_NAME_FQN to APP_PACKAGE_NAME,
            DATETIME_FQN to EVENT_TIMESTAMP,
            TIMEZONE_FQN to TIMEZONE,
            USER_FQN to USERS
        )

        private val column_names = listOf(V2_STUDY_EKID, V2_STUDY_ID, PARTICIPANT_ID) + FQN_TO_COLUMNS.values.toList()
        private val APP_USAGE_SURVEY_COLUMNS = column_names.joinToString(",") { it }
        private val APP_USAGE_SURVEY_PARAMS = column_names.joinToString(",") { "?" }

        private const val TABLE_NAME = "migrate_app_usage_survey"

        /**
         * PreparedStatement bind order
         * 1) studyEKID
         * 2) studyId
         * 3) participantId
         * 4) submissionDate,
         * 5) appLabel
         * 6) packageName
         * 7) timestamp
         * 8) timezone
         * 9) users
         */
        private val INSERT_INTO_APP_USAGE_SQL = """
            INSERT INTO $TABLE_NAME($APP_USAGE_SURVEY_COLUMNS) values ($APP_USAGE_SURVEY_PARAMS)
            ON CONFLICT DO NOTHING
        """.trimIndent()

        private val CREATE_APP_USAGE_SURVEY_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS $TABLE_NAME(
                $V2_STUDY_EKID uuid NOT NULL,
                $V2_STUDY_ID uuid NOT NULL,
                $PARTICIPANT_ID text NOT NULL,
                $SUBMISSION_DATE timestamp with time zone NOT NULL,
                $APPLICATION_LABEL text,
                $APP_PACKAGE_NAME text NOT NULL,
                $EVENT_TIMESTAMP timestamp with time zone NOT NULL,
                $TIMEZONE text,
                $USERS text[],
                PRIMARY KEY($APP_PACKAGE_NAME, $EVENT_TIMESTAMP)
            );
        """.trimIndent()
    }

    init {
        val hds = getDatasource()
        hds.connection.createStatement().use { stmt ->
            stmt.execute(CREATE_APP_USAGE_SURVEY_TABLE_SQL)
        }
    }

    override fun upgrade(): Boolean {
        getAllAppUsageSurveyData()

        logger.info("migrating app usage surveys to v3")

       val hds = getDatasource()
        try {
            val written = hds.connection.use { connection ->
                connection.prepareStatement(INSERT_INTO_APP_USAGE_SQL).use { ps ->
                    surveyDataByParticipantEKID.forEach { (key, data) ->
                        val studyEKID = studyEKIDByParticipantEKID[key]
                        val studyId = studyIdByStudyEKID[studyEKID]
                        val participantId = participantIdByEKID[key]

                        if (studyEKID == null || participantId == null || studyId == null) {
                            logger.warn("Skipping migration for participant $key. failed to retrieve associated studyId or participantId")
                            return@forEach
                        }

                        ps.setObject(1, studyEKID)
                        ps.setObject(2, studyId)
                        ps.setString(3, participantId)

                        data.forEach data@{ entity ->
                            val users = entity.getOrDefault(USER_FQN, listOf())
                                .map { it.toString() }.filter { it.isNotBlank() }.toList()

                            if (users.isEmpty() || users.first().toString().isBlank()) {
                                return@data
                            }

                            val submissionDate = getFirstValueOrNull(entity, LAST_WRITE_FQN)
                            val applicationLabels = getAllValuesOrNull(entity, TITLE_FQN)
                            val appPackageName = getFirstValueOrNull(entity, FULL_NAME_FQN)
                            val applicationLabel = when(applicationLabels.size) {
                                0 -> appPackageName
                                1 -> applicationLabels.first()
                                else -> applicationLabels.find { it != appPackageName }
                            }
                            val timezone = getFirstValueOrNull(entity, TIMEZONE_FQN)

                            if (submissionDate == null || appPackageName == null) {
                                return@data
                            }

                            val timestamps = entity[DATETIME_FQN]?.map { it.toString() }?.toSet() ?: setOf()
                            timestamps.forEach { timestamp ->
                                var index = 3
                                ps.setObject(++index, OffsetDateTime.parse(submissionDate))
                                ps.setString(++index, applicationLabel)
                                ps.setString(++index, appPackageName)
                                ps.setObject(++index, OffsetDateTime.parse(timestamp))
                                ps.setString(++index, timezone)
                                ps.setArray(++index, PostgresArrays.createTextArray(connection, users))
                                ps.addBatch()
                            }
                        }
                    }
                    ps.executeBatch().sum()
                }
            }

            logger.info("wrote {} entities to db. data query service returned {} entities", written, surveyDataByParticipantEKID.values.flatten().size)
            return true
        } catch (ex: Exception) {
            logger.error("error migrating app usage survey data to v3", ex)
            return false
        }
    }

    private fun getDatasource(): HikariDataSource {
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["chronicle"]!!
        val hc = HikariConfig(hikariConfiguration)
        return HikariDataSource(hc)
    }

    private fun getAllAppUsageSurveyData() {
        val v2DataCollectionOrgIds = appConfigs.keys
            .filter { it.appId == DATA_COLLECTION_APP_ID }
            .map { it.organizationId }
            .toMutableSet()

        val superUserPrincipals = getChronicleSuperUserPrincipals()

        (v2DataCollectionOrgIds + LEGACY_ORG_ID).forEach org@{ orgId ->
            logger.info("======================================")
            logger.info("getting app usage survey data for org $orgId")
            logger.info("=====================================")

            // entity sets
            val orgEntitySetIds = getOrgEntitySetIds(orgId)

            val studiesEntitySetId = orgEntitySetIds.getValue(STUDIES)
            val participatedInEntitySetId = orgEntitySetIds.getValue(PARTICIPATED_IN)
            val participantsEntitySetId = orgEntitySetIds[PARTICIPANTS] // this will be null if org is legacy
            val userAppsEntitySetId = orgEntitySetIds.getValue(USER_APPS)
            val usedByEntitySetId = orgEntitySetIds.getValue(USED_BY)

            val adminRoleAclKey = organizations[orgId]?.adminRoleAclKey
            if (adminRoleAclKey == null) {
                logger.warn("skipping {} since it doesn't have admin role", orgId)
                return@org
            }
            val principals = principalService.getAllUsersWithPrincipal(adminRoleAclKey).map { it.principal }.toSet() + superUserPrincipals

            val studies = filterInvalidStudies(getEntitiesByEntityKeyId(studiesEntitySetId))
            if (studies.isEmpty()) {
                logger.info("org {} has no studies. skipping", orgId)
                return@org
            }
            logger.info("found {} studies in org {}", studies.size, orgId)

            val studyEntityKeyIds = studies.keys
            val studyIdByStudyEKID = studies.mapValues { getFirstUUIDOrNull(it.value, STRING_ID_FQN)!! }
            val studyIds = studyIdByStudyEKID.values.toSet()

            val participantEntitySetIds = when (orgId) {
                LEGACY_ORG_ID -> getLegacyParticipantEntitySetIds(studyIds)
                else -> setOf(participantsEntitySetId!!)
            }

            val participants = getStudyParticipants(studiesEntitySetId, studyEntityKeyIds, participantEntitySetIds, participatedInEntitySetId, principals)

            val participantsByStudyEKID = participants.mapValues { (_, neighbor) -> neighbor.map { it.neighborId.get() }.toSet() }
            logger.info("org {} participant count by study {}", orgId, participantsByStudyEKID.mapValues { it.value.size })

            val participantIdByEKID =
                participants.values
                    .flatten()
                    .associate { getFirstUUIDOrNull(it.neighborDetails.get(), OL_ID_FQN)!! to getFirstValueOrNull(it.neighborDetails.get(), PERSON_FQN) }
            val studyEKIDByParticipantEKID = participantsByStudyEKID
                .map { (studyId, participants) -> participants.associateWith { studyId } }
                .flatMap {
                    it.asSequence()
                }.associate { it.key to it.value }

            val surveyDataByParticipantEKID = getSurveysDataByParticipant(
                principals, participantsByStudyEKID.values.flatten().toSet(), participantEntitySetIds, usedByEntitySetId, userAppsEntitySetId)

            logger.info("found {} survey entities in org {}", surveyDataByParticipantEKID.values.flatten().size, orgId)

            this.studyEKIDByParticipantEKID += studyEKIDByParticipantEKID
            this.participantIdByEKID += participantIdByEKID
            this.surveyDataByParticipantEKID += surveyDataByParticipantEKID
            this.studyIdByStudyEKID += studyIdByStudyEKID
        }
    }

    private fun getChronicleSuperUserPrincipals(): Set<Principal> {
        val userId = "auth0|5ae9026c04eb0b243f1d2bb6"
        val securablePrincipal = principalService.getSecurablePrincipal(userId)
        return principalService.getAllPrincipals(securablePrincipal).map { it.principal }.toSet() + Principal(PrincipalType.USER, userId)
    }

    private fun filterInvalidStudies(entities: Map<UUID, Map<FullQualifiedName, Set<Any>>>): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        return entities.filterValues { getFirstUUIDOrNull(it, STRING_ID_FQN) != null }
    }

    // returns a map of collection template name to entity set id
    private fun getOrgEntitySetIds(organizationId: UUID): Map<String, UUID> {
        val entitySetNameByTemplateName = when (organizationId) {
            LEGACY_ORG_ID -> mapOf(
                USER_APPS to LEGACY_USER_APPS_ES,
                USED_BY to LEGACY_USED_BY_ES,
                STUDIES to LEGACY_STUDIES_ES,
                PARTICIPATED_IN to LEGACY_PARTICIPATED_IN_ES
            )
            else -> {
                val orgIdToStr = organizationId.toString().replace("-", "")
                mapOf(
                    USER_APPS to "$DATA_COLLECTION_APP_PREFIX${orgIdToStr}_$USER_APPS",
                    USED_BY to "$DATA_COLLECTION_APP_PREFIX${orgIdToStr}_$USED_BY",
                    STUDIES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$STUDIES",
                    PARTICIPATED_IN to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$PARTICIPATED_IN",
                    PARTICIPANTS to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_${PARTICIPANTS}"
                )
            }
        }

        return entitySetNameByTemplateName.mapValues { entitySetIds.getValue(it.value).id }
    }

    private fun getLegacyParticipantEntitySetIds(studyIds: Set<UUID>): Set<UUID> {
        val entitySetNames = studyIds.map { "chronicle_participants_$it" }
        return entitySetNames.mapNotNull { entitySetIds[it]?.id }.toSet()
    }

    private fun getStudyParticipants(
        studiesEntitySetId: UUID,
        studyEntityKeyIds: Set<UUID>,
        participantEntitySetIds: Set<UUID>,
        participatedInEntitySetId: UUID,
        principals: Set<Principal>

    ): Map<UUID, MutableList<NeighborEntityDetails>> {
        val filter = EntityNeighborsFilter(
            studyEntityKeyIds, Optional.of(participantEntitySetIds), Optional.of(setOf(studiesEntitySetId)), Optional.of(setOf(participatedInEntitySetId)))
        return searchService.executeEntityNeighborSearch(setOf(studiesEntitySetId), PagedNeighborRequest(filter), principals).neighbors
    }

    private fun getSurveysDataByParticipant(
        principals: Set<Principal>,
        entityKeyIds: Set<UUID>,
        participantEntitySetIds: Set<UUID>,
        usedByEnEntitySetId: UUID,
        userAppsEntitySetId: UUID): Map<UUID, List<Map<FullQualifiedName, Set<Any?>>>> {

        val filter = EntityNeighborsFilter(entityKeyIds, Optional.of(setOf(userAppsEntitySetId)), Optional.of(participantEntitySetIds), Optional.of(setOf(usedByEnEntitySetId)))

        val neighbors = searchService
            .executeEntityNeighborSearch(participantEntitySetIds, PagedNeighborRequest(filter), principals).neighbors
            .mapValues { (_, v) ->
                v.associateBy { getFirstUUIDOrNull(it.associationDetails, OL_ID_FQN)!! }
            }

        val associationEntityKeyIds = neighbors.values.flatMap { it.keys }.toSet()
        val usedByEntities = getEntitiesByEntityKeyId(usedByEnEntitySetId, associationEntityKeyIds, setOf(MetadataOption.LAST_WRITE))

        return neighbors
            .mapValues { (_, v) ->
                v.map {
                    it.value.neighborDetails.get().toMutableMap() + usedByEntities.getOrDefault(it.key, mapOf())
                }
            }
    }

    private fun getFirstUUIDOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): UUID? {
        return when (val string = getFirstValueOrNull(entity, fqn)) {
            null -> null
            else -> UUID.fromString(string)
        }
    }

    private fun getAllValuesOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): Set<String> {
        entity[fqn]?.let { it ->
            return it.mapNotNull { it.toString() }.toSet()
        }
        return setOf()
    }

    private fun getFirstValueOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): String? {
        entity[fqn]?.iterator()?.let {
            if (it.hasNext()) return it.next().toString()
        }
        return null
    }

    private fun getEntitiesByEntityKeyId(
        entitySetId: UUID,
        entityKeyIds: Set<UUID> = setOf(),
        metadataOptions: Set<MetadataOption> = setOf()
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
            mapOf(entitySetId to Optional.of(entityKeyIds)),
            entitySetService.getPropertyTypesOfEntitySets(setOf(entitySetId)),
            mapOf(),
            metadataOptions,
            Optional.empty(),
            false,
        )
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
