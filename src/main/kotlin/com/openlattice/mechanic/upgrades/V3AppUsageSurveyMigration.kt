package com.openlattice.mechanic.upgrades

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.util.log
import com.openlattice.authorization.Principal
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
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class V3AppUsageSurveyMigration(
    toolbox: Toolbox,
    private val hds: HikariDataSource,
    private val principalService: SecurePrincipalsManager,
    private val searchService: SearchService,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val entitySetService: EntitySetManager
) : Upgrade {
    private val logger = LoggerFactory.getLogger(V3AppUsageSurveyMigration::class.java)

    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)
    private val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)
    private val entitySetIds = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).values.associateBy { it.name }

    private val allStudyIdsByParticipantEKID: MutableMap<UUID, UUID> = mutableMapOf()
    private val allParticipantIdsByEKID: MutableMap<UUID, String?> = mutableMapOf()
    private val allSurveyDataByParticipantEKID: MutableMap<UUID, List<Map<FullQualifiedName, Set<Any?>>>> = mutableMapOf()

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

        private val FQN_TO_COLUMNS = mapOf(
            LAST_WRITE_FQN to "submission_date",
            TITLE_FQN to "application_label",
            FULL_NAME_FQN to "app_package_name",
            DATETIME_FQN to "event_timestamp",
            TIMEZONE_FQN to "timezone",
            USER_FQN to "users"
        )

        private val column_names = listOf("study_id", "participant_id") + FQN_TO_COLUMNS.values.toList()
        private val APP_USAGE_SURVEY_COLUMNS = column_names.joinToString(",") { it }
        private val APP_USAGE_SURVEY_PARAMS = column_names.joinToString(",") { "?" }

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) participantId
         * 3) submissionDate,
         * 4) appLabel
         * 5) packageName
         * 6) timestamp
         * 7) timezone
         * 8) users
         */
        private val INSERT_INTO_APP_USAGE_SQL = """
            INSERT INTO app_usage_survey($APP_USAGE_SURVEY_COLUMNS) values ($APP_USAGE_SURVEY_PARAMS)
            ON CONFLICT DO NOTHING
        """.trimIndent()

    }

    override fun upgrade(): Boolean {
        getAllAppUsageSurveyData()

        logger.info("migrating app usage surveys to v3")

        try {
            val written = hds.connection.use { connection ->
                connection.prepareStatement(INSERT_INTO_APP_USAGE_SQL).use { ps ->
                    allSurveyDataByParticipantEKID.forEach { (key, data) ->
                        val studyId = allStudyIdsByParticipantEKID[key]
                        val participantId = allParticipantIdsByEKID[key]

                        if (studyId == null || participantId == null) {
                            logger.warn("Skipping migration for participant $key. failed to retrieve associated studyId or participantId")
                            return@forEach
                        }

                        ps.setObject(1, studyId)
                        ps.setString(2, participantId)

                        data.forEach data@{ entity ->
                            val users =  entity.getOrDefault(USER_FQN, listOf())
                                .map { it.toString() }.filter { it.isNotBlank() }.toList()

                            if (users.isEmpty() || users.first().toString().isBlank()) {
                                return@data
                            }

                            val submissionDate = getFirstValueOrNull(entity, LAST_WRITE_FQN)
                            val applicationLabel = getFirstValueOrNull(entity, TITLE_FQN)
                            val appPackageName = getFirstValueOrNull(entity, FULL_NAME_FQN)
                            val timezone = getFirstValueOrNull(entity, TIMEZONE_FQN)
                            val timestamp = getFirstValueOrNull(entity, DATETIME_FQN)

                            if (submissionDate == null || appPackageName == null || timestamp == null) {
                                return@data
                            }

                            ps.setObject(3, OffsetDateTime.parse(submissionDate))
                            ps.setString(4, applicationLabel)
                            ps.setString(5, appPackageName)
                            ps.setObject(6, OffsetDateTime.parse(timestamp))
                            ps.setString(7, timezone)
                            ps.setArray(8, PostgresArrays.createTextArray(connection, users))
                            ps.addBatch()
                        }
                    }
                    ps.executeBatch().sum()
                }
            }

            logger.info("wrote {} entities to db. data query service returned {} entities", written, allSurveyDataByParticipantEKID.values.flatten().size)
            return true
        } catch (ex: Exception) {
            logger.error("error migrating app usage survey data to v3", ex)
            return false
        }
    }

    private fun getAllAppUsageSurveyData() {
        val v2DataCollectionOrgIds = appConfigs.keys
            .filter { it.appId == DATA_COLLECTION_APP_ID }
            .map { it.organizationId }
            .toMutableSet()

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
            val adminPrincipals = principalService.getAllUsersWithPrincipal(adminRoleAclKey).map { it.principal }.toSet()

            val studies = filter(getEntitiesByEntityKeyId(studiesEntitySetId))
            if (studies.isEmpty()) {
                logger.info("org {} has no studies. skipping", orgId)
                return@org
            }
            logger.info("found {} studies in org {}", studies.size, orgId)

            val studyEntityKeyIds = studies.keys
            val studyIds = studies.map { getFirstUUIDOrNull(it.value, STRING_ID_FQN) }.filterNotNull().toSet()

            val participantEntitySetIds = when (orgId) {
                LEGACY_ORG_ID -> getLegacyParticipantEntitySetIds(studyIds)
                else -> setOf(participantsEntitySetId!!)
            }
            val participants = getStudyParticipants(studiesEntitySetId, studyEntityKeyIds, participantEntitySetIds, participatedInEntitySetId, adminPrincipals)

            val participantsByStudyId = participants.mapValues { (_, neighbor) -> neighbor.map { it.neighborId.get() }.toSet() }
            logger.info("org {} participant count by study {}", orgId, participantsByStudyId.mapValues { it.value.size })

            val participantIdByEKID =
                participants.values
                    .flatten()
                    .associate { getFirstUUIDOrNull(it.neighborDetails.get(), OL_ID_FQN)!! to getFirstValueOrNull(it.neighborDetails.get(), PERSON_FQN) }
            val studyIdByParticipantEKID = participantsByStudyId
                .map { (studyId, participants) -> participants.associateWith { studyId } }
                .flatMap {
                    it.asSequence()
                }.associate { it.key to it.value }

            val surveysDataByParticipant = getSurveysDataByParticipant(
                adminPrincipals, participantsByStudyId.values.flatten().toSet(), participantEntitySetIds, usedByEntitySetId, userAppsEntitySetId)

            logger.info("found {} survey entities in org {}", surveysDataByParticipant.values.size, orgId)

            allStudyIdsByParticipantEKID += studyIdByParticipantEKID
            allParticipantIdsByEKID += participantIdByEKID
            allSurveyDataByParticipantEKID += surveysDataByParticipant
        }
    }

    private fun filter(entities: Map<UUID, Map<FullQualifiedName, Set<Any>>>): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
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
        return entitySetNames.map { entitySetIds.getValue(it).id }.toSet()
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
