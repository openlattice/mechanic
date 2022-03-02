package com.openlattice.mechanic.upgrades

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EdmConstants
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
class MigrateTimeUseDiarySubmissions(
    val toolbox: Toolbox,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val entitySetService: EntitySetManager,
    private val searchService: SearchService,
    private val principalService: SecurePrincipalsManager
) : Upgrade {

    private val entitySetIds: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).associate { it.value.name to it.key }
    private val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(MigrateTimeUseDiarySubmissions::class.java)
        private val mapper = ObjectMappers.getJsonMapper()

        private const val SUPER_USER_PRINCIPAL_ID = "auth0|5ae9026c04eb0b243f1d2bb6"

        private val SURVEYS_APP_ID = UUID.fromString("bb44218b-515a-4314-b955-df2c991b2575")

        private const val CHRONICLE_APP_ES_PREFIX = "chronicle_"
        private const val SURVEYS_APP_ES_PREFIX = "chronicle_surveys_"

        // collection template names
        private const val STUDIES_TEMPLATE = "studies"
        private const val PARTICIPATED_IN_TEMPLATE = "participatedin"
        private const val PARTICIPANTS_TEMPLATE = "participants"
        private const val SUBMISSION_TEMPLATE = "submission"
        private const val QUESTION_TEMPLATE = "question"
        private const val ANSWER_TEMPLATE = "answer"
        private const val TIME_RANGE_TEMPLATE = "timerange"
        private const val REGISTERED_FOR_TEMPLATE = "registeredfor"
        private const val RESPONDS_WITH_TEMPLATE = "respondswith"
        private const val ADDRESSES_TEMPLATE = "addresses"

        // entity sets lookup name
        private const val STUDIES_ES = "studies"
        private const val PARTICIPATED_IN_ES = "participatedIn"
        private const val PARTICIPANTS_ES = "participants"
        private const val TIME_RANGE_ES = "timeRange"
        private const val SUBMISSION_ES = "submission"
        private const val QUESTION_ES = "question"
        private const val ANSWER_ES = "answer"
        private const val REGISTERED_FOR_ES = "registeredFor"
        private const val RESPONDED_WITH_ES = "respondedWith"
        private const val ADDRESSES_ES = "addresses"

        private val OL_ID_FQN = EdmConstants.ID_FQN
        private val STRING_ID_FQN = FullQualifiedName("general.stringid")
        private val PERSON_FQN = FullQualifiedName("nc.SubjectIdentification")
        private val TITLE_FQN = FullQualifiedName("ol.title") // question title
        private val DATE_TIME_START_FQN = FullQualifiedName("ol.datetimestart") //timerange
        private val DATE_TIME_END_FQN = FullQualifiedName("ol.datetimeend")
        private val VALUES_FQN = FullQualifiedName("ol.values")
        private val FULL_NAME_FQN = FullQualifiedName("general.fullname")
        private val DATE_TIME_FQN = FullQualifiedName("ol.datetime")
        private val ID_FQN = FullQualifiedName("ol.id") // question code

        // column names
        private const val STUDY_ID = "study_id"
        private const val PARTICIPANT_ID = "participant_id"
        private const val ORGANIZATION_ID = "organization_id"
        private const val SUBMISSION = "submission"
        private const val SUBMISSION_ID = "submission_id" //not unique for each row
        private const val SUBMISSION_DATE = "submission_date"

        private const val TABLE_NAME = "time_use_diary_v2"

        private val COLUMNS = linkedSetOf(
            STUDY_ID,
            ORGANIZATION_ID,
            PARTICIPANT_ID,
            SUBMISSION_ID,
            SUBMISSION,
            SUBMISSION_DATE
        ).joinToString { it }

        private val CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS $TABLE_NAME(
                $STUDY_ID uuid not null,
                $ORGANIZATION_ID uuid not null,
                $SUBMISSION_ID uuid not null,
                $PARTICIPANT_ID uuid not null,
                $SUBMISSION jsonb not null,
                $SUBMISSION_DATE  timestamp with time zone not null,
                PRIMARY KEY($SUBMISSION_ID, $SUBMISSION_DATE)
            )
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId,
         * 2) orgId
         * 3) participantId
         * 4) submissionId
         * 5) submission
         * 6) submissionDate
         */
        private val INSERT_INTO_TABLE_SQL = """
            INSERT INTO $TABLE_NAME ($COLUMNS) VALUES (?, ?, ?, ?, ?::jsonb, ?)
        """.trimIndent()
    }

    init {
        getHikariDataSource().connection.createStatement().use { statement ->
            statement.execute(CREATE_TABLE_SQL)
        }
    }

    private fun getHikariDataSource(): HikariDataSource {
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["chronicle"]!!
        val hc = HikariConfig(hikariConfiguration)
        return HikariDataSource(hc)
    }

    private fun getOrgEntitySetNames(orgId: UUID): Map<String, UUID> {
        val orgIdToStr = orgId.toString().replace("-", "")
        val entitySetNameByTemplateName = mapOf(
            STUDIES_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$STUDIES_TEMPLATE",
            PARTICIPATED_IN_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$PARTICIPATED_IN_TEMPLATE",
            PARTICIPANTS_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_${PARTICIPANTS_TEMPLATE}",
            ADDRESSES_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${ADDRESSES_TEMPLATE}",
            RESPONDED_WITH_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${RESPONDS_WITH_TEMPLATE}",
            REGISTERED_FOR_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${REGISTERED_FOR_TEMPLATE}",
            ANSWER_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${ANSWER_TEMPLATE}",
            QUESTION_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${QUESTION_TEMPLATE}",
            SUBMISSION_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${SUBMISSION_TEMPLATE}",
            TIME_RANGE_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${TIME_RANGE_TEMPLATE}"
        )

        return entitySetNameByTemplateName.filter { entitySetIds.keys.contains(it.value) }.mapValues { entitySetIds.getValue(it.value) }
    }

    private fun getChronicleSuperUserPrincipals(): Set<Principal> {
        val securablePrincipal = principalService.getSecurablePrincipal(SUPER_USER_PRINCIPAL_ID)
        return principalService.getAllPrincipals(securablePrincipal).map { it.principal }.toSet() + Principal(PrincipalType.USER, SUPER_USER_PRINCIPAL_ID)
    }

    private fun processOrganization(orgId: UUID, principals: Set<Principal>): List<SubmissionEntity> {
        logger.info("Processing org $orgId")

        // get studies
        val entitySets = getOrgEntitySetNames(orgId)
        val studies: Map<UUID, Study> = getOrgStudies(entitySetId = entitySets.getValue(STUDIES_ES))
        if (studies.isEmpty()) {
            logger.info("organization $orgId has no studies. Skipping")
            return listOf()
        }
        logger.info("Retrieved ${studies.size} studies")

        // study -> participants
        val participants: Map<UUID, Set<Participant>> = getOrgParticipants(
            participantEntitySetIds = setOf(entitySets.getValue(PARTICIPANTS_ES)),
            studiesEntitySetId = entitySets.getValue(STUDIES_ES),
            entityKeyIds = studies.keys,
            principals = principals,
            edgeEntitySetId = entitySets.getValue(PARTICIPATED_IN_ES)
        ).toMutableMap()

        if (participants.values.flatten().isEmpty()) {
            logger.info("No participants found in $orgId")
            return listOf()
        }

        // participant -> neighbor entity set id -> [neighbors]
        val participantNeighbors: Map<UUID, Map<UUID, List<NeighborEntityDetails>>> = getParticipantNeighbors(
            entityKeyIds = participants.values.flatten().map { it.id }.toSet(),
            entitySetIds = entitySetIds,
            principals = principals
        )

        // unique submission. Each submission entity is an entry in the tud submissions table
        val submissionsById = participantNeighbors
            .asSequence()
            .filter { it.value.keys.contains(entitySets.getValue(SUBMISSION_ES)) }
            .map { it.value.values.flatten() }
            .flatten().associateBy { it.neighborId.get() }

        val answersById = participantNeighbors.values
            .asSequence()
            .filter { it.keys.contains(entitySets.getValue(ANSWER_ES)) }
            .map { it.values.flatten() }
            .flatten().associateBy { it.neighborId.get() }

        // answerId -> neighbor esid -> [neighbors]
        val answerNeighbors = getAnswerNeighbors(
            entityKeyIds = answersById.keys,
            entitySetIds = entitySets,
            principals = principals
        )

        // submissionId -> [answer]
        val answersBySubmissionId = getAnswersBySubmissionId(
            entityKeyIds = submissionsById.keys,
            entitySetIds = entitySets,
            principals = principals
        )

        val participantBySubmissionId = participantNeighbors
            .filter { it.value.keys.contains(entitySets.getValue(SUBMISSION_ES)) }
            .mapValues { it.value.values.flatten().associate { neighbor -> neighbor.neighborId.get() to it.key } }
            .values.first()

        val participantsById = participants.values.flatten().associateBy { it.id }
        val studiesById = studies.values.associateBy { it.studyEntityKeyId }


        return answersBySubmissionId.map { (submissionId, answerEntities) ->
            getSubmissionEntity(
                orgId,
                submissionId,
                answerEntities,
                participantsById,
                participantBySubmissionId,
                studiesById,
                answerNeighbors.mapValues { answers -> answers.value.mapValues { neighbors -> neighbors.value.first() } },
                submissionsById.getValue(submissionId).neighborDetails.get(),
                entitySetIds
            )
        }
    }

    private fun getSubmissionEntity(
        orgId: UUID,
        submissionId: UUID,
        answers: List<NeighborEntityDetails>,
        participantsById: Map<UUID, Participant>,
        participantIdBySubmissionId: Map<UUID, UUID>,
        studiesById: Map<UUID, Study>,
        answerNeighbors: Map<UUID, Map<UUID, NeighborEntityDetails>>,
        submissionEntity: Map<FullQualifiedName, Set<Any>>,
        entitySetIds: Map<String, UUID>
    ): SubmissionEntity {
        val dateSubmitted = getFirstValueOrNull(submissionEntity, DATE_TIME_FQN)

        val participantId = participantIdBySubmissionId.getValue(submissionId)
        val participant = participantsById.getValue(participantId)

        val responses = answers.map { answer ->
            getResponse(
                answerId = answer.neighborId.get(),
                answerEntity = answer.neighborDetails.get(),
                answerNeighbors = answerNeighbors,
                entitySetIds = entitySetIds
            )
        }
        return SubmissionEntity(
            orgId = orgId,
            submissionId = submissionId,
            date = dateSubmitted?.let { OffsetDateTime.parse(it) },
            studyId = studiesById.getValue(participant.studyEntityKeyId).studyId,
            participantId = participant.participantId,
            responses = responses.filter { it.code != null && it.question != null }.toSet()
        )
    }


    private fun getResponse(
        answerId: UUID,
        answerEntity: Map<FullQualifiedName, Set<Any>>,
        answerNeighbors: Map<UUID, Map<UUID, NeighborEntityDetails>>,
        entitySetIds: Map<String, UUID>
    ): ResponseEntity {

        val responses = getAllValuesOrNull(answerEntity, VALUES_FQN)
        val questionEntity = answerNeighbors.getValue(answerId).getValue(entitySetIds.getValue(QUESTION_ES))
        // time range is optional
        val timeRangeEntity = answerNeighbors.getValue(answerId)[entitySetIds.getValue(TIME_RANGE_ES)]
        val startDateTime = timeRangeEntity?.let { getFirstValueOrNull(it.neighborDetails.get(), DATE_TIME_START_FQN) }
        val endDateTime = timeRangeEntity?.let { getFirstValueOrNull(it.neighborDetails.get(), DATE_TIME_END_FQN) }


        return ResponseEntity(
            code = getFirstValueOrNull(questionEntity.neighborDetails.get(), ID_FQN),
            question = getFirstValueOrNull(questionEntity.neighborDetails.get(), TITLE_FQN),
            response = responses,
            startDateTime = startDateTime?.let { OffsetDateTime.parse(it) },
            endDateTime = endDateTime?.let { OffsetDateTime.parse(it) }
        )
    }

    private fun getAnswersBySubmissionId(
        entityKeyIds: Set<UUID>,
        entitySetIds: Map<String, UUID>,
        principals: Set<Principal>
    ): Map<UUID, MutableList<NeighborEntityDetails>> {
        val registeredForEntitySetId = entitySetIds.getValue(REGISTERED_FOR_ES)
        val answerEntitySetId = entitySetIds.getValue(ANSWER_ES)

        val filter = EntityNeighborsFilter(
            entityKeyIds,
            Optional.of(setOf(answerEntitySetId)),
            Optional.empty(),
            Optional.of(setOf(registeredForEntitySetId))
        )

        return searchService.executeEntityNeighborSearch(
            setOf(entitySetIds.getValue(SUBMISSION_ES)),
            PagedNeighborRequest(filter),
            principals
        ).neighbors
    }

    private fun getAnswerNeighbors(
        entityKeyIds: Set<UUID>,
        entitySetIds: Map<String, UUID>,
        principals: Set<Principal>
    ): Map<UUID, Map<UUID, List<NeighborEntityDetails>>> {
        val registeredForEntitySetId = entitySetIds.getValue(REGISTERED_FOR_ES)
        val submissionEntitySetId = entitySetIds.getValue(SUBMISSION_ES)
        val timeRangeEntitySetId = entitySetIds.getValue(TIME_RANGE_ES)
        val questionEntitySetId = entitySetIds.getValue(QUESTION_ES)
        val addressesEntitySetId = entitySetIds.getValue(ADDRESSES_ES)

        val filter = EntityNeighborsFilter(
            entityKeyIds,
            Optional.empty(),
            Optional.of(setOf(submissionEntitySetId, timeRangeEntitySetId, questionEntitySetId)),
            Optional.of(setOf(registeredForEntitySetId, addressesEntitySetId))
        )
        return searchService.executeEntityNeighborSearch(
            setOf(entitySetIds.getValue(ANSWER_ES)),
            PagedNeighborRequest(filter),
            principals
        ).neighbors.mapValues { it.value.groupBy { neighbors -> neighbors.neighborEntitySet.get().id } }
    }

    private fun getParticipantNeighbors(
        entityKeyIds: Set<UUID>,
        entitySetIds: Map<String, UUID>,
        principals: Set<Principal>
    ): Map<UUID, Map<UUID, List<NeighborEntityDetails>>> {
        val submissionEntitySetId = entitySetIds.getValue(SUBMISSION_ES)
        val respondsWithEntitySetId = entitySetIds.getValue(RESPONDED_WITH_ES)
        val answerEntitySetId = entitySetIds.getValue(ANSWER_ES)

        val filter = EntityNeighborsFilter(
            entityKeyIds,
            Optional.empty(),
            Optional.of(setOf(submissionEntitySetId, answerEntitySetId)),
            Optional.of(setOf(respondsWithEntitySetId))
        )
        return searchService.executeEntityNeighborSearch(
            setOf(entitySetIds.getValue(PARTICIPANTS_ES)),
            PagedNeighborRequest(filter),
            principals
        ).neighbors.mapValues { it.value.groupBy { neighbors -> neighbors.neighborEntitySet.get().id } }
    }

    private fun getOrgStudies(entitySetId: UUID): Map<UUID, Study> {
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
            mapOf(entitySetId to Optional.empty()),
            entitySetService.getPropertyTypesOfEntitySets(setOf(entitySetId)),
            mapOf(),
            setOf(),
            Optional.empty(),
            false
        )
            .filter { getFirstUUIDOrNull(it.value, STRING_ID_FQN) != null }
            .mapValues { getStudyEntity(it.key, it.value) }
    }

    private fun getStudyEntity(studyEntityKeyId: UUID, entity: Map<FullQualifiedName, Set<Any>>): Study {
        val title = getFirstValueOrNull(entity, FULL_NAME_FQN)
        val studyId = getFirstUUIDOrNull(entity, STRING_ID_FQN)
        return Study(studyEntityKeyId, studyId!!, title)
    }

    // Returns a mapping from studyEntityKeyId to list of participants
    private fun getOrgParticipants(
        participantEntitySetIds: Set<UUID>,
        edgeEntitySetId: UUID,
        studiesEntitySetId: UUID,
        entityKeyIds: Set<UUID>,
        principals: Set<Principal>
    )
        : Map<UUID, Set<Participant>> {
        val filter = EntityNeighborsFilter(entityKeyIds, Optional.of(participantEntitySetIds), Optional.empty(), Optional.of(setOf(edgeEntitySetId)))

        return searchService
            .executeEntityNeighborSearch(setOf(studiesEntitySetId), PagedNeighborRequest(filter), principals)
            .neighbors
            .mapValues { it.value.map { neighbor -> getParticipantFromNeighborEntity(it.key, neighbor) }.toSet() }

    }

    private fun getParticipantFromNeighborEntity(studyEntityKeyId: UUID, entity: NeighborEntityDetails): Participant {
        val id = getFirstUUIDOrNull(entity.neighborDetails.get(), OL_ID_FQN)
        val participantId = getFirstValueOrNull(entity.neighborDetails.get(), PERSON_FQN)

        return Participant(studyEntityKeyId, id!!, participantId!!) // hope this force unwrapping doesn't throw NPE

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

    private fun writeEntitiesToTable(entities: List<SubmissionEntity>): Int {
        val hds = getHikariDataSource()

        return hds.connection.use { connection ->
            try {
                val wc = connection.prepareStatement(INSERT_INTO_TABLE_SQL).use { ps ->
                    entities.forEach {
                        var index = 0
                        ps.setObject(++index, it.studyId)
                        ps.setObject(++index, it.orgId)
                        ps.setString(++index, it.participantId)
                        ps.setObject(++index, it.submissionId)
                        ps.setString(++index, mapper.writeValueAsString(it.responses))
                        ps.setObject(++index, it.date)
                        ps.addBatch()
                    }
                    ps.executeBatch().sum()
                }
                return@use wc
            } catch (ex: Exception) {
                return 0
            }

        }
    }

    override fun upgrade(): Boolean {
        val superUserPrincipals = getChronicleSuperUserPrincipals()

        val orgIds = appConfigs.keys.filter { it.appId == SURVEYS_APP_ID }.map { it.organizationId }.toSet()
        val entities = orgIds.map { processOrganization(it, superUserPrincipals) }.flatten()
        val written = writeEntitiesToTable(entities)
        logger.info("Exported $written entities to $TABLE_NAME")
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}

private data class Study(
    val studyEntityKeyId: UUID,
    val studyId: UUID,
    val title: String?
)

data class Participant(
    val studyEntityKeyId: UUID,
    val id: UUID,
    val participantId: String,
)

data class ResponseEntity(
    val code: String?,
    val question: String?,
    val response: Set<String>,
    val startDateTime: OffsetDateTime?,
    val endDateTime: OffsetDateTime?
)

data class SubmissionEntity(
    val orgId: UUID,
    val studyId: UUID,
    val submissionId: UUID,
    val date: OffsetDateTime?,
    val participantId: String,
    val responses: Set<ResponseEntity>
)