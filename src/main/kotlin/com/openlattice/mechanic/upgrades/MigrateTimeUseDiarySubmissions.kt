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
    private val principalService: SecurePrincipalsManager,
) : Upgrade {

    private val entitySetIds: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).associate { it.value.name to it.key }
    private val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)

    var totalSubmissionEntities: Int = 0 //keep track of number of submission entities

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

        private const val TABLE_NAME = "migrate_time_use_diary"

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
                $PARTICIPANT_ID text not null,
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


        val orgEntitySetIds = getOrgEntitySetNames(orgId)

        // get all participants in studies
        val participants: Set<Participant> = getOrgParticipants(
            entitySetIds = orgEntitySetIds,
        )

        if (participants.isEmpty()) {
            logger.info("No participants found in org $orgId")
            return listOf()
        }

        // participant -> neighbor entity set id -> [neighbors]
        val participantNeighbors: Map<UUID, Map<UUID, List<NeighborEntityDetails>>> = getParticipantNeighbors(
            entityKeyIds = participants.map { it.id }.toSet(),
            entitySetIds = orgEntitySetIds,
            principals = principals
        )

        val studiesByParticipantId: Map<UUID, Study> = participantNeighbors
            .mapValues { it.value.getOrDefault(orgEntitySetIds.getValue(STUDIES_ES), listOf()).first() }
            .mapValues { getStudyEntity(it.value.neighborId.get(), it.value.neighborDetails.get()) }
        logger.info("Org studies: ${studiesByParticipantId.values.toSet()}")

        // unique submission. Each submission entity is an entry in the tud submissions table
        val submissionsById = participantNeighbors
            .values.map { it.getOrDefault(orgEntitySetIds.getValue(SUBMISSION_ES), listOf()) }.flatten().associateBy { it.neighborId.get() }
        totalSubmissionEntities += submissionsById.keys.size

        if (submissionsById.isEmpty()) {
            logger.info("no submissions found")
            return listOf()
        }

        val answersById = participantNeighbors
            .values.map { it.getOrDefault(orgEntitySetIds.getValue(ANSWER_ES), listOf()) }.flatten().associateBy { it.neighborId.get() }
        if (answersById.isEmpty()) {
            logger.warn("unexpected. submission should have answer entities")
            return listOf()
        }

        // answerId -> neighbor esid -> [neighbors]
        val answerNeighbors = getAnswerNeighbors(
            entityKeyIds = answersById.keys,
            entitySetIds = orgEntitySetIds,
            principals = principals
        )

        // submissionId -> [answer]
        val answersBySubmissionId = getAnswersBySubmissionId(
            entityKeyIds = submissionsById.keys,
            entitySetIds = orgEntitySetIds,
            principals = principals
        )

        val participantBySubmissionId = participantNeighbors
            .map { it.value.getOrDefault(orgEntitySetIds.getValue(SUBMISSION_ES), setOf()).associate { neighbor -> neighbor.neighborId.get() to it.key } }
            .asSequence()
            .flatMap { it.asSequence() }
            .groupBy({ it.key }, { it.value })
            .mapValues { it.value.first() }

        return answersBySubmissionId.map { (submissionId, answerEntities) ->
            getSubmissionEntity(
                orgId = orgId,
                submissionId = submissionId,
                answerEntities = answerEntities,
                participantsById = participants.associateBy { it.id },
                studiesByParticipantId = studiesByParticipantId,
                participantBySubmissionId = participantBySubmissionId,
                answerNeighbors = answerNeighbors.mapValues { answers -> answers.value.mapValues { neighbors -> neighbors.value.first() } },
                submissionEntity = submissionsById.getValue(submissionId).neighborDetails.get(),
                entitySetIds = orgEntitySetIds
            )
        }
    }

    private fun getSubmissionEntity(
        orgId: UUID,
        submissionId: UUID,
        answerEntities: List<NeighborEntityDetails>,
        participantsById: Map<UUID, Participant>,
        studiesByParticipantId: Map<UUID, Study>,
        participantBySubmissionId: Map<UUID, UUID>,
        answerNeighbors: Map<UUID, Map<UUID, NeighborEntityDetails>>,
        submissionEntity: Map<FullQualifiedName, Set<Any>>,
        entitySetIds: Map<String, UUID>
    ): SubmissionEntity {
        val dateSubmitted = getFirstValueOrNull(submissionEntity, DATE_TIME_FQN)

        val participantId = participantBySubmissionId.getValue(submissionId)
        val participant = participantsById.getValue(participantId)

        val responses = answerEntities.map { answer ->
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
            studyId = studiesByParticipantId.getValue(participantId).studyId,
            participantId = participant.participantId!!, //force unwrapping is safe because we have already filtered out "bad" participant entities
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
        val participatedInEntitySetId = entitySetIds.getValue(PARTICIPATED_IN_ES)
        val studiesEntitySetId = entitySetIds.getValue(STUDIES_ES)

        val filter = EntityNeighborsFilter(
            entityKeyIds,
            Optional.empty(),
            Optional.of(setOf(submissionEntitySetId, answerEntitySetId, studiesEntitySetId)),
            Optional.of(setOf(respondsWithEntitySetId, participatedInEntitySetId))
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
        return Study(studyEntityKeyId, studyId!!, title = title)
    }

    // Returns a mapping from studyEntityKeyId to list of participants
    private fun getOrgParticipants(
        entitySetIds: Map<String, UUID>,
    ): Set<Participant> {
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
            mapOf(entitySetIds.getValue(PARTICIPANTS_ES) to Optional.empty()),
            entitySetService.getPropertyTypesOfEntitySets(setOf(entitySetIds.getValue(PARTICIPANTS_ES))),
            mapOf(),
            setOf(),
            Optional.empty(),
            false
        ).mapValues { getParticipantEntity(it.key, it.value) }.values.filter { it.participantId != null }.toSet()

    }

    private fun getParticipantEntity(entityKeyId: UUID, entity: Map<FullQualifiedName, Set<Any>>): Participant {
        val participantId = getFirstValueOrNull(entity, PERSON_FQN)
        return Participant(
            id = entityKeyId,
            participantId = participantId,
            studyEntityKeyId = entityKeyId
        )
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
                throw ex
            }

        }
    }

    override fun upgrade(): Boolean {
        val superUserPrincipals = getChronicleSuperUserPrincipals()

        val orgIds = appConfigs.keys.filter { it.appId == SURVEYS_APP_ID }.map { it.organizationId }.toSet()
        val entities = orgIds.map { processOrganization(it, superUserPrincipals) }.flatten()
        val written = writeEntitiesToTable(entities)
        logger.info("Exported $written entities to $TABLE_NAME")
        logger.info("Actual number of entities found in all submission entity sets: $totalSubmissionEntities")
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}

private data class ResponseEntity(
    val code: String?,
    val question: String?,
    val response: Set<String>,
    val startDateTime: OffsetDateTime?,
    val endDateTime: OffsetDateTime?
)

private data class SubmissionEntity(
    val orgId: UUID,
    val studyId: UUID,
    val submissionId: UUID,
    val date: OffsetDateTime?,
    val participantId: String,
    val responses: Set<ResponseEntity>
)