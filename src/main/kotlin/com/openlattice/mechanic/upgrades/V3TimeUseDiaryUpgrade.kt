package com.openlattice.mechanic.upgrades

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.openlattice.authorization.Principal
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.PagedNeighborRequest
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.search.SearchService
import com.openlattice.search.requests.EntityNeighborsFilter
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.sql.Types
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Andrew Carter andrew@openlattice.com
 *
 * Reconstructs Time Use Diary submissions from the Chronicle
 * entity data model. Inserts submissions into a postgres table
 * for migration to Chronicle v3
 *
 */
class V3TimeUseDiaryUpgrade(
    private val toolbox: Toolbox,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val pgEntityDataQueryService: PostgresEntityDataQueryService,
    private val searchService: SearchService,
    private val principalService: HazelcastPrincipalService,
    ) : Upgrade {

    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(toolbox.hazelcast)
    private val entitySetIds = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).values.associateBy { it.name }
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)
    private val answerPropertyTypeIds = propertyTypes.getAll(setOf(OL_VALUES_ID, OL_ID_ID))
    private val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)
    // TODO: replace empty strings with chronicle super user ids (auth0 and google-oauth2) when running migration
    private val chronicleSuperUserIds = setOf("")

    companion object {

        val logger = LoggerFactory.getLogger(V3TimeUseDiaryUpgrade::class.java)
        val objectMapper = ObjectMapper().registerModule( JavaTimeModule() )
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

        var answersRead = 0
        var answersWritten = 0
        var submissionsRead = 0
        var submissionsWritten = 0
        var answersSkipped = 0

        private val CHRONICLE_SURVEYS_APP_ID = UUID.fromString("bb44218b-515a-4314-b955-df2c991b2575")
        private val COMPLETED_DT_FQN = FullQualifiedName("date.completeddatetime")
        private val SUBJECT_ID_FQN = FullQualifiedName("nc.SubjectIdentification")
        private val OL_TITLE_FQN = FullQualifiedName("ol.title")
        private val OL_ID_FQN = FullQualifiedName("ol.id")
        private val OL_DT_START_FQN = FullQualifiedName("ol.datetimestart")
        private val OL_DT_END_FQN = FullQualifiedName("ol.datetimeend")
        private val OL_VALUES_FQN = FullQualifiedName("ol.values")
        private val OL_VALUES_ID = UUID.fromString("dcc3bc24-3a5d-45cf-8e38-bc9ba8c43d06")
        private val OL_ID_ID = UUID.fromString("39e13db7-a730-421a-a600-ae0674060140")
        private val OL_STUDY_ID = UUID.fromString("80c86a96-0e3f-46eb-9fbb-60d9174566a5")
        private const val PARTICIPANT_ENTITY_SET_SUFFIX = "participants"
        private const val STUDY_ENTITY_SET_SUFFIX = "studies"
        private const val PARTICIPATED_IN_ENTITY_SET_SUFFIX = "participatedin"
        private const val ANSWER_ENTITY_SET_SUFFIX = "answer"
        private const val CHRONICLE_ENTITY_SET_PREFIX = "chronicle_"
        private const val CHRONICLE_SURVEYS_ENTITY_SET_PREFIX = "chronicle_surveys_"

        private val dropExistingTimeUseDiaryTableSql = """
            DROP TABLE IF EXISTS public.time_use_diary_submissions;
        """.trimIndent()

        private val createTimeUseDiarySubmissionsTableSql = """        
            CREATE TABLE public.time_use_diary_submissions (
                submission_id uuid NOT NULL,
                organization_id uuid NOT NULL,
                study_id uuid NOT NULL,
                participant_id text NOT NULL,
                submission_date timestamp with time zone DEFAULT now() NOT NULL,
                submission jsonb,
                PRIMARY KEY (submission_id)
            );
        """.trimIndent()

        private val insertTimeUseDiarySql = """
            INSERT INTO public.time_use_diary_submissions
            VALUES ( ?, ?, ?, ?, ?, ? );
        """.trimIndent()


        private val TimeUseDiarySubmissionPropIds =
            mapOf(
                UUID.fromString("a8bccb5b-cda9-4509-a4df-e0b0b8c8d8e4") to "submission",
                UUID.fromString("c949a756-70cc-4070-abca-4272a19c68f0") to "submission_date",
                UUID.fromString("94b57e93-7996-4769-9b6a-2df9ee676172") to "question",
                UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab") to "participant"
            )
    }

    init {
        // Drops existing time_use_diary_submissions pg table then creates a blank one
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["alpr"]!!
        val hc = HikariConfig(hikariConfiguration)
        val hds = HikariDataSource(hc)
        hds.connection.createStatement().use { statement ->
            statement.addBatch(dropExistingTimeUseDiaryTableSql)
            statement.addBatch(createTimeUseDiarySubmissionsTableSql)
            statement.executeBatch()
        }
    }

    override fun upgrade(): Boolean {
        logger.info("Starting migration of Time Use Diaries to table 'time_use_diary_submissions'")
        // Get all answer entity sets from EntitySets Map
        val superUserPrincipals = getChronicleSuperUserPrincipals()

        val v2ChronicleSurveyOrganizations = appConfigs.keys
            .filter { it.appId == CHRONICLE_SURVEYS_APP_ID }
            .map { it.organizationId }
            .toMutableSet()

        v2ChronicleSurveyOrganizations.forEach{ organizationId ->
            logger.info("Gathering answers for organization $organizationId")

            val answerEntitySet = getEntitySetForOrg(CHRONICLE_SURVEYS_ENTITY_SET_PREFIX, organizationId, ANSWER_ENTITY_SET_SUFFIX)
            val entityKeyIdsByEntitySetIds = mapOf(answerEntitySet.id to Optional.of(setOf<UUID>()))
            val answerAuthPropertyTypes = mapOf(answerEntitySet.id to answerPropertyTypeIds)
            val answerNeighborhoods = mutableListOf<AnswerNeighborhood>()

            // Use PostgresEntityDataQueryService to pull answer entities for organization
            val answerEntitiesByAnswerId = getAnswerEntities(entityKeyIdsByEntitySetIds, answerAuthPropertyTypes)

            // Gather each answer neighborhood using SearchService
            for ((answerId, fqnToValue) in answerEntitiesByAnswerId) {
                logger.info("Searching for neighbors of answer $answerId")
                answersRead += 1

                val adminRoleAclKey = organizations[organizationId]?.adminRoleAclKey
                val principals = principalService.getAllUsersWithPrincipal(adminRoleAclKey!!).map { it.principal }.toSet() + superUserPrincipals

                val filter = EntityNeighborsFilter(setOf(answerId))
                val searchResult = searchForNeighbors(answerId, setOf(answerEntitySet.id), filter, principals)

                val answerNeighborhood = extractAnswerNeighborhoodFromSearchResult(searchResult)
                answerNeighborhood.organizationId = organizationId
                answerNeighborhood.answer = fqnToValue.getOrDefault(OL_VALUES_FQN, setOf("")) as Set<String>

                if (answerNeighborhood.participantESID == null || answerNeighborhood.participantId == null) {
                    logger.warn("Skipping migration for participant $answerId. Failed to retrieve associated participant details.")
                    answersSkipped += 1
                    continue
                }

                val participantFilter = EntityNeighborsFilter(
                    setOf(answerNeighborhood.participantId),
                    Optional.of(setOf(getEntitySetForOrg(CHRONICLE_ENTITY_SET_PREFIX, organizationId, PARTICIPANT_ENTITY_SET_SUFFIX).id)),
                    Optional.of(setOf(getEntitySetForOrg(CHRONICLE_ENTITY_SET_PREFIX, organizationId, STUDY_ENTITY_SET_SUFFIX).id)),
                    Optional.of(setOf(getEntitySetForOrg(CHRONICLE_ENTITY_SET_PREFIX, organizationId, PARTICIPATED_IN_ENTITY_SET_SUFFIX).id))
                )
                val participantNeighbors = searchForNeighbors(
                    answerNeighborhood.participantId,
                    setOf(answerNeighborhood.participantESID),
                    participantFilter,
                    principals
                )

                val study = participantNeighbors.filter { it.neighborEntitySet.get().entityTypeId == OL_STUDY_ID }
                answerNeighborhood.studyId = study.first().neighborId.get() // study var should contain only a single study

                answerNeighborhoods.add(answerNeighborhood)
            }

            // Create Submission objects containing all questions and answers submitted as part of that survey
            val answersBySubmissionId = answerNeighborhoods.groupBy { it.submissionId }
            submissionsRead += answersBySubmissionId.keys.size
            val submissions = createSubmissionsFromAnswerNeighborhoods(answersBySubmissionId)

            // Insert all submissions into postgres
            insertSubmissions(submissions)
        }
        logger.info("Finished migration. Migration details:")
        logger.info("Answers read:        $answersRead")
        logger.info("Answers written:     $answersWritten")
        logger.info("Answers skipped:     $answersSkipped")
        logger.info("Submissions read:    $submissionsRead")
        logger.info("Submissions written: $submissionsWritten")
        return true
    }

    /**
     * Inserts a list of Submission objects with the PreparedStatement bind ordering for a Submission is as follows:
     * 1) submissionId
     * 2) organizationId
     * 3) studyId
     * 4) participantId
     * 5) submissionDate
     * 6) TimeUseDiaryResponse
     */
    private fun insertSubmissions(submissions: List<Submission>) {
        var answersInBatch = 0
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["alpr"]!!
        val hc = HikariConfig(hikariConfiguration)
        val hds = HikariDataSource(hc)
        hds.connection.prepareStatement(insertTimeUseDiarySql).use { preparedStatement ->
            try {
                for (submission in submissions) {
                    logger.info("Preparing to insert submission ${submission.submissionId}")
                    var index = 1
                    preparedStatement.setObject(index++, submission.submissionId)
                    preparedStatement.setObject(index++, submission.organizationId)
                    preparedStatement.setObject(index++, submission.studyId)
                    preparedStatement.setString(index++, submission.participant)
                    preparedStatement.setObject(index++, submission.submissionDate, Types.TIMESTAMP_WITH_TIMEZONE)
                    preparedStatement.setObject(index, objectMapper.writeValueAsString(submission.submission), Types.OTHER)
                    preparedStatement.addBatch()
                    answersInBatch += submission.submission.size
                }
                logger.info("Executing batch of submission inserts")
                submissionsWritten += preparedStatement.executeBatch().sum()
                answersWritten += answersInBatch
            } catch (ex: Exception) {
                logger.error("Error inserting submissions $ex")
            }
        }
    }

    private fun getEntitySetForOrg(prefix: String, organizationId: UUID, entityTypeName: String): EntitySet {
        val entitySetNameByTemplate = "$prefix${organizationId.toString().replace("-", "")}_$entityTypeName"
        return entitySetIds.getValue(entitySetNameByTemplate)
    }

    private fun getAnswerEntities(
        answerEntityKeyIds: Map<UUID, Optional<Set<UUID>>>,
        answerAuthPropertyTypes: Map<UUID, MutableMap<UUID, PropertyType>>
    ): Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>> {
        return pgEntityDataQueryService.getEntitiesWithPropertyTypeFqns(
            answerEntityKeyIds,
            answerAuthPropertyTypes,
            emptyMap(),
            EnumSet.of(MetadataOption.LAST_WRITE),
            Optional.empty(),
            false
        )
    }

    private fun searchForNeighbors(
        id: UUID,
        entitySetIds: Set<UUID>,
        filter: EntityNeighborsFilter,
        principals: Set<Principal>
    ): List<NeighborEntityDetails> {
        return searchService.executeEntityNeighborSearch(
            entitySetIds,
            PagedNeighborRequest(filter),
            principals
        ).neighbors.getOrDefault(id, listOf())
    }

    private fun extractAnswerNeighborhoodFromSearchResult(
        searchResult: List<NeighborEntityDetails>,
    ): AnswerNeighborhood {
        var submissionId: UUID? = null
        var submissionDateTime: OffsetDateTime? = null
        var participantName: String? = null
        var participantId: UUID? = null
        var startDateTime: OffsetDateTime? = null
        var endDateTime: OffsetDateTime?= null
        var question: String? = null
        var questionCode: String? = null
        var participantEntitySetId: UUID? = null

        searchResult.forEach {
            val neighborId = it.neighborId.get()
            val neighborEntitySet = it.neighborEntitySet.get()
            val neighborDetails = it.neighborDetails.get()

            when (TimeUseDiarySubmissionPropIds[neighborEntitySet.entityTypeId]) {
                "submission" -> {
                    submissionId = neighborId
                    submissionDateTime = OffsetDateTime.parse(fstStr(it.associationDetails[COMPLETED_DT_FQN]))
                }
                "participant" -> {
                    participantName = fstStr(neighborDetails[SUBJECT_ID_FQN])
                    participantId = neighborId
                    participantEntitySetId = neighborEntitySet.id
                }
                "question" -> {
                    question = fstStr(neighborDetails[OL_TITLE_FQN])
                    questionCode = fstStr(neighborDetails[OL_ID_FQN])
                }
                "submission_date" -> {
                    val olstartdatetime = neighborDetails[OL_DT_START_FQN]
                    if (olstartdatetime != null) {
                        startDateTime = OffsetDateTime.parse(fstStr(olstartdatetime))
                        endDateTime = OffsetDateTime.parse(fstStr(neighborDetails[OL_DT_END_FQN]))
                    }
                }
            }
        }

        return AnswerNeighborhood(
            submissionId,
            null,
            null,
            participantName,
            submissionDateTime,
            question,
            questionCode,
            null,
            startDateTime,
            endDateTime,
            participantId,
            participantEntitySetId
        )
    }

    private fun createSubmissionsFromAnswerNeighborhoods(answersBySubmissionId: Map<UUID?, List<AnswerNeighborhood>>): List<Submission> {
        val submissions = mutableListOf<Submission>()

        for (answers in answersBySubmissionId.values) {
            var submissionId: UUID? = null
            var studyId: UUID? = null
            var participant: String? = null
            var submissionDate: OffsetDateTime? = null
            var organizationId: UUID? = null

            val submissionResponses = mutableListOf<TimeUseDiaryResponse>()
            for (answer in answers) {
                if (answer.questionString == null || answer.answer == null || answer.questionCode == null) {
                    logger.warn("Skipping TimeUseDiaryResponse for answer $answer. Failed to retrieve required question or answer details.")
                    answersSkipped += 1
                    continue
                }
                submissionResponses.add(
                    TimeUseDiaryResponse(
                        answer.questionCode,
                        answer.questionString,
                        answer.answer!!,
                        answer.startDateTime,
                        answer.endDateTime,
                    )
                )

                // Do this to ensure that values passed into Submission below are not null
                if (submissionId == null && answer.submissionId != null) {
                    submissionId = answer.submissionId
                }
                if (studyId == null && answer.studyId != null) {
                    studyId = answer.studyId
                }
                if (participant == null && answer.participant != null) {
                    participant = answer.participant
                }
                if (submissionDate == null && answer.submissionDate != null) {
                    submissionDate = answer.submissionDate
                }
                if (organizationId == null && answer.organizationId != null) {
                    organizationId = answer.organizationId
                }
            }

            if (submissionId == null || studyId == null || participant == null || submissionDate == null || organizationId == null) {
                logger.warn("Skipping Submission for answer $answers. " +
                        "Failed to retrieve required detail for Submission from answer group: submissionId, submissionDate, studyId, or participantId.")
                continue
            }
            submissions.add(
                Submission(
                    submissionId,
                    organizationId,
                    studyId,
                    participant,
                    submissionDate,
                    submissionResponses
                )
            )
        }
        return submissions
    }

    private fun getChronicleSuperUserPrincipals(): Set<Principal> {
        return chronicleSuperUserIds
            .map { principalService.getSecurablePrincipal(it)}
            .map { principalService.getAllPrincipals(it).map { principal -> principal.principal } }
            .flatten().toSet()
    }

    private fun fstStr(collection: Collection<Any>?): String? {
        collection ?: return null
        return collection.first().toString()
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}

data class AnswerNeighborhood(
    val submissionId:       UUID?,
    var organizationId:     UUID?,
    var studyId:            UUID?,
    val participant:        String?,
    val submissionDate:     OffsetDateTime?,
    val questionString:     String?,
    val questionCode:       String?,
    var answer:             Set<String>?,
    val startDateTime:      OffsetDateTime?,
    val endDateTime:        OffsetDateTime?,
    val participantId:      UUID?,
    val participantESID:    UUID?,
)

data class TimeUseDiaryResponse(
    val code: String,
    val question: String,
    val response: Set<String>,
    val startDateTime: OffsetDateTime?,
    val endDateTime: OffsetDateTime?,
)

data class Submission(
    val submissionId: UUID,
    val organizationId: UUID,
    val studyId: UUID,
    val participant: String,
    val submissionDate: OffsetDateTime,
    val submission: List<TimeUseDiaryResponse>
)

