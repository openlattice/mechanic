package com.openlattice.mechanic.upgrades

import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principal
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
class MigrateChronicleParticipantStats(
    val toolbox: Toolbox,
    private val searchService: SearchService,
    private val principalService: SecurePrincipalsManager,
    private val entitySetService: EntitySetManager,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val authorizationService: AuthorizationManager
) : Upgrade {

    // entitySetName -> string
    private val entitySetIds: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).associate { it.value.name to it.key }
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

    private val entities: MutableList<ParticipantStats> = mutableListOf()

    companion object {
        private val logger = LoggerFactory.getLogger(MigrateChronicleParticipantStats::class.java)

        private val chronicleSuperUserIds = setOf("auth0|5ae9026c04eb0b243f1d2bb6")

        private val LEGACY_ORG_ID = UUID.fromString("7349c446-2acc-4d14-b2a9-a13be39cff93")
        private val DATA_COLLECTION_APP_ID = UUID.fromString("c4e6d8fd-daf9-41e7-8c59-2a12c7ee0857")
        private val SURVEY_APP_ID = UUID.fromString("bb44218b-515a-4314-b955-df2c991b2575")

        private const val CHRONICLE_APP_ES_PREFIX = "chronicle_"
        private const val SURVEYS_APP_ES_PREFIX = "chronicle_surveys_"

        // collection template names
        private const val STUDIES_TEMPLATE = "studies"
        private const val PARTICIPATED_IN_TEMPLATE = "participatedin"
        private const val RESPONDS_WITH_TEMPLATE = "respondswith"
        private const val SUBMISSION_TEMPLATE = "submission"
        private const val PARTICIPANTS_TEMPLATE = "participants"
        private const val HAS_TEMPLATE = "has"
        private const val METADATA_TEMPLATE = "metadata"

        // legacy entity set names
        private const val LEGACY_STUDIES_ES = "chronicle_study"
        private const val LEGACY_PARTICIPATED_IN_ES = "chronicle_participated_in"
        private const val LEGACY_RECORDED_BY_ES = "chronicle_recorded_by"
        private const val LEGACY_HAS_ES = "chronicle_has"
        private const val LEGACY_METADATA_ES = "chronicle_metadata"

        // entity sets lookup name
        private const val STUDIES_ES = "studies"
        private const val PARTICIPATED_IN_ES = "participatedIn"
        private const val PARTICIPANTS_ES = "participants"
        private const val SUBMISSION_ES = "submission"
        private const val RESPONDS_WITH_ES = "respondsWith"
        private const val HAS_ES = "has"
        private const val METADATA_ES = "metadata"

        private val OL_ID_FQN = EdmConstants.ID_FQN
        private val STRING_ID_FQN = FullQualifiedName("general.stringid")
        private val PERSON_FQN = FullQualifiedName("nc.SubjectIdentification")
        private val FULL_NAME_FQN = FullQualifiedName("general.fullname")
        private val DATE_TIME_START_FQN = FullQualifiedName("ol.datetimestart")
        private val DATE_TIME_END_FQN = FullQualifiedName("ol.datetimeend")
        private val DATETIME_FQN = FullQualifiedName("ol.datetime")
        private val RECORDED_DATE_FQN = FullQualifiedName("ol.recordeddate")

        // column names
        private const val ORGANIZATION_IO = "organization_id"
        private const val V2_STUDY_ID = "legacy_study_id"
        private const val V2_STUDY_EKID = "legacy_study_ekid"
        private const val PARTICIPANT_ID = "participant_id"
        private const val ANDROID_FIRST_DATE = "android_first_date"
        private const val ANDROID_LAST_DATE = "android_last_date"
        private const val ANDROID_DATES_COUNT = "android_dates_count"
        private const val IOS_FIRST_DATE = "ios_first_date"
        private const val IOS_LAST_DATE = "ios_last_date"
        private const val IOS_DATES_COUNT = "ios_dates_count"
        private const val TUD_FIRST_DATE = "tud_first_date"
        private const val TUD_LAST_DATE = "tud_last_date"
        private const val TUD_DATES_COUNT = "tud_dates_count"

        private val CREATE_STATS_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS participant_stats(
                $ORGANIZATION_IO uuid NOT NULL,
                $V2_STUDY_EKID uuid NOT NULL,
                $V2_STUDY_ID uuid NOT NULL,
                $PARTICIPANT_ID text NOT NULL,
                $ANDROID_FIRST_DATE timestamp with time zone,
                $ANDROID_LAST_DATE timestamp with time zone,
                $ANDROID_DATES_COUNT integer,
                $IOS_FIRST_DATE timestamp with time zone,
                $IOS_LAST_DATE timestamp with time zone,
                $IOS_DATES_COUNT integer,
                $TUD_FIRST_DATE timestamp with time zone,
                $TUD_LAST_DATE timestamp with time zone,
                $TUD_DATES_COUNT integer,
                PRIMARY KEY($V2_STUDY_EKID, $PARTICIPANT_ID)
            )
        """.trimIndent()

        private val COLS = setOf(
            ORGANIZATION_IO,
            V2_STUDY_EKID,
            V2_STUDY_ID,
            PARTICIPANT_ID,
            ANDROID_FIRST_DATE,
            ANDROID_LAST_DATE,
            ANDROID_DATES_COUNT,
            TUD_FIRST_DATE,
            TUD_LAST_DATE,
            TUD_DATES_COUNT
        )

        private val PARTICIPANT_STATS_COLS = COLS.joinToString { it }
        private val PARTICIPANT_STATS_PARAMS = COLS.joinToString { "?" }

        /**PreparedStatement bind order
         * 1) organizationId
         * 2) studyEntityKeyId,
         * 3) studyId,
         * 4) participantId
         * 5) androidFirstDate
         * 6) androidLastDate
         * 7) androidDatesCount,
         * 8) tudFirstDate,
         * 9) tudLastDate
         * 10) tudDatesCount
         */
        private val INSERT_PARTICIPANT_STATS_SQL = """
            INSERT INTO participant_stats ($PARTICIPANT_STATS_COLS) values ($PARTICIPANT_STATS_PARAMS)
        """.trimIndent()
    }

    init {
        // create table
        val hds = getHikariDataSource()
        hds.connection.createStatement().use { stmt -> stmt.execute(CREATE_STATS_TABLE_SQL) }
    }


    override fun upgrade(): Boolean {
        getEntitiesToInsert()
        val totalWritten = writeEntitiesToTable()
        logger.info("Exported $totalWritten entities to participant stats table. Expected to export ${entities.size} entities")
        return true
    }


    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }


    private fun getHikariDataSource(): HikariDataSource {
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["alpr"]!!
        val hc = HikariConfig(hikariConfiguration)
        return HikariDataSource(hc)
    }

    private fun getEntitiesToInsert() {

        val orgIdsByAppId = getOrgIdsByAppId().toMutableMap()
        orgIdsByAppId.getValue(DATA_COLLECTION_APP_ID).add(LEGACY_ORG_ID)
        val superUserPrincipals = getChronicleSuperUserPrincipals()

        (orgIdsByAppId.values.flatten().toSet()).forEach { orgId ->
            logger.info("---------------------------------------------")
            logger.info("Retrieving entities in organization $orgId")
            logger.info("----------------------------------------------")

            // get principals
            val adminRoleAclKey = organizations[orgId]?.adminRoleAclKey
            if (adminRoleAclKey == null) {
                logger.warn("skipping {} since it doesn't have admin role", orgId)
                return@forEach
            }
            val principals = principalService.getAllUsersWithPrincipal(adminRoleAclKey).map { it.principal }.toSet() + superUserPrincipals

            val entitySets = getOrgEntitySetNames(orgId)

            // step 1: get studies in org: {studyEntityKeyId -> study}
            val studies: Map<UUID, Study> = getOrgStudies(entitySetId = entitySets.getValue(STUDIES_ES))
            if (studies.isEmpty()) {
                logger.info("organization $orgId has no studies. Skipping")
                return@forEach
            }
            logger.info("Retrieved ${studies.size} studies")

            // step 2: get all participants in org
            val participantEntitySets = when (orgId) {
                LEGACY_ORG_ID -> getLegacyParticipantEntitySetIds(studies.values.map { it.studyId }.toSet())
                else -> setOf(entitySets.getValue(PARTICIPANTS_ES))
            }.filter { authorizationService.checkIfHasPermissions(AclKey(it), superUserPrincipals, EnumSet.of(Permission.READ)) }.toSet()
            logger.info("participant entity sets: $participantEntitySets")

            val participants = getOrgParticipants(
                participantEntitySetIds = participantEntitySets,
                studiesEntitySetId = entitySets.getValue(STUDIES_ES),
                entityKeyIds = studies.keys,
                principals = principals,
                edgeEntitySetId = entitySets.getValue(PARTICIPATED_IN_ES)
            ).toMutableMap()

            logger.info("participant entity sets: $participantEntitySets")
            logger.info("Retrieved ${participants.values.flatten().size} participants")
            logger.info("Participant count by study: ${participants.map { studies.getValue(it.key).title to it.value.size }.toMap()}")

            // check for duplicates
            participants.forEach { (studyEntityKeyId, studyParticipants) ->
                val unique = studyParticipants.distinct()
                val duplicates = studyParticipants - unique.toSet()

                if (duplicates.isNotEmpty()) {
                    logger.info("Found duplicate participants in study ${studies.getValue(studyEntityKeyId)}: $duplicates")
                    participants[studyEntityKeyId] = unique
                }
            }

            // step 3: neighbor search on participant entity set
            val participantStats = getParticipantStats(
                participantEntitySets = participantEntitySets,
                entitySetIds = entitySets,
                orgIdsByAppId = orgIdsByAppId,
                orgId = orgId,
                principals = principals,
                participantById = participants.values.flatten().associateBy { it.id },
                studies = studies
            )

            logger.info("Participant stats entities by study: ${participantStats.map { studies.getValue(it.key).title to it.value.size }.toMap()}")
            entities.addAll(participantStats.values.flatten())

        }

        logger.info("Total entities to write: ${entities.size}")
    }

    private fun writeEntitiesToTable(): Int {
        return getHikariDataSource().connection.use { connection ->
            try {
                val wc = connection.prepareStatement(INSERT_PARTICIPANT_STATS_SQL).use { ps ->
                    entities.forEach {
                        var index = 0
                        ps.setObject(++index, it.organizationId)
                        ps.setObject(++index, it.studyEntityKeyId)
                        ps.setObject(++index, it.studyId)
                        ps.setString(++index, it.participantId)
                        ps.setObject(++index, it.androidFirstDate)
                        ps.setObject(++index, it.androidLastDate)
                        ps.setInt(++index, it.androidDatesCount)
                        ps.setObject(++index, it.tudFirstDate)
                        ps.setObject(++index, it.tudLastDate)
                        ps.setObject(++index, it.tudDatesCount)
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

    // Returns a mapping from studyEntityKeyId to list of participants
    private fun getOrgParticipants(
        participantEntitySetIds: Set<UUID>,
        edgeEntitySetId: UUID,
        studiesEntitySetId: UUID,
        entityKeyIds: Set<UUID>,
        principals: Set<Principal>
    )
        : Map<UUID, List<Participant>> {
        val filter = EntityNeighborsFilter(entityKeyIds, Optional.of(participantEntitySetIds), Optional.empty(), Optional.of(setOf(edgeEntitySetId)))

        return searchService
            .executeEntityNeighborSearch(setOf(studiesEntitySetId), PagedNeighborRequest(filter), principals)
            .neighbors
            .mapValues { it.value.map { neighbor -> getParticipantFromNeighborEntity(it.key, neighbor) } }

    }

    // mapping from studyEntityKeyId to a list of participant stats objects
    private fun getParticipantStats(
        participantEntitySets: Set<UUID>,
        entitySetIds: Map<String, UUID>,
        orgIdsByAppId: Map<UUID, Set<UUID>>,
        orgId: UUID,
        principals: Set<Principal>,
        participantById: Map<UUID, Participant>,
        studies: Map<UUID, Study>,
    ): Map<UUID, List<ParticipantStats>> {

        val srcEntitySetIds: MutableSet<UUID> = participantEntitySets.toMutableSet()
        val edgeEntitySetIds: MutableSet<UUID> = mutableSetOf(entitySetIds.getValue(HAS_ES))
        val dstEntitySetIds: MutableSet<UUID> = mutableSetOf(entitySetIds.getValue(METADATA_ES))

        if (isAppIdInOrg(orgId, SURVEY_APP_ID, orgIdsByAppId)) {
            edgeEntitySetIds.add(entitySetIds.getValue(RESPONDS_WITH_ES))
            dstEntitySetIds.add(entitySetIds.getValue(SUBMISSION_ES))
        }

        val filter = EntityNeighborsFilter(
            participantById.keys,
            Optional.of(srcEntitySetIds),
            Optional.of(dstEntitySetIds),
            Optional.of(edgeEntitySetIds)
        )

        return searchService.executeEntityNeighborSearch(participantEntitySets, PagedNeighborRequest(filter), principals)
            .neighbors
            .mapValues { (id, neighbors) ->
                val neighborsByAssociationES = neighbors.groupBy { it.associationEntitySet.id }
                val androidStats = getParticipantAndroidStats(neighborsByAssociationES[entitySetIds.getValue(HAS_ES)])
                val tudStats = getParticipantTudStats(neighborsByAssociationES[entitySetIds[RESPONDS_WITH_ES]]) // not every org has respondsWith entity set

                val studyEntityKeyId = participantById.getValue(id).studyEntityKeyId
                ParticipantStats(
                    organizationId = orgId,
                    studyEntityKeyId = studyEntityKeyId,
                    studyId = studies.getValue(studyEntityKeyId).studyId,
                    participantId = participantById.getValue(id).participantId,
                    androidFirstDate = androidStats.first,
                    androidLastDate = androidStats.second,
                    androidDatesCount = androidStats.third,
                    tudFirstDate = tudStats.first,
                    tudLastDate = tudStats.second,
                    tudDatesCount = tudStats.third
                )
            }.values.groupBy { it.studyEntityKeyId }
    }

    // start, end date, count
    // in theory each participant should only have a single NeighborEntityDetails in the metadata entity set,
    // but some might have multiple entities
    private fun getParticipantAndroidStats(neighbors: List<NeighborEntityDetails>?): Triple<OffsetDateTime?, OffsetDateTime?, Int> {

        if (neighbors == null || neighbors.isEmpty()) {
            return Triple(null, null, 0)
        }

        val dateTimeStartValues = getOffsetDateTimesFromNeighborEntities(neighbors, DATE_TIME_START_FQN)
        val dateTimeEndValues = getOffsetDateTimesFromNeighborEntities(neighbors, DATE_TIME_END_FQN)
        val datesRecorded = getOffsetDateTimesFromNeighborEntities(neighbors, RECORDED_DATE_FQN)

        return Triple(
            first = dateTimeStartValues.stream().min(OffsetDateTime::compareTo).get(),
            second = dateTimeEndValues.stream().max(OffsetDateTime::compareTo).get(),
            third = datesRecorded.map { it.toLocalDate() }.toSet().size // unique dates
        )
    }

    // start date, end date, count
    private fun getParticipantTudStats(neighbors: List<NeighborEntityDetails>?): Triple<OffsetDateTime?, OffsetDateTime?, Int> {
        if (neighbors == null) return Triple(null, null, 0)

        val dateTimeValues = getOffsetDateTimesFromNeighborEntities(neighbors, DATETIME_FQN)

        return Triple(
            first = dateTimeValues.stream().min(OffsetDateTime::compareTo).get(),
            second = dateTimeValues.stream().max(OffsetDateTime::compareTo).get(),
            third = dateTimeValues.map { it.toLocalDate() }.toSet().size // unique dates
        )
    }

    // returns a mapping from appId to setOf organizations containing app
    private fun getOrgIdsByAppId(): Map<UUID, MutableSet<UUID>> {
        return HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast).keys
            .filter { it.appId == DATA_COLLECTION_APP_ID || it.appId == SURVEY_APP_ID }
            .groupBy { it.appId }
            .mapValues { it.value.map { config -> config.organizationId }.toMutableSet() }
    }


    private fun getOffsetDateTimesFromNeighborEntities(entities: List<NeighborEntityDetails>, fqn: FullQualifiedName): Set<OffsetDateTime> {
        return entities
            .map { getAllValuesOrNull(it.neighborDetails.get(), fqn) }
            .flatten().map { OffsetDateTime.parse(it) }.toSet()
    }

    private fun isAppIdInOrg(orgId: UUID, appId: UUID, orgIdsByAppId: Map<UUID, Set<UUID>>): Boolean {
        return orgIdsByAppId.getValue(appId).contains(orgId)
    }

    private fun getChronicleSuperUserPrincipals(): Set<Principal> {
        return chronicleSuperUserIds
            .map { principalService.getSecurablePrincipal(it) }
            .map { principalService.getAllPrincipals(it).map { principal -> principal.principal } }
            .flatten().toSet()
    }

    private fun getLegacyParticipantEntitySetIds(studyIds: Set<UUID>): Set<UUID> {
        val entitySetNames = studyIds.map { "chronicle_participants_$it" }
        return entitySetNames.mapNotNull { entitySetIds[it] }.toSet()
    }

    private fun getOrgEntitySetNames(orgId: UUID): Map<String, UUID> {
        val entitySetNameByTemplateName = when (orgId) {
            LEGACY_ORG_ID -> mapOf(
                STUDIES_ES to LEGACY_STUDIES_ES,
                PARTICIPATED_IN_ES to LEGACY_PARTICIPATED_IN_ES,
                HAS_ES to LEGACY_HAS_ES,
                METADATA_ES to LEGACY_METADATA_ES
            )
            else -> {
                val orgIdToStr = orgId.toString().replace("-", "")
                mapOf(
                    STUDIES_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$STUDIES_TEMPLATE",
                    HAS_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$HAS_TEMPLATE",
                    METADATA_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$METADATA_TEMPLATE",
                    PARTICIPATED_IN_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$PARTICIPATED_IN_TEMPLATE",
                    PARTICIPANTS_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_${PARTICIPANTS_TEMPLATE}",
                    SUBMISSION_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${SUBMISSION_TEMPLATE}",
                    RESPONDS_WITH_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${RESPONDS_WITH_TEMPLATE}",
                )
            }
        }

        return entitySetNameByTemplateName.filter { entitySetIds.keys.contains(it.value) }.mapValues { entitySetIds.getValue(it.value) }
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

    private fun getStudyEntity(studyEntityKeyId: UUID, entity: Map<FullQualifiedName, Set<Any>>): Study {
        val title = getFirstValueOrNull(entity, FULL_NAME_FQN)
        val studyId = getFirstUUIDOrNull(entity, STRING_ID_FQN)
        return Study(studyEntityKeyId, studyId!!, title)
    }
}

class Participant(
    val studyEntityKeyId: UUID,
    val id: UUID,
    val participantId: String,
) {
    override fun equals(other: Any?): Boolean {
        other as Participant
        if (other.participantId == this.participantId && other.studyEntityKeyId == this.studyEntityKeyId) return true
        return false
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + studyEntityKeyId.hashCode()
        result = 31 * result + participantId.hashCode()
        return result
    }
}

private data class ParticipantStats(
    val organizationId: UUID,
    val studyEntityKeyId: UUID,
    val studyId: UUID,
    val participantId: String,
    val androidFirstDate: Any?,
    val androidLastDate: Any?,
    val androidDatesCount: Int = 0,
    val tudFirstDate: Any?,
    val tudLastDate: Any?,
    val tudDatesCount: Int = 0
)

private data class Study(
    val studyEntityKeyId: UUID,
    val studyId: UUID,
    val title: String?
)