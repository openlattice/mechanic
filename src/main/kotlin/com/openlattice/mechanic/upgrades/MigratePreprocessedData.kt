package com.openlattice.mechanic.upgrades

import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EntitySetManager
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
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class MigratePreprocessedData(
    val toolbox: Toolbox,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val entitySetService: EntitySetManager,
    private val searchService: SearchService,
    private val principalService: SecurePrincipalsManager,
) : Upgrade {

    private val entitySetIds: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).associate { it.value.name to it.key }
    private val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)

    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(MigratePreprocessedData::class.java)

        private const val SUPER_USER_PRINCIPAL_ID = "auth0|5ae9026c04eb0b243f1d2bb6"

        private val DATA_COLLECTION_APP_ID = UUID.fromString("c4e6d8fd-daf9-41e7-8c59-2a12c7ee0857")
        private val LEGACY_ORG_ID = UUID.fromString("7349c446-2acc-4d14-b2a9-a13be39cff93")

        private const val CHRONICLE_APP_ES_PREFIX = "chronicle_"
        private const val DATA_COLLECTION_APP_ES_PREFIX = "chronicle_data_collection_"

        private const val LEGACY_STUDIES_ES = "chronicle_study"
        private const val LEGACY_RECORDED_BY_ES = "chronicle_recorded_by"
        private const val LEGACY_PREPROCESSED_ES = "chronicle_preprocessed_app_data"
        private const val LEGACY_PARTICIPATED_IN_ES = "chronicle_participated_in"


        // collection template names
        private const val PRE_PROCESSED_TEMPLATE = "preprocesseddata";
        private const val PARTICIPATED_IN_TEMPLATE = "participatedin"
        private const val PARTICIPANTS_TEMPLATE = "participants"
        private const val RECORDED_BY_TEMPLATE = "recordedby"

        // entity sets lookup name
        private const val RECORDED_BY_ES = "recordedBy"
        private const val PRE_PROCESSED_ES = "preprocessed"
        private const val PARTICIPATED_IN_ES = "participatedIn"
        private const val PARTICIPANTS_ES = "participants"

        // table columns
        private const val PARTICIPANT_ID = "participant_id"
        private const val STUDY_ID = "study_id"
        private const val APP_LABEL = "app_label"
        private const val DATE_TIME_START = "datetime_start"
        private const val DATE_TIME_END = "datetime_end"
        private const val APP_PACKAGE_NAME = "app_package_name"
        private const val TIMEZONE = "timezone"
        private const val RECORD_TYPE = "record_type"
        private const val NEW_PERIOD = "new_period"
        private const val NEW_APP = "new_app"
        private const val DURATION = "duration_seconds"
        private const val WARNING = "warning"

        private val PERSON_FQN = FullQualifiedName("nc.SubjectIdentification")
        private val TITLE_FQN = FullQualifiedName("ol.title")
        private val DATE_TIME_START_FQN = FullQualifiedName("ol.datetimestart")
        private val DATE_TIME_END_FQN = FullQualifiedName("general.EndTime")
        private val FULL_NAME_FQN = FullQualifiedName("general.fullname")
        private val TIMEZONE_FQN = FullQualifiedName("ol.timezone")
        private val RECORD_TYPE_FQN = FullQualifiedName("ol.recordtype")
        private val NEW_PERIOD_FQN = FullQualifiedName("ol.newperiod")
        private val DURATION_FQN = FullQualifiedName("general.Duration")
        private val WARNING_FQN = FullQualifiedName("ol.warning")
        private val NEW_APP_FQN = FullQualifiedName("ol.newapp")


        private val CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS preprocessed_usage_events(
                $STUDY_ID uuid not null,
                $PARTICIPANT_ID text not null,
                $APP_LABEL text,
                $APP_PACKAGE_NAME text,
                $DATE_TIME_START timestamp with time zone,
                $DATE_TIME_END timestamp with time zone,
                $TIMEZONE text,
                $RECORD_TYPE text,
                $NEW_PERIOD boolean,
                $NEW_APP boolean,
                $DURATION DOUBLE PRECISION ,
                $WARNING text
            )
        """.trimIndent()

        private val PARAMS_BINDING = linkedSetOf(
            STUDY_ID,
            PARTICIPANT_ID,
            APP_LABEL,
            APP_PACKAGE_NAME,
            DATE_TIME_START,
            DATE_TIME_END,
            TIMEZONE,
            RECORD_TYPE,
            NEW_PERIOD,
            NEW_APP,
            DURATION,
            WARNING
        ).joinToString { "?" }

        /**
         * PreparedStatement bind order
         * 1) legacy study id
         * 2) participant_id
         * 3) app label
         * 4) appPackageName
         * 5) dateStart
         * 6) dateEnd,
         * 7) timezone
         * 8) recordType
         * 9) newPeriod
         * 10) newApp
         * 11) duration
         * 12) warning
         */
        private val INSERT_SQL = """
            INSERT INTO preprocessed_usage_events values ($PARAMS_BINDING)
        """.trimIndent()
    }

    init {
        getHikariDataSource().connection.createStatement().use { statement ->
            statement.execute(CREATE_TABLE_SQL)
        }
    }

    override fun upgrade(): Boolean {

        val participants = BasePostgresIterable(
            PreparedStatementHolderSupplier(
                getHikariDataSource(),
                "select * from (select * from participants_export union select * from missed_participants_fresh) AS participants"
            ) {}
        ) {
            participant(it)
        }.groupBy { it.organization_id }

        val orgIds = (appConfigs.keys.filter { it.appId == DATA_COLLECTION_APP_ID }.map { it.organizationId } + LEGACY_ORG_ID).toSet()
        val principals = getChronicleSuperUserPrincipals()
        val invalidOrgIds = orgIds.filter { !participants.keys.contains(it) }
        logger.info("Organizations not found. Skipping: ${invalidOrgIds.map { organizations[it] }}")

        val hds = getHikariDataSource()
        (orgIds - invalidOrgIds.toSet()).forEach { orgId ->
            exportEntities(
                hds,
                orgId,
                participants.getValue(orgId).associateBy { participant -> participant.participant_ek_id },
                principals
            )
        }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }

    private fun writeEntities(entities: List<PreProcessedEntity>, hds: HikariDataSource): Int {
        return hds.connection.use { connection ->
            try {
                val wc = connection.prepareStatement(INSERT_SQL).use { ps ->
                    entities.forEach {
                        var index = 0
                        ps.setObject(++index, it.study_id)
                        ps.setString(++index, it.participant_id)
                        ps.setString(++index, it.appLabel)
                        ps.setString(++index, it.packageName)
                        ps.setObject(++index, it.datetimeStart)
                        ps.setObject(++index, it.datetimeEnd)
                        ps.setString(++index, it.timezone)
                        ps.setString(++index, it.recordType)
                        ps.setBoolean(++index, it.newPeriod)
                        ps.setBoolean(++index, it.newApp)
                        ps.setObject(++index, it.duration)
                        ps.setString(++index, it.warning)
                        ps.addBatch()
                    }
                    ps.executeBatch().sum()
                }
                return@use wc
            } catch (ex: Exception) {
                logger.error("exception", ex)
                throw ex
            }
        }
    }

    private fun getHikariDataSource(): HikariDataSource {
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["chronicle"]!!
        val hc = HikariConfig(hikariConfiguration)
        return HikariDataSource(hc)
    }

    private fun participant(rs: ResultSet): ParticipantExport {
        return ParticipantExport(
            participant_ek_id = rs.getObject("participant_ek_id", UUID::class.java),
            legacy_study_id = rs.getObject("legacy_study_id", UUID::class.java),
            legacy_participant_id = rs.getString("legacy_participant_id"),
            organization_id = rs.getObject("organization_id", UUID::class.java)
        )
    }

    private fun exportEntities(
        hds: HikariDataSource,
        orgId: UUID,
        participants: Map<UUID, ParticipantExport>,
        principals: Set<Principal>
    ) {
        logger.info("getting preprocessed data entities for org $orgId")
        val orgEntitySetIds = getOrgEntitySetNames(orgId)

        val participantEntitySetIds = when (orgId) {
            LEGACY_ORG_ID -> {
                val entitySetNames = participants.values.map { it.legacy_study_id }.map { studyId -> "chronicle_participants_$studyId" }
                entitySetIds.filter { entitySetNames.contains(it.key) }.values.toSet()
            }
            else -> setOf(orgEntitySetIds.getValue(PARTICIPANTS_ES))
        }
        if (participants.isEmpty()) {
            logger.info("No participants found. Skipping org")
            return
        }

        val allIds = participants.keys.toMutableSet()
        while (allIds.isNotEmpty()) {
            val current = allIds.take(20).toSet()
            logger.info("processing batch of ${current.size}. Remaining: ${(allIds - current).size}")
            val participantNeighbors: Map<UUID, List<NeighborEntityDetails>> = getParticipantNeighbors(
                entityKeyIds = current,
                entitySetIds = orgEntitySetIds,
                participantEntitySetIds = participantEntitySetIds,
                principals = principals
            )
            val entities = participantNeighbors.mapValues {
                it.value.map { entityDetails -> getEntity(entityDetails.neighborDetails.get(), it.key, participants) }
            }.values.flatten().filter { it.study_id != null && it.participant_id != null}

            logger.info("retrieved ${entities.size} preprocessed entities")
            val written = writeEntities(entities, hds)
            logger.info("exported $written entities to table")

            allIds -= current
        }
    }

    private fun getEntity(
        entity: Map<FullQualifiedName, Set<Any>>,
        participant_ek_id: UUID,
        participants: Map<UUID, ParticipantExport>
    ): PreProcessedEntity {
        val participant = participants[participant_ek_id]

        return PreProcessedEntity(
            study_id = participant?.legacy_study_id,
            participant_id = participant?.legacy_participant_id,
            appLabel = getFirstValueOrNull(entity, TITLE_FQN),
            packageName = getFirstValueOrNull(entity, FULL_NAME_FQN),
            datetimeStart = getFirstValueOrNull(entity, DATE_TIME_START_FQN)?.let { OffsetDateTime.parse(it) },
            datetimeEnd = getFirstValueOrNull(entity, DATE_TIME_END_FQN)?.let { OffsetDateTime.parse(it) },
            timezone = getFirstValueOrNull(entity, TIMEZONE_FQN),
            recordType = getFirstValueOrNull(entity, RECORD_TYPE_FQN),
            newPeriod = getFirstValueOrNull(entity, NEW_PERIOD_FQN).toBoolean(),
            duration = getFirstValueOrNull(entity, DURATION_FQN)?.toDouble(),
            warning = getFirstValueOrNull(entity, WARNING_FQN),
            newApp = getFirstValueOrNull(entity, NEW_APP_FQN).toBoolean()
        )
    }

    private fun getLegacyParticipantEntitySetIds(): Set<UUID> {
        return entitySetIds.filter { it.key.startsWith("chronicle_participants_") }.map { it.value }.toSet()
    }

    private fun getParticipantNeighbors(
        entityKeyIds: Set<UUID>,
        entitySetIds: Map<String, UUID>,
        participantEntitySetIds: Set<UUID>,
        principals: Set<Principal>
    ): Map<UUID, List<NeighborEntityDetails>> {
        val preprocessedEntitySetId = entitySetIds.getValue(PRE_PROCESSED_ES)
        val participatedInEntitySetId = entitySetIds.getValue(PARTICIPATED_IN_ES)
        val recordedByEntitySetId = entitySetIds.getValue(RECORDED_BY_ES)

        val filter = EntityNeighborsFilter(
            entityKeyIds,
            Optional.of(setOf(preprocessedEntitySetId)),
            Optional.empty(),
            Optional.of(setOf(recordedByEntitySetId, participatedInEntitySetId))
        )
        return searchService.executeEntityNeighborSearch(
            participantEntitySetIds,
            PagedNeighborRequest(filter),
            principals
        ).neighbors
    }


    // Returns participants in an org
    private fun getOrgParticipants(
        participantEntitySetIds: Set<UUID>
    ): Set<UUID> {
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
            participantEntitySetIds.associateWith { Optional.empty() },
            entitySetService.getPropertyTypesOfEntitySets(participantEntitySetIds),
            mapOf(),
            setOf(),
            Optional.empty(),
            false
        ).keys
    }


    private fun getFirstValueOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): String? {
        entity[fqn]?.iterator()?.let {
            if (it.hasNext()) return it.next().toString()
        }
        return null
    }


    private fun getOrgEntitySetNames(orgId: UUID): Map<String, UUID> {
        val entitySetNameByTemplateName = when (orgId) {
            LEGACY_ORG_ID -> mapOf(
                PRE_PROCESSED_ES to LEGACY_PREPROCESSED_ES,
                PARTICIPATED_IN_ES to LEGACY_PARTICIPATED_IN_ES,
                RECORDED_BY_ES to LEGACY_RECORDED_BY_ES
            )
            else -> {
                val orgIdToStr = orgId.toString().replace("-", "")
                mapOf(
                    PRE_PROCESSED_ES to "$DATA_COLLECTION_APP_ES_PREFIX${orgIdToStr}_$PRE_PROCESSED_TEMPLATE",
                    RECORDED_BY_ES to "$DATA_COLLECTION_APP_ES_PREFIX${orgIdToStr}_$RECORDED_BY_TEMPLATE",
                    PARTICIPANTS_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_${PARTICIPANTS_TEMPLATE}",
                    PARTICIPATED_IN_ES to "$CHRONICLE_APP_ES_PREFIX${orgIdToStr}_$PARTICIPATED_IN_TEMPLATE",
                )
            }
        }

        return entitySetNameByTemplateName.mapValues { entitySetIds.getValue(it.value) }
    }

    private fun getChronicleSuperUserPrincipals(): Set<Principal> {
        val securablePrincipal = principalService.getSecurablePrincipal(SUPER_USER_PRINCIPAL_ID)
        return principalService.getAllPrincipals(securablePrincipal).map { it.principal }.toSet() + Principal(PrincipalType.USER, SUPER_USER_PRINCIPAL_ID)
    }
}

data class PreProcessedEntity(
    val study_id: UUID?,
    val participant_id: String?,
    val appLabel: String?,
    val datetimeStart: OffsetDateTime?,
    val datetimeEnd: OffsetDateTime?,
    val packageName: String?,
    val timezone: String?,
    val recordType: String?,
    val newPeriod: Boolean,
    val newApp: Boolean,
    val duration: Double?,
    val warning: String?
)

data class ParticipantExport(
    val participant_ek_id: UUID,
    val legacy_study_id: UUID,
    val legacy_participant_id: String,
    val organization_id: UUID
)