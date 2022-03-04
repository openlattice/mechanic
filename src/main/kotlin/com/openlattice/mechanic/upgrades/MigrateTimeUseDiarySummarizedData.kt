package com.openlattice.mechanic.upgrades

import com.fasterxml.jackson.databind.json.JsonMapper
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.rhizome.configuration.RhizomeConfiguration
import com.geekbeast.util.log
import com.openlattice.authorization.AuthorizationManager
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
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class MigrateTimeUseDiarySummarizedData(
    toolbox: Toolbox,
    private val rhizomeConfiguration: RhizomeConfiguration,
    private val principalService: SecurePrincipalsManager,
    private val searchService: SearchService,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val entitySetService: EntitySetManager
) : Upgrade{
    private val entitySetIds: Map<String, UUID> = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast).associate { it.value.name to it.key }
    private val appConfigs = HazelcastMap.APP_CONFIGS.getMap(toolbox.hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(MigrateTimeUseDiarySummarizedData::class.java)
        private val mapper = ObjectMappers.getJsonMapper()

        private const val SUPER_USER_PRINCIPAL_ID = "auth0|5ae9026c04eb0b243f1d2bb6"
        private val SURVEYS_APP_ID = UUID.fromString("bb44218b-515a-4314-b955-df2c991b2575")

        private const val SURVEYS_APP_ES_PREFIX = "chronicle_surveys_"

        private const val SUBMISSION_TEMPLATE = "submission"
        private const val SUMMARY_TEMPLATE = "summaryset"
        private const val REGISTERED_FOR_TEMPLATE = "registeredfor"

        private const val SUBMISSION_ES = "submission"
        private const val SUMMARY_ES = "summary"
        private const val REGISTERED_FOR_ES = "registeredFor"

        private val VARIABLE_FQN = FullQualifiedName("ol.variable")
        private val VALUES_FQN = FullQualifiedName("ol.values")

        private val CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS time_use_diary_summary(
                submission_id uuid not null,
                data jsonb not null,
                PRIMARY KEY (submission_id)
            )
        """.trimIndent()

        private val INSERT_INTO_TABLE_SQL = """
            INSERT INTO time_use_diary_summary values(?, ?::jsonb)
        """.trimIndent()
    }


    init {
        getHikariDataSource().connection.createStatement().use { statement ->
            statement.execute(CREATE_TABLE_SQL)
        }
    }

    override fun upgrade(): Boolean {
        val orgIds = appConfigs.keys.filter { it.appId == SURVEYS_APP_ID }.map { it.organizationId }.toSet()
        val principals = getChronicleSuperUserPrincipals()
        val entities = orgIds.map { getEntitiesForOrg(it, principals) }.flatten().toSet()

        val written = writeEntitiesToTable(entities)
        logger.info("Wrote $written entities to table")
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }

    private fun writeEntitiesToTable(entities: Set<SubmissionEntity>): Int {
        val hds = getHikariDataSource()
        return hds.connection.use { connection ->
            val wc = connection.prepareStatement(INSERT_INTO_TABLE_SQL).use { ps ->
                entities.forEach {
                    ps.setObject(1, it.submissionId)
                    ps.setString(3, mapper.writeValueAsString(it.entities))
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
            return@use wc
        }
    }


    private fun getEntitiesForOrg(orgId: UUID, principals: Set<Principal>): Set<SubmissionEntity> {
        logger.info("processing org $orgId")
        val entitySets = getOrgEntitySetNames(orgId)
        logger.info("entity sets: $entitySets")


        val submissionEntitySetId = entitySets[SUBMISSION_ES]
        val registeredForEntitySetId = entitySets[REGISTERED_FOR_ES]
        val summaryEntitySetId = entitySets[SUMMARY_ES]

        if (submissionEntitySetId == null || registeredForEntitySetId == null || summaryEntitySetId == null) {
            logger.info("submission: {}, registered_for: {}, summary: {}", submissionEntitySetId, registeredForEntitySetId, summaryEntitySetId)
            return setOf()
        }

        val submissionIds = dataQueryService.getEntitiesWithPropertyTypeFqns(
            mapOf(submissionEntitySetId to Optional.empty()),
            entitySetService.getPropertyTypesOfEntitySets(setOf(submissionEntitySetId)),
            mapOf(),
            setOf(),
            Optional.empty(),
            false
        ).keys

        // get entities from summarized entity set associated with submission ids
        val filter = EntityNeighborsFilter(
            submissionIds,
            Optional.of(setOf(summaryEntitySetId)),
            Optional.empty(),
            Optional.of(setOf(registeredForEntitySetId))
        )
        val searchResult = searchService.executeEntityNeighborSearch(
            setOf(submissionEntitySetId),
            PagedNeighborRequest(filter),
            principals
        ).neighbors.mapValues { getSummaryEntityForSubmission(it.key, it.value) }

        return searchResult.values.toSet()
    }

    private fun getFirstValueOrNull(entity: Map<FullQualifiedName, Set<Any?>>, fqn: FullQualifiedName): String? {
        entity[fqn]?.iterator()?.let {
            if (it.hasNext()) return it.next().toString()
        }
        return null
    }

    private fun getSummaryEntityForSubmission(submissionId: UUID, neighbors: List<NeighborEntityDetails>): SubmissionEntity {
        val values = neighbors.map {
            val entity = it.neighborDetails.get()
            SummarizedEntity(
                variable = getFirstValueOrNull(entity, VARIABLE_FQN),
                value = getFirstValueOrNull(entity, VALUES_FQN)
            )
        }.filter { it.value != null && it.variable != null }.toSet()

        return SubmissionEntity(
            submissionId = submissionId,
            entities = values,
        )
    }

    private fun getOrgEntitySetNames(orgId: UUID): Map<String, UUID> {
        val orgIdToStr = orgId.toString().replace("-", "")
        val entitySetNameByTemplateName = mapOf(
            REGISTERED_FOR_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${REGISTERED_FOR_TEMPLATE}",
            SUBMISSION_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${SUBMISSION_TEMPLATE}",
            SUMMARY_ES to "$SURVEYS_APP_ES_PREFIX${orgIdToStr}_${SUMMARY_TEMPLATE}"
        )

        return entitySetNameByTemplateName.filter { entitySetIds.keys.contains(it.value) }.mapValues { entitySetIds.getValue(it.value) }
    }


    private fun getHikariDataSource(): HikariDataSource {
        val (hikariConfiguration) = rhizomeConfiguration.datasourceConfigurations["chronicle"]!!
        val hc = HikariConfig(hikariConfiguration)
        return HikariDataSource(hc)
    }

    private fun getChronicleSuperUserPrincipals(): Set<Principal> {
        val securablePrincipal = principalService.getSecurablePrincipal(SUPER_USER_PRINCIPAL_ID)
        return principalService.getAllPrincipals(securablePrincipal).map { it.principal }.toSet() + Principal(PrincipalType.USER, SUPER_USER_PRINCIPAL_ID)
    }

}

private data class SummarizedEntity(
    val variable: String?,
    val value: String?
)

private data class SubmissionEntity(
    val submissionId: UUID,
    val entities: Set<SummarizedEntity>,
)
