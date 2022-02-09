package com.openlattice.mechanic.upgrades

import com.geekbeast.postgres.PostgresArrays
import com.openlattice.authorization.Principals
import com.openlattice.authorization.PrincipalsMapManager
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.edm.EdmConstants.Companion.ID_FQN
import com.openlattice.edm.EdmConstants.Companion.LAST_WRITE_FQN
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.PagedNeighborRequest
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.openlattice.search.SearchService
import com.openlattice.search.requests.EntityNeighborsFilter

import com.hazelcast.query.Predicates
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.zaxxer.hikari.HikariDataSource

import java.sql.Connection
import java.time.OffsetDateTime
import java.util.EnumSet
import java.util.Optional
import java.util.UUID

class V3StudyMigrationUpgrade(
    toolbox: Toolbox,
    private val hds: HikariDataSource,
    private val principalsMapManager: PrincipalsMapManager,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val searchService: SearchService
): Upgrade {

    private val logger = LoggerFactory.getLogger(V3StudyMigrationUpgrade::class.java)

    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(toolbox.hazelcast)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

    private val STRING_ID_FQN = FullQualifiedName( "general.stringid" )
    private val FULL_NAME_FQN = FullQualifiedName( "general.fullname" )
    private val DESCRIPTION_FQN = FullQualifiedName("diagnosis.Description")
    private val LAT_FQN = FullQualifiedName("location.latitude")
    private val LON_FQN = FullQualifiedName("location.longitude")
    private val SHARING_NAME_FQN =FullQualifiedName("sharing.name")
    private val VERSION_FQN = FullQualifiedName( "ol.version")
    private val EMAIL_FQN = FullQualifiedName("contact.Email")

    private val fqnToColumnName = mapOf(
        STRING_ID_FQN to "study_id",
        FULL_NAME_FQN to "title",
        DESCRIPTION_FQN to "description",
        LAST_WRITE_FQN to "updated_at",
        LAT_FQN to "latitude",
        LON_FQN to "longitude",
        SHARING_NAME_FQN to "study_group",
        VERSION_FQN to "study_version",
        EMAIL_FQN to "contact"
    )

    // Property Types of ol.study
    private val studiesPropertyTypes = propertyTypes.getAll(
        setOf(
            // "general.stringid",
            UUID.fromString("ee3a7573-aa70-4afb-814d-3fad27cda988"),
            // "general.fullname",
            UUID.fromString("70d2ff1c-2450-4a47-a954-a7641b7399ae"),
            // "diagnosis.Description",
            UUID.fromString("c502df5b-ec63-4f15-b529-d38695366c75"),
            // "location.latitude",
            UUID.fromString("06083695-aebe-4a56-9b98-da6013e93a5e"),
            // "location.longitude",
            UUID.fromString("e8f9026a-2494-4749-84bb-1499cb7f215c"),
            // "sharing.name",
            UUID.fromString("54f64f11-9c4d-4701-897b-630ffeb94a21"),
            // "ol.version",
            UUID.fromString("0011bbfe-d5d4-4f88-97a8-cdeb821deb6f"),
            // "contact.Email"
            UUID.fromString("6a1f7cf6-80eb-4fe9-a9f4-49cad15c6154")
        )
    ).toMap()

    override fun upgrade(): Boolean {
        logger.info("starting migration of studies to v3")

        hds.connection.use { connection ->
            connection.autoCommit = false
            entitySets.entrySet(
                // filter out entity set of entity type ol.study
                Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, UUID.fromString("80c86a96-0e3f-46eb-9fbb-60d9174566a5"))
            )
                .groupBy { it.value.organizationId }
                .forEach { orgId, orgStudyEntitySets ->
                    val studyEntityKeyIds = orgStudyEntitySets.associate { it.key to Optional.of(setOf<UUID>()) }.toMap()
                    val orgStudyEntitySetIds = studyEntityKeyIds.keys
                    val studyAuthorizedPropertyTypes = orgStudyEntitySets.associate { it.key to studiesPropertyTypes }.toMap()

                    logger.info("================================")
                    logger.info("================================")
                    logger.info("starting to process studies belonging to org $orgId")

                    // get all studies for the org
                    dataQueryService.getEntitiesWithPropertyTypeFqns(
                            studyEntityKeyIds,
                            studyAuthorizedPropertyTypes,
                            emptyMap(),
                            EnumSet.of(MetadataOption.LAST_WRITE),
                            Optional.empty(),
                            false
                    ).forEach { ekid, fqnToValue ->
                        // process study entities into a Study table
                        logger.info("Processing study ${fqnToValue[STRING_ID_FQN]} with entity_key_id ${ekid}")

                        logger.info("Inserting study ${fqnToValue} into studies")
                        insertIntoStudies(connection, fqnToValue)

                        val orgParticipantEntitySetIds = entitySets.keySet(
                            // filter out entity sets of {orgId} of entity type general.person
                            Predicates.and(
                                Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ORGANIZATION_INDEX, orgId),
                                Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab"))
                            )
                        ).toSet()

                        // process participants of studies
                        logger.info("Processing all participants of ${fqnToValue[STRING_ID_FQN]}")
                        processParticipantsOfStudy(connection, orgId, ekid, orgStudyEntitySetIds, orgParticipantEntitySetIds)
                    }

                    // logger.info("progress ${index + 1}/${organizations.size}")
                    logger.info("================================")
                    logger.info("================================")
                }
        }

        return true
    }

    private fun insertIntoStudies(conn: Connection, fqnToValue: MutableMap<FullQualifiedName, MutableSet<Any>>) {
        val binder = "(?" + ",?".repeat(fqnToValue.size-2) + ")"
        val columns = fqnToColumnName.filter { fqnToValue.containsKey(it.key) }.values.joinToString()
        val INSERT_SQL = """
            INSERT INTO studies (${columns}) VALUES ${binder}
        """.trimIndent()
        logger.debug(INSERT_SQL)

        conn.prepareStatement(INSERT_SQL).use { ps ->
            try {
                var index = 1

                fqnToColumnName.filter { fqnToValue.containsKey(it.key) }.forEach { fqn, _ ->
                    when (fqn.getNamespace()) {
                        "location" -> ps.setDouble(index++, fqnToValue[fqn]!!.first() as Double)
                        "openlattice" -> ps.setObject(index++, fqnToValue[fqn]!!.first() as OffsetDateTime)
                        else -> ps.setString(index++, fqnToValue[fqn]!!.first() as String)
                    }
                }

                logger.debug(ps.toString())
                ps.addBatch()
                ps.executeBatch()
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Exception occurred inserting study ${fqnToValue}", ex)
                conn.rollback()
            }
        }
    }

    private fun processParticipantsOfStudy(conn: Connection, orgId: UUID, ekid: UUID, orgStudyEntitySetIds: Set<UUID>, orgParticipantEntitySetIds: Set<UUID>) {
        val adminRoleAclKey = organizations.getValue(orgId).adminRoleAclKey
        val principal = principalsMapManager.getSecurablePrincipal(adminRoleAclKey).principal
        logger.info("Using principal ${principal} to execute neighbour search")

        val filter = EntityNeighborsFilter(setOf(ekid), Optional.of(orgParticipantEntitySetIds), Optional.of(orgStudyEntitySetIds), Optional.empty())

        val searchResult = searchService.executeEntityNeighborSearch(
            orgStudyEntitySetIds,
            PagedNeighborRequest(filter),
            setOf(principal)
        ).neighbors.getOrDefault(ekid, listOf())

        if (searchResult.isNotEmpty()) {
            val neighborIds = searchResult.filter { it.getNeighborId().isPresent }.map { it.getNeighborId().get() }.toSet()
            val neighborEntitySetId = searchResult.first().getNeighborEntitySet().get().getId()

            // get all participants for the org
            dataQueryService.getEntitiesWithPropertyTypeFqns(
                    mapOf(neighborEntitySetId to Optional.of(neighborIds)),
                    mapOf(neighborEntitySetId to participantPropertyTypes),
                    emptyMap(),
                    EnumSet.noneOf(MetadataOption::class.java),
                    Optional.empty(),
                    false
            ).forEach {
                logger.info("Inserting participant: ${it} into study_participants")
            }
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}