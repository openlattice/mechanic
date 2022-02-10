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
import java.time.LocalDate
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

    private val STRING_ID_FQN = FullQualifiedName("general.stringid")
    private val FULL_NAME_FQN = FullQualifiedName("general.fullname")
    private val DESCRIPTION_FQN = FullQualifiedName("diagnosis.Description")
    private val LAT_FQN = FullQualifiedName("location.latitude")
    private val LON_FQN = FullQualifiedName("location.longitude")
    private val SHARING_NAME_FQN = FullQualifiedName("sharing.name")
    private val VERSION_FQN = FullQualifiedName( "ol.version")
    private val EMAIL_FQN = FullQualifiedName("contact.Email")

    private val fqnToStudiesColumnName = mapOf(
        // FullQualifiedName("general.stringid") to "study_id",
        FullQualifiedName("general.fullname") to "title",
        FullQualifiedName("diagnosis.Description") to "description",
        LAST_WRITE_FQN to "updated_at",
        FullQualifiedName("location.latitude") to "latitude",
        FullQualifiedName("location.longitude") to "longitude",
        FullQualifiedName("sharing.name") to "study_group",
        FullQualifiedName( "ol.version") to "study_version",
        FullQualifiedName("contact.Email") to "contact"
    )

    private val fqnToCandidatesColumnName = mapOf(
        FullQualifiedName("Person.GivenName") to "first_name",
        FullQualifiedName("Person.SurName") to "last_name",
        FullQualifiedName("general.fullname") to "name",
        FullQualifiedName("nc.PersonBirthDate") to "dob"
    )

    // Property Types of ol.study
    private val studiesPropertyTypes = propertyTypes.getAll(
        setOf(
            // "general.stringid",
            // UUID.fromString("ee3a7573-aa70-4afb-814d-3fad27cda988"),
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

    private val associationParticipatedInEntityType = UUID.fromString("34836b35-76b1-4ecf-b588-c22ad19e2378")

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

                    val orgParticipantEntitySetIds = entitySets.keySet(
                        // filter out entity sets of {orgId} of entity type general.person
                        Predicates.and(
                            Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ORGANIZATION_INDEX, orgId),
                            Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab"))
                        )
                    ).toSet()

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
                        val v3Id = insertIntoStudiesTable(connection, orgId, fqnToValue)

                        if (v3Id != null) {
                            // process participants of studies
                            logger.info("Processing all participants of ${fqnToValue[STRING_ID_FQN]}")
                            processParticipantsOfStudy(connection, orgId, ekid, v3Id, orgStudyEntitySetIds, orgParticipantEntitySetIds)
                        }
                    }

                    // logger.info("progress ${index + 1}/${organizations.size}")
                    logger.info("================================")
                    logger.info("================================")
                }
        }

        return true
    }

    private fun insertIntoStudiesTable(conn: Connection, orgId: UUID, fqnToValue: Map<FullQualifiedName, MutableSet<Any>>): UUID? {
        val columns = fqnToStudiesColumnName.filter { fqnToValue.containsKey(it.key) }
        if (columns.size == 0) {
            return null
        }

        val studyId = UUID.randomUUID() // temp, need to use id generation service
        val INSERT_INTO_STUDY_SQL = """
            INSERT INTO studies (study_id, ${columns.values.joinToString()}) VALUES (?${",?".repeat(columns.size)})
        """.trimIndent()
        logger.debug(INSERT_INTO_STUDY_SQL)

        conn.prepareStatement(INSERT_INTO_STUDY_SQL).use { ps ->
            try {
                var index = 1

                ps.setObject(index++, studyId)
                columns.keys.forEach {
                    when (it.getNamespace()) {
                        "location" -> ps.setDouble(index++, fqnToValue[it]!!.first() as Double)
                        "openlattice" -> ps.setObject(index++, fqnToValue[it]!!.first() as OffsetDateTime)
                        else -> ps.setString(index++, fqnToValue[it]!!.first() as String)
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

        val INSERT_INTO_ORGANIZATION_STUDIES_SQL = """
            INSERT INTO organization_studies (organization_id, study_id) VALUES (?,?)
        """.trimIndent()

        conn.prepareStatement(INSERT_INTO_ORGANIZATION_STUDIES_SQL).use { ps ->
            try {
                ps.setObject(1, orgId)
                ps.setObject(2, studyId)

                logger.debug(ps.toString())
                ps.addBatch()
                ps.executeBatch()
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Exception occurred inserting participant ${fqnToValue} into study_participants", ex)
                conn.rollback()
            }
        }
        return studyId
    }

    private fun processParticipantsOfStudy(conn: Connection, orgId: UUID, v2Id: UUID, studyId: UUID, orgStudyEntitySetIds: Set<UUID>, orgParticipantEntitySetIds: Set<UUID>) {
        val adminRoleAclKey = organizations.getValue(orgId).adminRoleAclKey
        val principal = principalsMapManager.getSecurablePrincipal(adminRoleAclKey)!!.principal
        logger.info("Using principal ${principal} to execute neighbour search")

        val filter = EntityNeighborsFilter(setOf(v2Id), Optional.of(orgParticipantEntitySetIds), Optional.of(orgStudyEntitySetIds), Optional.empty())

        // get all participants for the study
        val searchResult = searchService.executeEntityNeighborSearch(
            orgStudyEntitySetIds,
            PagedNeighborRequest(filter),
            setOf(principal)
        ).neighbors.getOrDefault(v2Id, listOf())

        if (searchResult.isNotEmpty()) {
            searchResult.filter { it.getNeighborId().isPresent && it.getAssociationEntitySet().entityTypeId == associationParticipatedInEntityType}
                .forEach {
                    // logger.info("Neighbour ${it.getNeighborId()} details:")
                    // logger.info("Association Entity Set:${it.getAssociationEntitySet()}")
                    // logger.info("Association Details: ${it.getAssociationDetails()}")
                    // logger.info("Neighbour Entity Set: ${it.getNeighborEntitySet()}")
                    logger.info("Inserting participant: ${it.getNeighborDetails().get()} into study_participants")
                    val fqnToValue = it.getNeighborDetails().get() + it.getAssociationDetails().filterKeys { it == FullQualifiedName("ol.status") }
                    logger.info("${fqnToValue}")
                    insertIntoCandidatesTable(conn, studyId, fqnToValue)
                }
        }
    }

    private fun insertIntoCandidatesTable(conn: Connection, studyId: UUID, fqnToValue: Map<FullQualifiedName, Set<Any>>) {
        val columns = fqnToCandidatesColumnName.filter { fqnToValue.containsKey(it.key) }
        if (columns.size == 0) {
            return
        }

        val candidateId = UUID.randomUUID() // temp, need to use id generation service
        val INSERT_INTO_CANDIDATE_SQL = """
            INSERT INTO candidates (candidate_id, ${columns.values.joinToString()}) VALUES (?${",?".repeat(columns.size)})
        """.trimIndent()
        logger.debug(INSERT_INTO_CANDIDATE_SQL)

        conn.prepareStatement(INSERT_INTO_CANDIDATE_SQL).use { ps ->
            try {
                var index = 1

                ps.setObject(index++, candidateId)
                columns.keys.forEach {
                    when (it.getNamespace()) {
                        "nc" -> ps.setObject(index++, fqnToValue[it]!!.first() as LocalDate)
                        else -> ps.setString(index++, fqnToValue[it]!!.first() as String)
                    }
                }

                logger.debug(ps.toString())
                ps.addBatch()
                ps.executeBatch()
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Exception occurred inserting participant ${fqnToValue} into candidates", ex)
                conn.rollback()
            }
        }

        val INSERT_INTO_STUDY_PARTICIPANT_SQL = if (fqnToValue[FullQualifiedName("ol.status")] != null) {
            """
                INSERT INTO study_participants (study_id, candidate_id, participation_status) VALUES (?,?,?)
            """.trimIndent()
        } else {
            """
                INSERT INTO study_participants (study_id, candidate_id) VALUES (?,?)
            """.trimIndent()
        }

        conn.prepareStatement(INSERT_INTO_STUDY_PARTICIPANT_SQL).use { ps ->
            try {
                ps.setObject(1, studyId)
                ps.setObject(2, candidateId)
                if (fqnToValue[FullQualifiedName("ol.status")] != null) {
                    ps.setString(3, fqnToValue[FullQualifiedName("ol.status")]!!.first() as String)
                }

                logger.debug(ps.toString())
                ps.addBatch()
                ps.executeBatch()
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Exception occurred inserting participant ${fqnToValue} into study_participants", ex)
                conn.rollback()
            }
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}