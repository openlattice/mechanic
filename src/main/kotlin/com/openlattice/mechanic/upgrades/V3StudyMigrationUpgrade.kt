package com.openlattice.mechanic.upgrades

import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyApi

import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.edm.EdmConstants
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore

import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.LinkedHashSet
import java.util.Optional
import java.util.UUID

class V3StudyMigrationUpgrade(
    toolbox: Toolbox,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val storageResolver: StorageResolver,
    private val studyService: StudyService
): Upgrade {
    companion object {
        private val logger = LoggerFactory.getLogger(V3StudyMigrationUpgrade::class.java)

        private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(toolbox.hazelcast)
        private val entitySets = HazelcastMap.ENTITY_SETS.getMap(toolbox.hazelcast)
        private val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)

        // private val GET_V2_STUDIES_SQL = """
        //     SELECT id, ? AS ? FROM
        //     (SELECT id, ? AS ? FROM data WHERE entity_set_id = ANY(?) AND property_type_id = ? ORDER BY id) AS ?
        // """.trimIndent()

        // private val V2_DATA_INNERJOIN_SQL = """
        //     INNER JOIN
        //     (SELECT id, ? AS ? FROM data WHERE entity_set_id = ANY(?) AND property_type_id = ? ORDER BY id) AS ?
        //     USING (id)
        // """.trimIndent()
    }

    override fun upgrade(): Boolean {
        logger.info("starting migration of studies to v3")

        val (flavor, hds) = storageResolver.getPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for studies." }

        // Property Types of ol.study
        val studiesPropertyTypes = propertyTypes.entrySet(
            Predicates.`in`<UUID, PropertyType>(
                PropertyTypeMapstore.ID_INDEX,
                UUID.fromString("ee3a7573-aa70-4afb-814d-3fad27cda988"), // general.stringid
                UUID.fromString("70d2ff1c-2450-4a47-a954-a7641b7399ae"), // general.fullname
                UUID.fromString("c502df5b-ec63-4f15-b529-d38695366c75"), // diagnosis.Description
                UUID.fromString("06083695-aebe-4a56-9b98-da6013e93a5e"), // location.latitude
                UUID.fromString("e8f9026a-2494-4749-84bb-1499cb7f215c"), // location.longitude
                UUID.fromString("54f64f11-9c4d-4701-897b-630ffeb94a21"), // sharing.name
                UUID.fromString("0011bbfe-d5d4-4f88-97a8-cdeb821deb6f"), // ol.version
                UUID.fromString("6a1f7cf6-80eb-4fe9-a9f4-49cad15c6154") // contact.Email
            )
        )

        entitySets.entrySet(
            Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, UUID.fromString("80c86a96-0e3f-46eb-9fbb-60d9174566a5"))
        )
            .groupBy { it.value.organizationId }
            .forEachIndexed { index, orgIdToStudiesESSet ->
                val orgId = orgIdToStudiesESSet.key
                val studyEntityKeyIds = orgIdToStudiesESSet.value.flatMap { it.value.id to Optional.empty() }
                val studyAuthorizedPropertyTypes = orgIdToStudiesESSet.value.flatMap { it.value.id to studiesPropertyTypes }

                logger.info("================================")
                logger.info("================================")
                logger.info("starting to process org $orgId")

                // get all studies
                val studyEntities = dataQueryService.getEntitiesWithPropertyTypeFqns(
                        studyEntityKeyIds,
                        studyAuthorizedPropertyTypes,
                        emptyMap(),
                        EnumSet.noneOf(MetadataOption::class.java),
                        Optional.empty(),
                        false
                ).forEach { id, FQNtoValue ->
                    hds.connection.use { connection ->
                        studyService.createStudy(
                            connection,
                            Study(
                                studyId = FQNtoValue[EdmConstants.STRING_ID_FQN],
                                title = FQNtoValue[EdmConstants.FULL_NAME_FQN],
                                description = FQNtoValue[FullQualifiedName("diagnosis.Description")],
                                lat = FQNtoValue[FullQualifiedName("location.latitude")],
                                lon = FQNtoValue[FullQualifiedName("location.longitude")],
                                group = FQNtoValue[FullQualifiedName("sharing.name")]
                                version = FQNtoValue[EdmConstants.VERSION_FQN],
                                contact = FQNtoValue[FullQualifiedName("contact.Email")]
                                organizationIds = setOf(orgId)
                            )
                        )
                    }
                }

                logger.info("progress ${index + 1}/${organizations.size}")
                logger.info("================================")
                logger.info("================================")
            }

        return true
    }
}