package com.openlattice.mechanic.upgrades

import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyApi

import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.edm.EdmConstants.Companion.LAST_WRITE_FQN
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.mapstores.PropertyTypeMapstore

import com.hazelcast.query.Predicates
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.time.OffsetDateTime
import java.util.EnumSet
import java.util.Optional
import java.util.UUID

class V3StudyMigrationUpgrade(
    toolbox: Toolbox,
    private val dataQueryService: PostgresEntityDataQueryService,
    private val storageResolver: StorageResolver,
    private val studyService: StudyService
): Upgrade {

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

    override fun upgrade(): Boolean {
        logger.info("starting migration of studies to v3")

        val (flavor, hds) = storageResolver.getPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for studies." }

        // Property Types of ol.study
        val studiesPropertyTypes = propertyTypes.entrySet(
            Predicates.`in`<UUID, PropertyType>(
                PropertyTypeMapstore.NAMESPACE_INDEX,
                "general.stringid", // UUID.fromString("ee3a7573-aa70-4afb-814d-3fad27cda988")
                "general.fullname", // UUID.fromString("70d2ff1c-2450-4a47-a954-a7641b7399ae")
                "diagnosis.Description", // UUID.fromString("c502df5b-ec63-4f15-b529-d38695366c75")
                "location.latitude", // UUID.fromString("06083695-aebe-4a56-9b98-da6013e93a5e")
                "location.longitude", // UUID.fromString("e8f9026a-2494-4749-84bb-1499cb7f215c")
                "sharing.name", // UUID.fromString("54f64f11-9c4d-4701-897b-630ffeb94a21")
                "ol.version", // UUID.fromString("0011bbfe-d5d4-4f88-97a8-cdeb821deb6f")
                "contact.Email" // UUID.fromString("6a1f7cf6-80eb-4fe9-a9f4-49cad15c6154"
            )
        )
            // need to do this silly conversion or entrySet will return Set<Map.Entry<,>>
            .map { it.key to it.value }.toMap()

        entitySets.entrySet(
            Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, UUID.fromString("80c86a96-0e3f-46eb-9fbb-60d9174566a5"))
        )
            .groupBy { it.value.organizationId }
            .forEach { orgId, orgStudyEntitySets ->
                val studyEntityKeyIds = orgStudyEntitySets.associate { it.key to Optional.of(setOf<UUID>()) }.toMap()
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
                ).forEach { _, FQNtoValue ->
                    hds.connection.use { connection ->
                        studyService.createStudy(
                            connection,
                            Study(
                                studyId = FQNtoValue[EdmConstants.STRING_ID_FQN] as UUID,
                                title = FQNtoValue[EdmConstants.FULL_NAME_FQN] as String,
                                description = FQNtoValue[FullQualifiedName("diagnosis.Description")] as String,
                                updatedAt = FQNtoValue[LAST_WRITE_FQN] as OffsetDateTime,
                                lat = FQNtoValue[FullQualifiedName("location.latitude")] as Double,
                                lon = FQNtoValue[FullQualifiedName("location.longitude")] as Double,
                                group = FQNtoValue[FullQualifiedName("sharing.name")] as String,
                                version = FQNtoValue[EdmConstants.VERSION_FQN] as String,
                                contact = FQNtoValue[FullQualifiedName("contact.Email")] as String,
                                organizationIds = setOf(orgId)
                            )
                        )
                    }
                }

                // logger.info("progress ${index + 1}/${organizations.size}")
                logger.info("================================")
                logger.info("================================")
            }

        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}