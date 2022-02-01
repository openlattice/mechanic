package com.openlattice.mechanic.upgrades

import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyApi

import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.edm.EdmConstants
import com.openlattice.hazelcast.HazelcastMap

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
        val authorizedPropertyTypes = UUID.randomUUID() to propertyTypes.entrySet(
            Predicates.`in`<UUID, PropertyType>("id", ??)
        )

        entitySets.entrySet(
            Predicates.like<UUID, EntitySet>("name", "chronicle%studies")
        )
            .groupBy { it.value.organizationId }
            .forEachIndexed { index, orgIdToStudiesESSet ->
                val orgId = orgIdToStudiesESSet.key
                val studyEntitySetIds = orgIdToStudiesESSet.value.flatMap { it.value.id to Optional.empty() }

                logger.info("================================")
                logger.info("================================")
                logger.info("starting to process org $orgId")

                // get all studies
                val studyEntities = dataQueryService.getEntitiesWithPropertyTypeFqns(
                        studyEntitySetIds,
                        authorizedPropertyTypes,
                        emptyMap(),
                        EnumSet.noneOf(MetadataOption::class.java),
                        Optional.empty(),
                        false
                ).forEach { id, FQNtoValue ->
                    hds.connection.use { connection ->
                        studyService.createStudy(
                            connection,
                            Study(
                                studyId = id,
                                title = FQNtoValue[EdmConstants.FULL_NAME_FQN],
                                description = FQNtoValue[FullQualifiedName("diagnosis.Description")],
                                lat = FQNtoValue[FullQualifiedName("location.latitude")],
                                lon = FQNtoValue[FullQualifiedName("location.longitude")],
                                version = FQNtoValue[EdmConstants.VERSION_FQN],
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