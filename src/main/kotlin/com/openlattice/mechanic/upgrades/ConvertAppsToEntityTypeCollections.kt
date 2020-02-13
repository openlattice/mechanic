package com.openlattice.mechanic.upgrades

import com.dataloom.mappers.ObjectMappers
import com.google.common.collect.ImmutableSet
import com.google.common.eventbus.EventBus
import com.hazelcast.query.Predicates
import com.openlattice.apps.App
import com.openlattice.apps.AppRole
import com.openlattice.apps.historical.HistoricalAppConfig
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.collections.CollectionTemplateKey
import com.openlattice.collections.CollectionTemplateType
import com.openlattice.collections.EntitySetCollection
import com.openlattice.collections.EntityTypeCollection
import com.openlattice.edm.events.EntitySetCollectionCreatedEvent
import com.openlattice.edm.events.EntityTypeCollectionCreatedEvent
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.mechanic.pods.legacy.AppType
import com.openlattice.mechanic.pods.legacy.LegacyApp
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.APPS
import com.openlattice.postgres.PostgresTable.APP_CONFIGS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AppConfigMapstore
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.nd4j.linalg.primitives.Quad
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors
import kotlin.collections.LinkedHashSet

private val SET_NEW_APP_FIELDS_SQL = "UPDATE ${APPS.name} SET ${ENTITY_TYPE_COLLECTION_ID.name} = ?, ${ROLES.name} = ?::jsonb WHERE ${ID.name} = ?"

/** This update should be run *after* the UpdateAppTables upgrade has run **/

class ConvertAppsToEntityTypeCollections(
        private val toolbox: Toolbox,
        private val eventBus: EventBus
) : Upgrade {

    val hazelcast = toolbox.hazelcast

    val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcast)

    val entityTypeCollections = HazelcastMap.ENTITY_TYPE_COLLECTIONS.getMap(hazelcast)
    val entitySetCollections = HazelcastMap.ENTITY_SET_COLLECTIONS.getMap(hazelcast)
    val entitySetCollectionsConfig = HazelcastMap.ENTITY_SET_COLLECTION_CONFIG.getMap(hazelcast)

    val reservations = HazelcastAclKeyReservationService(hazelcast)

    val authorizationQueryService = AuthorizationQueryService(toolbox.hds, hazelcast)
    val authorizations = HazelcastAuthorizationService(hazelcast, authorizationQueryService, eventBus)


    companion object {
        private val logger = LoggerFactory.getLogger(ConvertAppsToEntityTypeCollections::class.java)
        private val mapper = ObjectMappers.getJsonMapper()
    }

    override fun upgrade(): Boolean {
        alterAppsTable()

        val appTypes = getAppTypes()
        val appConfigs = getLegacyAppConfigs()

        getLegacyApps().forEach { migrateApp(it, getAppTypesForApp(it, appTypes), appConfigs.getValue(it.id)) }

        return true
    }

    private fun getAppTypesForApp(app: LegacyApp, appTypes: Map<UUID, AppType>): Map<UUID, AppType> {
        return app.appTypeIds.associateWith { appTypes.getValue(it) }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }

    private fun alterAppsTable() {
        val sql = "ALTER TABLE ${APPS.name} " +
                "ADD COLUMN ${ENTITY_TYPE_COLLECTION_ID.sql()}, " +
                "ADD COLUMN ${ROLES.sql()}, " +
                "ADD COLUMN ${SETTINGS.sql()}"

        toolbox.hds.connection.use {
            it.createStatement().use { stmt ->
                stmt.execute(sql)
            }
        }
    }

    private fun migrateApp(app: LegacyApp, appTypes: Map<UUID, AppType>, appConfigs: List<Triple<UUID, UUID, UUID>>) {

        val entityTypeCollection = createEntityTypeCollectionForApp(app, appTypes)

        updateAppFields(app, entityTypeCollection)

        migrateAppConfigToEntitySetCollections(app, appTypes, appConfigs, entityTypeCollection)

    }

    private fun createEntityTypeCollectionForApp(app: LegacyApp, appTypes: Map<UUID, AppType>): EntityTypeCollection {
        logger.info("About to create EntityTypeCollection for app ${app.name}")


        val template = appTypes.values.map {
            CollectionTemplateType(
                    Optional.empty(),
                    it.type.fullQualifiedNameAsString,
                    it.title,
                    Optional.of(it.description),
                    it.entityTypeId
            )
        }.toCollection(LinkedHashSet())

        val entityTypeCollection = EntityTypeCollection(
                Optional.empty(),
                FullQualifiedName("app", app.name),
                app.title,
                Optional.of(app.description),
                ImmutableSet.of(),
                template
        )

        if (reservations.isReserved(entityTypeCollection.type.fullQualifiedNameAsString)) {
            val entityTypeCollectionId = reservations.getId(entityTypeCollection.type.fullQualifiedNameAsString)
            return entityTypeCollections[entityTypeCollectionId]!!
        }

        reservations.reserveIdAndValidateType(entityTypeCollection)
        entityTypeCollections.putIfAbsent(entityTypeCollection.id, entityTypeCollection)
        eventBus.post(EntityTypeCollectionCreatedEvent(entityTypeCollection))

        logger.info("Finished creating entity type collection ${entityTypeCollection.id} for app ${app.name}")

        return entityTypeCollection
    }

    private fun updateAppFields(app: LegacyApp, entityTypeCollection: EntityTypeCollection) {

        logger.info("About to update entityTypeCollectionId and roles fields for app ${app.name}")

        val entityTypesAndPropertyTypes = mutableMapOf<UUID, Optional<Set<UUID>>>()
        entityTypeCollection.template.forEach {
            entityTypesAndPropertyTypes[it.entityTypeId] = Optional.of(toolbox.entityTypes.getValue(it.entityTypeId).properties)
        }

        val roles = EnumSet.of(Permission.READ, Permission.WRITE, Permission.OWNER).map {
            AppRole(
                    UUID.randomUUID(),
                    "${app.title} - $it",
                    "${app.title} - $it",
                    "$it permission for ${app.title} app",
                    mapOf(it to entityTypesAndPropertyTypes)
            )
        }

        toolbox.hds.connection.prepareStatement(SET_NEW_APP_FIELDS_SQL).use {
            it.setObject(1, entityTypeCollection.id)
            it.setString(2, mapper.writeValueAsString(roles))
            it.setObject(3, app.id)
            it.execute()
        }

        logger.info("Finished updating fields for app ${app.name}")
    }

    private fun migrateAppConfigToEntitySetCollections(app: LegacyApp, appTypes: Map<UUID, AppType>, appConfigs: List<Triple<UUID, UUID, UUID>>, entityTypeCollection: EntityTypeCollection) {

        val appId = app.id

        val templateByFqn = entityTypeCollection.template.map { it.name to it.id }.toMap()

        val appTypeIdsToTemplateTypeIds = appTypes.map {
            it.key to templateByFqn.getValue(it.value.type.fullQualifiedNameAsString)
        }.toMap()

        val templatesByOrg = mutableMapOf<UUID, MutableMap<UUID, UUID>>()

        appConfigs.forEach {
            val orgId = it.first
            val templateTypeId = appTypeIdsToTemplateTypeIds.getValue(it.second)
            val entitySetId = it.third

            val orgTemplates = templatesByOrg.getOrDefault(orgId, mutableMapOf())
            orgTemplates[templateTypeId] = entitySetId
            templatesByOrg[orgId] = orgTemplates
        }

        val orgs = organizations.getAll(templatesByOrg.keys)
        val orgOwners = authorizations.getOwnersForSecurableObjects(templatesByOrg.keys.map { AclKey(it) })

        templatesByOrg.forEach {
            val orgId = it.key

            val entitySetCollection = EntitySetCollection(
                    Optional.empty(),
                    "${app.name}_$orgId",
                    "${orgs.getValue(orgId).title} ${app.title} [$orgId]",
                    Optional.of(app.description),
                    entityTypeCollection.id,
                    it.value,
                    setOf(),
                    orgId
            )

            // TODO role mapping

            reservations.reserveIdAndValidateType(entitySetCollection, entitySetCollection::name)

            entitySetCollections[entitySetCollection.id] = entitySetCollection
            entitySetCollectionsConfig.putAll(entitySetCollection.template.entries.associate { CollectionTemplateKey(entitySetCollection.id, it.key) to it.value })

            authorizations.setSecurableObjectType(AclKey(entitySetCollection.id), SecurableObjectType.EntitySetCollection)
            authorizations.addPermissions(listOf(
                    Acl(
                            AclKey(entitySetCollection.id),
                            orgOwners[AclKey(orgId)].map { Ace(it, EnumSet.allOf(Permission::class.java)) }.toList()
                    )
            ))

            eventBus.post(EntitySetCollectionCreatedEvent(entitySetCollection))
        }

    }


    // LOAD FROM OLD TABLES

    private fun getAppTypes(): Map<UUID, AppType> {
        val sql = "SELECT * FROM app_types"

        return BasePostgresIterable(StatementHolderSupplier(toolbox.hds, sql)) {
            AppType(
                    ResultSetAdapters.id(it),
                    ResultSetAdapters.fqn(it),
                    ResultSetAdapters.title(it),
                    Optional.of(ResultSetAdapters.description(it)),
                    ResultSetAdapters.entityTypeId(it)
            )
        }.associateBy { it.id }
    }

    private fun getLegacyApps(): List<LegacyApp> {
        val sql = "SELECT * FROM ${APPS.name}"

        return BasePostgresIterable(StatementHolderSupplier(toolbox.hds, sql)) {
            LegacyApp(
                    ResultSetAdapters.id(it),
                    ResultSetAdapters.name(it),
                    ResultSetAdapters.title(it),
                    Optional.of(ResultSetAdapters.description(it)),
                    Arrays.stream(it.getArray("config_type_ids").array as Array<UUID>).collect(Collectors.toCollection { LinkedHashSet<UUID>() }),
                    ResultSetAdapters.url(it)
            )
        }.toList()
    }

    // appId -> [ <orgId, appTypeId, entitySetId> ]
    private fun getLegacyAppConfigs(): Map<UUID, List<Triple<UUID, UUID, UUID>>> {
        val sql = "SELECT * FROM ${APP_CONFIGS.name}_legacy"

        return BasePostgresIterable(StatementHolderSupplier(toolbox.hds, sql)) {
            Pair(
                    ResultSetAdapters.appId(it),
                    Triple(
                            ResultSetAdapters.organizationId(it),
                            it.getObject("config_type_id", UUID::class.java),
                            ResultSetAdapters.entitySetId(it))
            )
        }.groupBy { it.first }.mapValues { it.value.map { pair -> pair.second } }
    }

}
