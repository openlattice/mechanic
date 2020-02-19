package com.openlattice.mechanic.upgrades

import com.dataloom.mappers.ObjectMappers
import com.google.common.collect.ImmutableSet
import com.google.common.eventbus.EventBus
import com.openlattice.apps.App
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppRole
import com.openlattice.apps.AppTypeSetting
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
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors
import kotlin.collections.LinkedHashSet

/** This update should be run *after* the UpdateAppTables upgrade has run **/

class ConvertAppsToEntityTypeCollections(
        private val toolbox: Toolbox,
        private val eventBus: EventBus
) : Upgrade {

    val hazelcast = toolbox.hazelcast

    val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcast)

    val apps = HazelcastMap.APPS.getMap(hazelcast)
    val appConfigs = HazelcastMap.APP_CONFIGS.getMap(hazelcast)
    val entityTypeCollections = HazelcastMap.ENTITY_TYPE_COLLECTIONS.getMap(hazelcast)
    val entitySetCollections = HazelcastMap.ENTITY_SET_COLLECTIONS.getMap(hazelcast)
    val entitySetCollectionsConfig = HazelcastMap.ENTITY_SET_COLLECTION_CONFIG.getMap(hazelcast)

    val reservations = HazelcastAclKeyReservationService(hazelcast)

    val authorizationQueryService = AuthorizationQueryService(toolbox.hds, hazelcast)
    val authorizations = HazelcastAuthorizationService(hazelcast, authorizationQueryService, eventBus)


    companion object {
        private val logger = LoggerFactory.getLogger(ConvertAppsToEntityTypeCollections::class.java)
        private val mapper = ObjectMappers.getJsonMapper()

        private val DEFAULT_ROLE_PERMISSIONS = EnumSet.of(Permission.READ, Permission.WRITE, Permission.OWNER)
    }

    /** We assume the apps and app_configs tables have already been udpated **/
    override fun upgrade(): Boolean {

        /** Load legacy appTypes from old app_types table **/
        val appTypes = getAppTypes()

        /** Load legacy app configs, mapping appId -> List<(orgId, appTypeId, entitySetId)> **/
        val appConfigs = getLegacyAppConfigs()

        /**
         * For each app:
         *
         * 1) convert app to EntityTypeCollection
         * 2) convert all its configs to EntitySetCollections
         * 3) create mappings for existing roles
         * 4) create new app configs
         *
         * **/
        getLegacyApps().forEach { migrateApp(it, getAppTypesForApp(it, appTypes), appConfigs.getOrDefault(it.id, listOf())) }

        return true
    }

    private fun getAppTypesForApp(app: LegacyApp, appTypes: Map<UUID, AppType>): Map<UUID, AppType> {
        return app.appTypeIds.associateWith { appTypes.getValue(it) }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }

    /**
     *
     * 1) convert app to EntityTypeCollection
     * 2) convert all its configs to EntitySetCollections
     * 3) create mappings for existing roles
     * 4) create new app configs
     *
     * **/
    private fun migrateApp(app: LegacyApp, appTypes: Map<UUID, AppType>, appConfigs: List<Triple<UUID, UUID, UUID>>) {

        val entityTypeCollection = createEntityTypeCollectionForApp(app, appTypes)

        val appRoles = createAppRolesAndEntityTypeCollectionId(app, entityTypeCollection)

        migrateAppConfigsToEntitySetCollections(app, appTypes, appRoles, appConfigs, entityTypeCollection)

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

    private fun createAppRolesAndEntityTypeCollectionId(app: LegacyApp, entityTypeCollection: EntityTypeCollection): List<AppRole> {

        logger.info("About to update entityTypeCollectionId and roles fields for app ${app.name}")

        val entityTypesAndPropertyTypes = mutableMapOf<UUID, Optional<Set<UUID>>>()
        entityTypeCollection.template.forEach {
            entityTypesAndPropertyTypes[it.entityTypeId] = Optional.of(toolbox.entityTypes.getValue(it.entityTypeId).properties)
        }

        val roles = DEFAULT_ROLE_PERMISSIONS.map {
            val roleTitle = "${app.title} - $it"
            AppRole(
                    UUID.randomUUID(),
                    roleTitle,
                    roleTitle,
                    "$it permission for ${app.title} app",
                    mapOf(it to entityTypesAndPropertyTypes)
            )
        }

        apps[app.id] = App(
                app.id,
                app.name,
                app.title,
                Optional.of(app.description),
                app.url,
                entityTypeCollection.id,
                roles.toMutableSet(),
                mutableMapOf()
        )

        logger.info("Finished updating fields for app ${app.name}")

        return roles
    }

    private fun migrateAppConfigsToEntitySetCollections(
            app: LegacyApp,
            appTypes: Map<UUID, AppType>,
            appRoles: List<AppRole>,
            appConfigsToMigrate: List<Triple<UUID, UUID, UUID>>,
            entityTypeCollection: EntityTypeCollection
    ) {

        val appId = app.id

        val templateByFqn = entityTypeCollection.template.map { it.name to it.id }.toMap()

        val appTypeIdsToTemplateTypeIds = appTypes.map {
            it.key to templateByFqn.getValue(it.value.type.fullQualifiedNameAsString)
        }.toMap()

        val templatesByOrg = mutableMapOf<UUID, MutableMap<UUID, UUID>>()

        appConfigsToMigrate.forEach {
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

            /** map roles **/
            val mappedRoles = appRoles.filter { appRole ->
                reservations.isReserved("$orgId|${appRole.name}")
            }.associate { appRole ->
                val orgAppRoleName = "$orgId|${appRole.name}"
                val roleId = reservations.getId(orgAppRoleName)

                appRole.id!! to AclKey(orgId, roleId)
            }.toMutableMap()

                /** create entity set collection **/
                reservations.reserveIdAndValidateType(entitySetCollection, entitySetCollection::name)
                entitySetCollections[entitySetCollection.id] = entitySetCollection

                /** create entity set collection mappings **/
                entitySetCollectionsConfig.putAll(entitySetCollection.template.entries.associate { CollectionTemplateKey(entitySetCollection.id, it.key) to it.value })

                /** grant permissions on entity set collection to organization owners **/
                authorizations.setSecurableObjectType(AclKey(entitySetCollection.id), SecurableObjectType.EntitySetCollection)
                authorizations.addPermissions(listOf(
                        Acl(
                                AclKey(entitySetCollection.id),
                                orgOwners[AclKey(orgId)].map { Ace(it, EnumSet.allOf(Permission::class.java)) }.toList()
                        )
                ))

                /** trigger indexing **/
                eventBus.post(EntitySetCollectionCreatedEvent(entitySetCollection))

                /** create app config **/
                appConfigs[AppConfigKey(appId, orgId)] = AppTypeSetting(UUID.randomUUID(), entitySetCollection.id, mappedRoles, mutableMapOf())
            }
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
        val sql = "SELECT * FROM ${APPS.name}_legacy"

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

    /** appId -> List<(orgId, appTypeId, entitySetId)> **/
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
