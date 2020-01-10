package com.openlattice.mechanic.upgrades

import com.google.common.collect.ImmutableSet
import com.google.common.eventbus.EventBus
import com.hazelcast.query.Predicates
import com.openlattice.apps.App
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppType
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
import com.openlattice.organizations.Organization
import com.openlattice.postgres.mapstores.AppConfigMapstore
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*
import kotlin.collections.LinkedHashSet

class ConvertAppsToEntityTypeCollections(
        private val toolbox: Toolbox,
        private val eventBus: EventBus
) : Upgrade {

    val hazelcast = toolbox.hazelcast

    val organizations = HazelcastMap.ORGANIZATIONS.getMap( hazelcast )

    val apps = HazelcastMap.APPS.getMap( hazelcast )
    val appTypes = HazelcastMap.APP_TYPES.getMap( hazelcast )
    val appConfigs = HazelcastMap.APP_CONFIGS.getMap( hazelcast )

    val entityTypeCollections = HazelcastMap.ENTITY_TYPE_COLLECTIONS.getMap( hazelcast )
    val entitySetCollections = HazelcastMap.ENTITY_SET_COLLECTIONS.getMap( hazelcast )
    val entitySetCollectionsConfig = HazelcastMap.ENTITY_SET_COLLECTION_CONFIG.getMap( hazelcast )

    val reservations = HazelcastAclKeyReservationService(hazelcast)

    val authorizationQueryService = AuthorizationQueryService(toolbox.hds, hazelcast)
    val authorizations = HazelcastAuthorizationService(hazelcast, authorizationQueryService, eventBus);

    override fun upgrade(): Boolean {
        apps.values.forEach { migrateApp(it) }
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }

    private fun migrateApp(app: App) {

        val entityTypeCollection = migrateAppToEntityTypeCollection(app)
        migrateAppConfigToEntitySetCollections(app, entityTypeCollection)

    }

    private fun migrateAppToEntityTypeCollection(app: App): EntityTypeCollection {

        val appTypesForApp = appTypes.getAll(app.appTypeIds)

        val template = appTypesForApp.values.map {
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

        return entityTypeCollection
    }

    private fun migrateAppConfigToEntitySetCollections(app: App, entityTypeCollection: EntityTypeCollection) {

        val appId = app.id

        val templateByFqn = entityTypeCollection.template.map { it.name to it.id }.toMap()

        val appTypeIdsToTemplateTypeIds = appTypes.getAll(app.appTypeIds).map {
            it.key to templateByFqn.getValue(it.value.type.fullQualifiedNameAsString)
        }.toMap()

        val templatesByOrg = mutableMapOf<UUID, MutableMap<UUID, UUID>>()

        appConfigs.entrySet(Predicates.equal(AppConfigMapstore.APP_ID, appId)).forEach {
            val orgId = it.key.organizationId
            val templateTypeId = appTypeIdsToTemplateTypeIds.getValue(it.key.appTypeId)
            val entitySetId = it.value.entitySetId

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


}
