package com.openlattice.mechanic.upgrades

import com.openlattice.client.ApiClient
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.search.SearchService
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author Andrew Carter andrew@openlattice.com
 */
class V3TimeUseDiaryUpgrade(
    private val toolbox: Toolbox,
    private val hds: HikariDataSource,
    private val pgEntityDataQueryService: PostgresEntityDataQueryService,
    private val searchService: SearchService
) : Upgrade {

    companion object {
        val logger = LoggerFactory.getLogger(V3TimeUseDiaryUpgrade::class.java)
    }

    override fun upgrade(): Boolean {
        val adminRoleAclKey = organizations.getValue(orgId).adminRoleAclKey
        val principal = principalsMapManager.getSecurablePrincipal(adminRoleAclKey).principal

        val answerId = UUID.fromString("7912f235-1959-4dd2-8a61-d85cd09b0c34")
        val entitySetIds = searchService.entitySetService.getEntitySetIdsOfType(answerId)
        val answerNeighbors = searchService.executeEntityNeighborSearch(entitySetIds.toSet(),)
        return true
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}