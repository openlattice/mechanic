package com.openlattice.mechanic.upgrades

import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.geekbeast.rhizome.jobs.JobStatus
import com.google.common.base.Stopwatch
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizingComponent
import com.geekbeast.controllers.exceptions.ForbiddenException
import com.openlattice.data.DataDeletionManager
import com.openlattice.data.DeleteType
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.ORGANIZATION
import com.openlattice.postgres.PostgresTable.ORGANIZATIONS
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.StatementHolderSupplier
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

const val META_ESIDS = "metadataEntitySetIds"
const val COLUMNS_ESID = "columnsESID"
const val COLUMNS_KEY = "columns"
const val DATASETS_ESID = "datasetsESID"
const val DATASETS_KEY = "datasets"
const val ORGANIZATION_ESID = "organizationESID"
const val ORGANIZATION_KEY = "organization"

class DeleteOrgMetadataEntitySets(
    private val toolbox: Toolbox,
    private val auditService: AuditRecordEntitySetsManager,
    private val dataDeletionService: DataDeletionManager,
    private val entitySetService: EntitySetManager,
    private val jobService: HazelcastJobService
) : Upgrade {

    companion object {
        private val logger = LoggerFactory.getLogger(DeleteOrgMetadataEntitySets::class.java)
        private val NON_BLOCKING_JOB_STATUSES = EnumSet.of(JobStatus.FINISHED, JobStatus.CANCELED, JobStatus.PAUSED)
        private val UNINITIALIZED_ID = UUID(0, 0)
    }

    override fun upgrade(): Boolean {

        val organizations = HazelcastMap.ORGANIZATIONS.getMap(toolbox.hazelcast)
        organizations.values.forEachIndexed { index, org ->

            try {
                logger.info("================================")
                logger.info("================================")
                logger.info("starting to process org ${org.id}")

                val timer = Stopwatch.createStarted()

                val ids = BasePostgresIterable(
                    StatementHolderSupplier(toolbox.hds, selectMetadataEntitySetIdsSql(org.id))
                ) {
                    Triple(
                        it.getString(ORGANIZATION_ESID),
                        it.getString(DATASETS_ESID),
                        it.getString(COLUMNS_ESID)
                    )
                }
                    .toList()
                    .map {
                        val orgESID = getUuidFromString(org.id, it.first)
                        val dataSetsESID = getUuidFromString(org.id, it.second)
                        val columnsESID = getUuidFromString(org.id, it.third)
                        if (orgESID == null && dataSetsESID == null && columnsESID == null) {
                            return@map null
                        }
                        else {
                            return@map Triple(orgESID, dataSetsESID, columnsESID)
                        }
                    }
                    .filterNotNull()

                if (ids.isEmpty()) {
                    logger.error("missing metadata entity set ids - org ${org.id}}")
                    return@forEachIndexed
                }

                val idTriple = ids.first()
                var jsonDeletes = 0

                if (deleteMetadataAndAuditEntitySets(org.id, idTriple.first, ORGANIZATION_KEY)) {
                    jsonDeletes++
                }

                if (deleteMetadataAndAuditEntitySets(org.id, idTriple.second, DATASETS_KEY)) {
                    jsonDeletes++
                }

                if (deleteMetadataAndAuditEntitySets(org.id, idTriple.third, COLUMNS_KEY)) {
                    jsonDeletes++
                }

                if (jsonDeletes == 3) {
                    deleteMetadataFromJson(org.id)
                }

                logger.info(
                    "processing org took {} ms - org ${org.id}",
                    timer.elapsed(TimeUnit.MILLISECONDS),
                )
                logger.info("progress ${index + 1}/${organizations.size}")
                logger.info("================================")
                logger.info("================================")
            }
            catch(e: Exception) {
                logger.error("something went wrong processing org ${org.id}", e)
            }
        }

        return true
    }

    private fun selectMetadataEntitySetIdsSql(orgId: UUID): String {
        return """
            SELECT
            ${ORGANIZATION.name}->'$META_ESIDS'->>'$COLUMNS_KEY' as $COLUMNS_ESID,
            ${ORGANIZATION.name}->'$META_ESIDS'->>'$DATASETS_KEY' as $DATASETS_ESID,
            ${ORGANIZATION.name}->'$META_ESIDS'->>'$ORGANIZATION_KEY' as $ORGANIZATION_ESID
            FROM ${ORGANIZATIONS.name} WHERE ${ID.name} = '$orgId'
        """.trimIndent()
    }

    private fun getUuidFromString(orgId: UUID, entitySetId: String?): UUID? {
        return try {
            UUID.fromString(entitySetId)
        } catch (e: Exception) {
            logger.error("error converting id to uuid - org $orgId entity set id $entitySetId", e)
            null
        }
    }

    private fun deleteMetadataAndAuditEntitySets(orgId: UUID, entitySetId: UUID?, metadataKey :String): Boolean {

        if (entitySetId == null) {
            logger.warn("entity set id is null - org $orgId")
            return false
        }

        try {

            val auditEdgeEntitySetIds = auditService.getAuditEdgeEntitySets(AclKey(entitySetId))
            if (auditEdgeEntitySetIds.isEmpty()) {
                logger.warn("no audit edge entity sets - org $orgId entity set $entitySetId")
            }

            val auditRecordEntitySetIds = auditService.getAuditRecordEntitySets(AclKey(entitySetId))
            if (auditRecordEntitySetIds.isEmpty()) {
                logger.warn("no audit record entity sets - org $orgId entity set $entitySetId")
            }

            auditEdgeEntitySetIds.forEach { deleteEntitySet(orgId, it) }
            auditRecordEntitySetIds.forEach { deleteEntitySet(orgId, it) }

            if (deleteEntitySet(orgId, entitySetId)) {
                return deleteMetadataFromJson(orgId, metadataKey)
            }
            // if the esid is all zeros AND there's no audit entity sets, it's likely the org wasn't initialized
            // correctly. since there's no entity sets to delete, we'll try deleting metadata from json.
            else if (
                UNINITIALIZED_ID == entitySetId
                && auditEdgeEntitySetIds.isEmpty()
                && auditRecordEntitySetIds.isEmpty()
            ) {
                return deleteMetadataFromJson(orgId, metadataKey)
            }
        }
        catch (e: Exception) {
            logger.error("error deleting metadata and audit entity sets - org $orgId entity set $entitySetId", e)
        }

        return false
    }

    private fun deleteEntitySet(orgId: UUID, entitySetId: UUID): Boolean {

        try {
            logger.info("starting to delete entity set - org $orgId entity set $entitySetId")
            val timer = Stopwatch.createStarted()

            if (AuthorizingComponent.internalIds.contains(entitySetId)) {
                throw ForbiddenException("$entitySetId is a reserved id and cannot be deleted")
            }

            val entitySet = toolbox.entitySets[entitySetId]
            checkNotNull(entitySet) { "entity set is null - org $orgId entity set $entitySetId" }

            val t1 = Stopwatch.createStarted()
            val deleteJobId = dataDeletionService.clearOrDeleteEntitySet(entitySet.id, DeleteType.Hard)
            waitForDeleteJobToFinish(deleteJobId, orgId, entitySet.id)
            logger.info(
                "dataDeletionService.clearOrDeleteEntitySet took {} ms - org $orgId entity set $entitySetId",
                t1.elapsed(TimeUnit.MILLISECONDS),
            )

            val t2 = Stopwatch.createStarted()
            entitySetService.deleteEntitySet(entitySet)
            logger.info(
                "entitySetService.deleteEntitySet took {} ms - org $orgId entity set $entitySetId",
                t2.elapsed(TimeUnit.MILLISECONDS),
            )

            logger.info(
                "deleting entity set took {} ms - org $orgId entity set $entitySetId",
                timer.elapsed(TimeUnit.MILLISECONDS),
            )
            logger.info("finished deleting entity set - org $orgId entity set $entitySetId")
            return true
        } catch (e: Exception) {
            logger.error("error deleting entity set - org $orgId entity set $entitySetId", e)
        }

        return false
    }

    private fun waitForDeleteJobToFinish(jobId: UUID, orgId: UUID, entitySetId: UUID) {

        var status = jobService.getStatus(jobId)
        while (!NON_BLOCKING_JOB_STATUSES.contains(status)) {
            try {
                logger.info("waiting for job to finish - org $orgId entity set $entitySetId job $jobId status $status")
                Thread.sleep(1000)
                status = jobService.getStatus(jobId)
            } catch (e: Exception) {
                logger.error("error waiting for job $jobId")
                return
            }
        }

        if (status != JobStatus.FINISHED) {
            logger.error("stopping waiting for job though it has not finished - job $jobId status $status")
        }
    }

    private fun deleteMetadataFromJson(orgId: UUID, key: String? = null): Boolean {

        if (StringUtils.isNotBlank(key)) {
            logger.info("deleting organization->$META_ESIDS->$key - org $orgId")
            val sql = """
                UPDATE ${ORGANIZATIONS.name}
                SET ${ORGANIZATION.name} = ${ORGANIZATION.name}::jsonb #- ('{"' || ? || '", "' || ? || '"}')::text[]
                WHERE ${ID.name} = ?
            """.trimIndent()
            val update = toolbox.hds.connection.use { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, META_ESIDS)
                    ps.setString(2, key)
                    ps.setObject(3, orgId)
                    ps.executeUpdate()
                }
            }
            return update == 1
        } else {
            logger.info("deleting organization->$META_ESIDS - org $orgId")
            val sql = """
                UPDATE ${ORGANIZATIONS.name}
                SET ${ORGANIZATION.name} = ${ORGANIZATION.name}::jsonb #- ('{"' || ? || '"}')::text[]
                WHERE ${ID.name} = ?
            """.trimIndent()
            val update = toolbox.hds.connection.use { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, META_ESIDS)
                    ps.setObject(2, orgId)
                    ps.executeUpdate()
                }
            }
            return update == 1
        }
    }

    override fun getSupportedVersion(): Long {
        return Version.V2021_07_23.value
    }
}
