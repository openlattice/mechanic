package com.openlattice.mechanic.upgrades

import com.google.common.util.concurrent.ListeningExecutorService
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(PermissionsTableMigration::class.java)

class PermissionsTableMigration(
        private val hds: HikariDataSource,
        private val executor: ListeningExecutorService
) {

    fun addExpirationDateCol() {
        hds.connection.use {
            it.createStatement().execute("alter table permissions add column expiration_date timestamp with time zone;")
        }
    }
}