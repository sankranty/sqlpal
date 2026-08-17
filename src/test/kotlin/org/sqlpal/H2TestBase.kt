package org.sqlpal

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.sqlpal.SqlPal

/**
 * Shared base for integration tests that need a real JDBC connection.
 *
 * Each subclass gets its own in-memory H2 database in PostgreSQL compatibility
 * mode (`MODE=PostgreSQL`), named after the test class so tests in different
 * classes don't see each other's tables.
 *
 * `DB_CLOSE_DELAY=-1` keeps the in-memory DB alive across pool connections;
 * the pool itself is closed in [tearDown].
 *
 * Subclasses implement [createSchema] which runs once before any test in the
 * class. Use [exec] for ad-hoc DDL or for clearing state in `@AfterEach`.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class H2TestBase {

    protected lateinit var ds: HikariDataSource

    @BeforeAll
    fun bootstrap() {
        val dbName = "${javaClass.simpleName}_${System.nanoTime()}"
        ds = HikariDataSource().apply {
            jdbcUrl = "jdbc:h2:mem:$dbName;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
            maximumPoolSize = 4
        }
        SqlPal.setDataSource(ds)
        createSchema()
    }

    @AfterAll
    fun tearDown() {
        ds.close()
    }

    /** Subclass hook: create the tables this test class needs. */
    protected abstract fun createSchema()

    /** Execute raw SQL via the test data source — for DDL and bulk cleanup. */
    protected fun exec(sql: String) {
        ds.connection.use { conn ->
            conn.createStatement().use { it.execute(sql) }
        }
    }
}
