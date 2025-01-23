package org.jetbrains.exposed.sql.tests.r2dbc

import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.jetbrains.exposed.sql.tests.shared.assertEquals
import org.junit.Assume
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@Suppress("UnusedPrivateProperty")
class R2dbcConnectionTests : DatabaseTestsBase() {
    // PROBLEM 1: SQL Server, Oracle, & PostgreSQL
    // Including these only succeeds if JDK of module is bumped to JDK11,
    // which causes issues with other modules that depending on testing module.
    private val jdk11RequiredDb = setOf(TestDB.SQLSERVER, TestDB.ORACLE, TestDB.POSTGRESQL)

    private val r2dbcSupportedDb = TestDB.ALL_H2_V2 + TestDB.ALL_MYSQL_MARIADB // + jdk11RequiredDb

    // PROBLEM 2: H2 (example)
    // As confirmed in testLowLevelR2dbcConnection() below, the test is being run on the R2DBC driver
    // which has an older version than the version specified in libs.version.toml.
    // This is ok, because it is a test for R2DBC, but it has a side-effect of affecting existing JDBC tests.
    // If testJdbcConnectionUsesJdbcDriver() is run alone, it fails because it is using the older dependency.
    // If the R2DBC dependency is removed from the build file, the test then passes.
    // This is what causes tests in SelectTests.kt & ArrayColumnTypeTests.kt to fail now.

    // PROBLEM 3: withR2dbcDb()
    // This function was created to give access to a R2dbcTransaction block, which requires calling from
    // a suspend function. It would be great if the original withDB() and its variants could still be used
    // for both JDBC and R2DBC tests, but maybe that isn't logical.

    @Test
    fun testConnectionWithExec01() = runTest {
        withR2dbcDb(r2dbcSupportedDb) {
            val extra = if (it == TestDB.ORACLE) { " FROM DUAL" } else { "" }
            exec("SELECT 13, 'Hello World'$extra") {
                (it.get(0) as Number).toInt() to it.get(1)
            }?.collect {
                assertNotNull(it)
                assertEquals(13, it.first)
                assertEquals("Hello World", it.second)
            }
        }
    }

    @Test
    fun testConnectionWithExec02() = runTest {
        withR2dbcDb(r2dbcSupportedDb) {
            val dataType = if (it == TestDB.ORACLE) "VARCHAR2(256)" else "TEXT"
            exec("CREATE TABLE TESTER (AMOUNT INT NOT NULL, WORDS $dataType NOT NULL)")

            exec("INSERT INTO TESTER (AMOUNT, WORDS) VALUES (13, 'Hello World')") {
                (it.get(0) as Number).toInt() to it.get(1)
            }?.collect {
                assertNotNull(it)
                assertEquals(13, it)
                assertEquals("Hello World", it.second)
            }

            exec("DROP TABLE TESTER")
        }
    }

    @Test
    fun testJdbcConnectionUsesJdbcDriver() {
        withDb(listOf(TestDB.H2_V2)) {
            val version = exec("SELECT H2VERSION();") {
                it.next()
                it.getString(1)
            }

            assertEquals("2.2.224", version)
        }
    }

    @Test
    fun testLowLevelR2dbcConnection() {
        Assume.assumeTrue(TestDB.H2_V2 in TestDB.enabledDialects())

        val options = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "h2")
            .option(ConnectionFactoryOptions.PROTOCOL, "mem")
            .option(ConnectionFactoryOptions.DATABASE, "regular;DB_CLOSE_DELAY=-1;")
            .build()
        val cxFactory = ConnectionFactories.get(options)
        val cxPublisher = cxFactory.create()
        val version = runBlocking {
            flow {
                val stmt = cxPublisher.awaitSingle().createStatement("SELECT H2VERSION()")
                stmt
                    .execute()
                    .collect { result ->
                        result
                            .map { row, _ -> row.get(0).toString() }
                            .collect { emit(it) }
                    }
            }.single()
        }
        assertEquals("2.1.214", version)
    }
}
