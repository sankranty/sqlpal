package org.sqlpal

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.sqlpal.bool
import org.sqlpal.boolVal
import org.sqlpal.byte
import org.sqlpal.byteVal
import org.sqlpal.date
import org.sqlpal.dec
import org.sqlpal.double
import org.sqlpal.doubleVal
import org.sqlpal.dt
import org.sqlpal.enum
import org.sqlpal.enumVal
import org.sqlpal.float
import org.sqlpal.floatVal
import org.sqlpal.int
import org.sqlpal.intVal
import org.sqlpal.long
import org.sqlpal.longVal
import org.sqlpal.odt
import org.sqlpal.otime
import org.sqlpal.query.read
import org.sqlpal.short
import org.sqlpal.shortVal
import org.sqlpal.str
import org.sqlpal.time
import org.sqlpal.unaryMinus
import org.sqlpal.zdt
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Tests for the convenience ResultSet extension functions exposed to user-defined
 * createItem callbacks (the second argument to `read(query) { rs -> ... }`).
 */
class ResultSetExtTest : H2TestBase() {

    enum class Flavor { Sweet, Sour, Salty }

    override fun createSchema() {
        exec("""
            CREATE TABLE rs_ext (
                id BIGINT PRIMARY KEY,
                col_int INT,
                col_long BIGINT,
                col_byte SMALLINT,
                col_short SMALLINT,
                col_double DOUBLE,
                col_float REAL,
                col_bool BOOLEAN,
                col_str VARCHAR(255),
                col_dec DECIMAL(20, 4),
                col_date DATE,
                col_time TIME,
                col_otime TIME WITH TIME ZONE,
                col_dt TIMESTAMP,
                col_odt TIMESTAMP WITH TIME ZONE,
                col_flavor VARCHAR(16)
            )
        """.trimIndent())
    }

    @AfterEach fun clear() { exec("DELETE FROM rs_ext") }

    private fun insertRow(
        id: Long,
        int: Int? = 1, long: Long? = 2L, byte: Byte? = 3, short: Short? = 4,
        double: Double? = 5.5, float: Float? = 6.5f, bool: Boolean? = true,
        str: String? = "hi", dec: BigDecimal? = BigDecimal("9.5"),
        date: LocalDate? = LocalDate.of(2024, 1, 2),
        time: LocalTime? = LocalTime.of(12, 30),
        otime: OffsetDateTime? = OffsetDateTime.of(2024, 1, 2, 12, 30, 0, 0, ZoneOffset.UTC),
        dt: LocalDateTime? = LocalDateTime.of(2024, 1, 2, 12, 30, 45),
        odt: OffsetDateTime? = OffsetDateTime.of(2024, 1, 2, 12, 30, 0, 0, ZoneOffset.UTC),
        flavor: String? = "Sweet",
    ) {
        ds.connection.use { c ->
            c.prepareStatement("""
                INSERT INTO rs_ext (id, col_int, col_long, col_byte, col_short, col_double, col_float, col_bool,
                                    col_str, col_dec, col_date, col_time, col_otime, col_dt, col_odt, col_flavor)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """.trimIndent()).use {
                it.setLong(1, id)
                it.setObject(2, int)
                it.setObject(3, long)
                it.setObject(4, byte)
                it.setObject(5, short)
                it.setObject(6, double)
                it.setObject(7, float)
                it.setObject(8, bool)
                it.setObject(9, str)
                it.setObject(10, dec)
                it.setObject(11, date)
                it.setObject(12, time)
                it.setObject(13, otime?.toOffsetTime())
                it.setObject(14, dt)
                it.setObject(15, odt)
                it.setObject(16, flavor)
                it.executeUpdate()
            }
        }
    }

    @Test fun `not-null Val extensions read primitives by name`() {
        insertRow(id = 1L)
        val data = read(-"SELECT * FROM rs_ext WHERE id = ${1L}") { rs ->
            mapOf(
                "int" to (rs intVal "col_int"),
                "long" to (rs longVal "col_long"),
                "byte" to (rs byteVal "col_byte"),
                "short" to (rs shortVal "col_short"),
                "double" to (rs doubleVal "col_double"),
                "float" to (rs floatVal "col_float"),
                "bool" to (rs boolVal "col_bool"),
            )
        }.single()
        assertEquals(1, data["int"])
        assertEquals(2L, data["long"])
        assertEquals(3.toByte(), data["byte"])
        assertEquals(4.toShort(), data["short"])
        assertEquals(5.5, data["double"])
        assertEquals(6.5f, data["float"])
        assertEquals(true, data["bool"])
    }

    @Test fun `nullable extensions return null on null column`() {
        insertRow(
            id = 2L,
            int = null, long = null, byte = null, short = null,
            double = null, float = null, bool = null, str = null, dec = null,
            date = null, time = null, otime = null, dt = null, odt = null, flavor = null,
        )
        val data = read(-"SELECT * FROM rs_ext WHERE id = ${2L}") { rs ->
            listOf(
                rs int "col_int",
                rs long "col_long",
                rs byte "col_byte",
                rs short "col_short",
                rs double "col_double",
                rs float "col_float",
                rs bool "col_bool",
                rs str "col_str",
                rs dec "col_dec",
                rs date "col_date",
                rs time "col_time",
                rs otime "col_otime",
                rs dt "col_dt",
                rs odt "col_odt",
                rs zdt "col_odt",
                rs.enum<Flavor>("col_flavor"),
            )
        }.single()
        data.forEach { assertNull(it) }
    }

    @Test fun `str, dec, date, time, dt, odt, zdt read complex types`() {
        insertRow(id = 3L)
        val data = read(-"SELECT * FROM rs_ext WHERE id = ${3L}") { rs ->
            mapOf(
                "str" to (rs str "col_str"),
                "dec" to (rs dec "col_dec"),
                "date" to (rs date "col_date"),
                "time" to (rs time "col_time"),
                "dt" to (rs dt "col_dt"),
                "odt" to (rs odt "col_odt"),
                "zdt" to (rs zdt "col_odt"),
            )
        }.single()
        assertEquals("hi", data["str"])
        assertEquals(0, BigDecimal("9.5").compareTo(data["dec"] as BigDecimal))
        assertEquals(LocalDate.of(2024, 1, 2), data["date"])
        assertEquals(LocalTime.of(12, 30), data["time"])
        assertEquals(LocalDateTime.of(2024, 1, 2, 12, 30, 45), data["dt"])
        val expectedOdt = OffsetDateTime.of(2024, 1, 2, 12, 30, 0, 0, ZoneOffset.UTC)
        assertEquals(expectedOdt, data["odt"])
        // zdt is built from the same column as odt; only the source matters here.
        assertEquals(expectedOdt.toZonedDateTime(), data["zdt"])
    }

    @Test fun `enum extension reads nullable enum`() {
        insertRow(id = 4L, flavor = "Salty")
        val flavor = read(-"SELECT * FROM rs_ext WHERE id = ${4L}") { rs ->
            rs.enum<Flavor>("col_flavor")
        }.single()
        assertEquals(Flavor.Salty, flavor)
    }

    @Test fun `enumVal extension reads non-null enum`() {
        insertRow(id = 5L, flavor = "Sour")
        val flavor = read(-"SELECT * FROM rs_ext WHERE id = ${5L}") { rs ->
            rs.enumVal<Flavor>("col_flavor")
        }.single()
        assertEquals(Flavor.Sour, flavor)
    }
}
