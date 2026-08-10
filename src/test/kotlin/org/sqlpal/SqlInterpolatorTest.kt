package org.sqlpal

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame

/**
 * Unit tests for the [Sql] string interpolator.
 *
 * The interpolator's contract: `parts()` returns N strings, `params()` returns N-1 values.
 * `parts[0]` is the literal text before the first param, `parts[i]` is the text between
 * params[i-1] and params[i], and the last entry is the literal text after the final param.
 *
 * Tests construct these lists directly instead of relying on the Terpal compiler plugin,
 * to keep the unit isolated.
 */
class SqlInterpolatorTest {

    private fun interp(parts: List<String>, params: List<Any>): Query =
        Sql.interpolate({ parts }, { params })

    // ---------- Basic bind parameters ----------

    @Test fun `no params produces literal sql`() {
        val q = interp(listOf("SELECT 1"), emptyList())
        assertEquals("SELECT 1", q.sql)
        assertEquals(0, q.bindParams.size)
    }

    @Test fun `single bind param`() {
        val q = interp(listOf("WHERE id = ", ""), listOf(42))
        assertEquals("WHERE id = ?", q.sql)
        assertEquals(listOf<Any?>(42), q.bindParams)
    }

    @Test fun `multiple bind params`() {
        val q = interp(listOf("a = ", " AND b = ", ""), listOf("x", 5))
        assertEquals("a = ? AND b = ?", q.sql)
        assertEquals(listOf<Any?>("x", 5), q.bindParams)
    }

    @Test fun `param at start of string`() {
        val q = interp(listOf("", " < 10"), listOf(3))
        assertEquals("? < 10", q.sql)
    }

    @Test fun `consecutive params`() {
        val q = interp(listOf("", "", ""), listOf(1, 2))
        assertEquals("??", q.sql)
        assertEquals(listOf<Any?>(1, 2), q.bindParams)
    }

    // ---------- $If / $Else ----------

    @Test fun `If with true condition keeps content to line break`() {
        // "SELECT * FROM x $If $cond WHERE a = b\nORDER BY id"
        val q = interp(
            listOf("SELECT * FROM x ", " ", " WHERE a = b\nORDER BY id"),
            listOf(If, true)
        )
        // String between If and the condition (parts[1]=" ") is intentionally elided.
        assertEquals("SELECT * FROM x  WHERE a = b\nORDER BY id", q.sql)
        assertEquals(0, q.bindParams.size)
    }

    @Test fun `If with false condition skips to line break`() {
        val q = interp(
            listOf("SELECT * FROM x ", " ", " WHERE a = b\nORDER BY id"),
            listOf(If, false)
        )
        assertEquals("SELECT * FROM x ORDER BY id", q.sql)
    }

    @Test fun `If false with no line break in remainder skips everything`() {
        val q = interp(
            listOf("SELECT 1 ", " ", " AND x = 1"),
            listOf(If, false)
        )
        // Nothing after $cond contains '\n', so the rest of the template is dropped.
        assertEquals("SELECT 1 ", q.sql)
    }

    @Test fun `If with non-boolean condition throws`() {
        assertFailsWith<SqlInterpolatorException> {
            interp(listOf("", " ", ""), listOf(If, "not boolean"))
        }
    }

    @Test fun `If as final param throws`() {
        assertFailsWith<SqlInterpolatorException> {
            interp(listOf("a ", ""), listOf(If))
        }
    }

    @Test fun `If with true and a bind param inside keeps the param`() {
        // "SELECT $If $c x = $v\nLIMIT 1"
        val q = interp(
            listOf("SELECT ", " ", " x = ", "\nLIMIT 1"),
            listOf(If, true, 7)
        )
        assertEquals("SELECT  x = ?\nLIMIT 1", q.sql)
        assertEquals(listOf<Any?>(7), q.bindParams)
    }

    @Test fun `If true Else skips Else content`() {
        // "a $If $c x\n$Else y\nrest"
        val q = interp(
            listOf("a ", " ", " x\n", " y\nrest"),
            listOf(If, true, Else)
        )
        assertEquals("a  x\nrest", q.sql)
    }

    @Test fun `If false Else keeps Else content`() {
        val q = interp(
            listOf("a ", " ", " x\n", " y\nrest"),
            listOf(If, false, Else)
        )
        assertEquals("a  y\nrest", q.sql)
    }

    @Test fun `If false Else If picks the next branch`() {
        // "a $If $c1 b\n$Else$If $c2 d\nrest"
        val q = interp(
            listOf("a ", " ", " b\n", "", " ", " d\nrest"),
            listOf(If, false, Else, If, true)
        )
        assertEquals("a  d\nrest", q.sql)
    }

    // ---------- $I (inlined literal) ----------

    @Test fun `I inlines the next value directly into the SQL`() {
        // "LIMIT $I${100}"
        val q = interp(listOf("LIMIT ", "", ""), listOf(I, 100))
        assertEquals("LIMIT 100", q.sql)
        assertEquals(0, q.bindParams.size)
    }

    @Test fun `I followed by bind param mixes inline and bind`() {
        // "ORDER BY $I${"id"}\nWHERE x = $y"
        val q = interp(
            listOf("ORDER BY ", "", "\nWHERE x = ", ""),
            listOf(I, "id", 5)
        )
        assertEquals("ORDER BY id\nWHERE x = ?", q.sql)
        assertEquals(listOf<Any?>(5), q.bindParams)
    }

    // ---------- Like ----------

    @Test fun `Like becomes condition with bind param`() {
        val q = interp(listOf("WHERE ", ""), listOf("name" includesIgnoreCase "Mic"))
        assertEquals("WHERE lower(name) like ? escape '!' ", q.sql)
        assertEquals(1, q.bindParams.size)
        assertEquals("%mic%", q.bindParams[0])
    }

    @Test fun `case sensitive Like does not lowercase column`() {
        val q = interp(listOf("WHERE ", ""), listOf("name" includes "Foo"))
        assertEquals("WHERE name like ? escape '!' ", q.sql)
        assertEquals("%Foo%", q.bindParams[0])
    }

    // ---------- IN with collections / arrays ----------

    @Test fun `IN unfolds a List`() {
        val q = interp(listOf("WHERE x IN ", ""), listOf(listOf(1, 2, 3)))
        assertEquals("WHERE x IN (?,?,?)", q.sql)
        assertEquals(listOf<Any?>(1, 2, 3), q.bindParams)
    }

    @Test fun `IN unfolds a typed Array`() {
        val q = interp(listOf("WHERE x IN ", ""), listOf(arrayOf(1, 2)))
        assertEquals("WHERE x IN (?,?)", q.sql)
        assertEquals(listOf<Any?>(1, 2), q.bindParams)
    }

    @Test fun `IN unfolds a CollectionAndType wrapper`() {
        val coll = CollectionAndType(listOf("a", "b"), String::class)
        val q = interp(listOf("WHERE x IN ", ""), listOf(coll))
        assertEquals("WHERE x IN (?,?)", q.sql)
        assertEquals(listOf<Any?>("a", "b"), q.bindParams)
    }

    @Test fun `IN detection is case insensitive`() {
        val q = interp(listOf("WHERE x in ", ""), listOf(listOf(1)))
        assertEquals("WHERE x in (?)", q.sql)
    }

    @Test fun `IN unfolds primitive array`() {
        val q = interp(listOf("WHERE x IN ", ""), listOf(intArrayOf(7, 8, 9)))
        assertEquals("WHERE x IN (?,?,?)", q.sql)
    }

    @Test fun `IN is detected when the fragment is exactly the IN keyword (B2)`() {
        // Mimics a dynamically-inlined left operand (e.g. $I$col) so the fragment before
        // the collection is just " IN ". After the B2 fix this is detected and unfolded.
        val q = interp(listOf("", " IN ", ""), listOf("x", listOf(1, 2)))
        assertEquals("? IN (?,?)", q.sql)
        assertEquals(listOf<Any?>("x", 1, 2), q.bindParams)
    }

    @Test fun `empty collection after IN becomes IN (NULL) — matches nothing`() {
        val q = interp(listOf("WHERE x IN ", ""), listOf(emptyList<Int>()))
        assertEquals("WHERE x IN (NULL)", q.sql)
        assertEquals(0, q.bindParams.size)
    }

    @Test fun `empty collection after IN works case-insensitively`() {
        val q = interp(listOf("WHERE x in ", ""), listOf(emptyList<Int>()))
        assertEquals("WHERE x in (NULL)", q.sql)
    }

    /**
     * An empty NOT IN can't be expressed universally (`NOT IN (NULL)` would match
     * nothing instead of everything), so the interpolator leaves `NOT IN ()` — which
     * errors at the DB. Documented: callers guard an empty NOT IN with `$If`.
     */
    @Test fun `empty collection after NOT IN is left as empty parens (documented)`() {
        val q = interp(listOf("WHERE x NOT IN ", ""), listOf(emptyList<Int>()))
        assertEquals("WHERE x NOT IN ()", q.sql)
        assertEquals(0, q.bindParams.size)
    }

    @Test fun `Collection not after IN and without minus prefix throws`() {
        assertFailsWith<SqlInterpolatorException> {
            interp(listOf("WHERE x = ", ""), listOf(listOf(1, 2)))
        }
    }

    @Test fun `CollectionAndType outside IN is treated as a single bind param`() {
        // For an equality comparison against a CollectionAndType (e.g. PostgreSQL array equality),
        // the wrapper is bound as a single param, not unfolded.
        val coll = CollectionAndType(listOf(1, 2), Int::class)
        val q = interp(listOf("WHERE x = ", ""), listOf(coll))
        assertEquals("WHERE x = ?", q.sql)
        assertSame(coll, q.bindParams[0])
    }

    // ---------- unaryMinus operator ----------

    @Test fun `unaryMinus wraps a Collection with element-type info`() {
        val list = listOf(1, 2, 3)
        val wrapped = -list
        assertNotNull(wrapped)
        assertSame(list, wrapped.list)
        assertEquals(Int::class, wrapped.componentType)
    }

    @Test fun `unaryMinus on null collection yields null`() {
        val list: List<Int>? = null
        val wrapped = -list
        assertNull(wrapped)
    }
}
