package org.sqlpal

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Tests the LIKE convenience helpers (`includes`, `beginsWith`, `finishesWith`
 * and their `…IgnoreCase` variants) through their target usage: embedded inside
 * a `-"..."` Sql string, e.g.
 *
 * ```
 * read<Person>(-"... WHERE ${"name" beginsWithIgnoreCase "Mic"}")
 * ```
 *
 * Each helper produces a `Like` that the interpolator turns into a `col like ?
 * escape '!'` fragment plus a single bind parameter holding the (escaped) pattern.
 * We assert on the generated `Query.sql` and `Query.bindParams`.
 */
class LikeTest {

    /** The single LIKE pattern bound by the query. */
    private fun pattern(q: Query): String = q.bindParams.single() as String

    // ---------- condition fragment (case sensitivity) ----------

    @Test fun `case-sensitive helper does not wrap the column`() {
        val q = -"WHERE ${"name" includes "x"}"
        assertEquals("WHERE name like ? escape '!' ", q.sql)
    }

    @Test fun `ignoreCase helper wraps the column in lower()`() {
        val q = -"WHERE ${"name" includesIgnoreCase "x"}"
        assertEquals("WHERE lower(name) like ? escape '!' ", q.sql)
    }

    // ---------- pattern shape ----------

    @Test fun `includes surrounds the text with percent`() {
        assertEquals("%foo%", pattern(-"WHERE ${"col" includes "foo"}"))
    }

    @Test fun `beginsWith appends a trailing percent only`() {
        assertEquals("foo%", pattern(-"WHERE ${"col" beginsWith "foo"}"))
    }

    @Test fun `finishesWith prepends a leading percent only`() {
        assertEquals("%foo", pattern(-"WHERE ${"col" finishesWith "foo"}"))
    }

    @Test fun `ignoreCase variants lowercase the text body`() {
        assertEquals("%foo%", pattern(-"WHERE ${"col" includesIgnoreCase "Foo"}"))
        assertEquals("bar%", pattern(-"WHERE ${"col" beginsWithIgnoreCase "BAR"}"))
        assertEquals("%baz", pattern(-"WHERE ${"col" finishesWithIgnoreCase "BaZ"}"))
    }

    @Test fun `case-sensitive variants preserve original casing`() {
        assertEquals("%Foo%", pattern(-"WHERE ${"col" includes "Foo"}"))
        assertEquals("BAR%", pattern(-"WHERE ${"col" beginsWith "BAR"}"))
    }

    // ---------- wildcard escaping ----------

    @Test fun `percent in text is escaped`() {
        assertEquals("%10!%%", pattern(-"WHERE ${"col" includes "10%"}"))
    }

    @Test fun `underscore in text is escaped`() {
        assertEquals("a!_b%", pattern(-"WHERE ${"col" beginsWith "a_b"}"))
    }

    @Test fun `the escape char itself is escaped`() {
        // '!' is the escape character, so a literal '!' in the text is doubled.
        assertEquals("%a!!b%", pattern(-"WHERE ${"col" includes "a!b"}"))
    }

    @Test fun `backslash in text is treated as an ordinary character`() {
        // Backslash is no longer the escape char, so it passes through unescaped.
        assertEquals("%a\\b%", pattern(-"WHERE ${"col" includes "a\\b"}"))
    }

    @Test fun `mixed wildcards and case-insensitive together`() {
        assertEquals("%100!%!_discount%", pattern(-"WHERE ${"col" includesIgnoreCase "100%_Discount"}"))
    }

    @Test fun `empty text yields just the wildcards`() {
        assertEquals("%%", pattern(-"WHERE ${"col" includes ""}"))
        assertEquals("%", pattern(-"WHERE ${"col" beginsWith ""}"))
        assertEquals("%", pattern(-"WHERE ${"col" finishesWith ""}"))
    }

    // ---------- composition with the rest of a query ----------

    @Test fun `Like composes with other conditions and bind params`() {
        val id = 5
        val q = -"SELECT * FROM pers WHERE id > $id and ${"name" beginsWithIgnoreCase "Mic"}"
        assertEquals("SELECT * FROM pers WHERE id > ? and lower(name) like ? escape '!' ", q.sql)
        assertEquals(listOf<Any?>(5, "mic%"), q.bindParams)
    }
}
