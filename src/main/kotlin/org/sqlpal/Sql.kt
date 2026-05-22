package org.sqlpal

import io.exoquery.terpal.Interpolator
import io.exoquery.terpal.InterpolatorFunction
import io.exoquery.terpal.interpolatorBody
import kotlin.reflect.*

/** Used in [Sql] string and means - if value of next param is true, then continue normally,
 * otherwise skip to the line break. See [Sql] for details. */
object If

/** Used in [Sql] string and means - if condition of the [If] is true, then skip to the line break,
 * otherwise continue normally. See [Sql] for details. */
object Else

/** Used in [Sql] string and means - inline value of next param directly into the string
 * instead of adding it as a binding parameter. See [Sql] for details. */
object I

/** Exception that signals incorrect placement of the specified parameters. */
class SqlInterpolatorException(message: String) : Exception(message)

/** Wrapper for the [Collection] that also contains information about generic type of the [Collection]. */
class CollectionAndType (val list: Any, val componentType: KClass<*>)

/** Wraps [Collection] with an object that also contains information about generic type of the [Collection].
 * It's necessary to handle empty collections, as unlike [Array], empty [Collection] does not contain
 * information about its generic type, what makes impossible to map it to appropriate SQL type. */
inline operator fun <reified T> Collection<T>?.unaryMinus() =
    if (this != null) CollectionAndType(this, T::class) else null

/** Allows to use more compact -"..." syntax instead of Sql("...") syntax. */
@InterpolatorFunction<Sql>(Sql::class)
operator fun String.unaryMinus(): Query = interpolatorBody()

/** Stores interpolated values from the provided String as a bind parameters
 * and returns [Query] object that can be used to execute provided query.
 * It can be used with Sql("...") or Sql("""...""") syntax, as well as with more compact -"..." or -"""...""" syntax.
 *
 * To include part of the query conditionally use $[If] $condition where 'condition' is boolean variable.
 * If 'condition' is false, then the rest of the content to the line break is not included.
 * For several mutually exclusive conditions use $[Else]$[If] and $[Else] which also have scope upto the line break.
 *
 * To inline value directly into the query string (instead of adding it as a bind parameter), use $[I]$ instead of $.
 *
 * For common LIKE conditions you can use next convenience functions for more compact syntax:
 * - [includes] and [includesIgnoreCase],
 * - [beginsWith] and [beginsWithIgnoreCase],
 * - [finishesWith] and [finishesWithIgnoreCase], e.g.:
 * ```
 * read<Person>(-"SELECT * FROM pers WHERE id > $id and ${"name" beginsWithIgnoreCase "Mic"}")
 * ```*/
object Sql: Interpolator<Any, Query> {

    override fun interpolate(parts: () -> List<String>, params: () -> List<Any>): Query {
        val strings = parts()
        val params = params()

        val builder = StringBuilder()
        val bindParams = mutableListOf<Any?>()
        var i = 0 // Index of parameter and string before it.
        var isTrue = false // To know If condition in the Else block.

        builder.append(strings[i]) // There is always a string before the first parameter, even when the parameter is at the very beginning.
        while (i < params.count()) {
            if (params[i] == If) {
                i++ // Move to the condition parameter and blank string between 'If' and condition parameter.
                isTrue = paramValueIsTrue(i, params)
                if (!isTrue) {
                    i = skipToLineBreakAndAppendRestOfString(i, strings, builder)
                    continue
                }
            }
            else if (params[i] == Else) {
                if (isTrue) {
                    i = skipToLineBreakAndAppendRestOfString(i, strings, builder)
                    continue
                }
            }
            else if (params[i] == I) {
                i++  // Move to the parameter after 'I'.
                builder.append(params[i]) // inline value of the param directly into the string instead of adding it as a binding parameter.
            }
            // All other values add as binding parameters.
            else if (params[i] is Like) {
                val like = params[i] as Like
                like.appendLikeCondition(builder)
                bindParams.add(like.getLikePattern())
            }
            else if (!handleInWithCollection(params[i], strings[i], builder, bindParams))
            {
                if (params[i] is Collection<*>) throw SqlInterpolatorException(
                    "Parameters of the Collection type, specified in the query, must be prefixed with the '-'. " +
                        "Unary minus operator is overloaded by SqlPal and converts Collection to typed Array." +
                        "It's necessary to handle empty Collections, because unlike Array, empty Collection does not contain " +
                        "information about its generic type, what makes impossible to map it to appropriate SQL type." +
                        "The only case when '-' prefix is not required, is when collection is specified after IN operator, " +
                        "as collection is unfolded into values in this case.")
                builder.append('?')
                bindParams.add(params[i])
            }
            i++
            builder.append(strings[i]) // Append string after parameter.
        }
        return Query(builder.toString(), bindParams)
    }

    private fun paramValueIsTrue(paramIndex: Int, params: List<Any>): Boolean {
        if (paramIndex >= params.count())
            throw SqlInterpolatorException("'If' parameter can't be the last one. Add boolean parameter right after it.")
        if (params[paramIndex] !is Boolean)
            throw SqlInterpolatorException("Next parameter after the 'If' must be of Boolean type.")

        return (params[paramIndex] as Boolean)
    }

    // Looks for string with line break starting from the string next to 'from' index.
    // If found, then part of the string after line break is appended to StringBuilder.
    // Returns index of the string that contains line break, or index of the last string if no line break found.
    private fun skipToLineBreakAndAppendRestOfString(from: Int, strings: List<String>, builder: StringBuilder): Int {
        var i = from + 1
        while (i < strings.count()) {
            val breakIndex = strings[i].indexOf('\n')
            if (breakIndex >= 0) {
                builder.append(strings[i], breakIndex + 1, strings[i].length)
                return i
            }
            i++
        }
        return i
    }

    private fun handleInWithCollection(value: Any, str: String,
                                       builder: StringBuilder, bindParams: MutableList<Any?>): Boolean {
        // Check that the value is some kind of collection. Arrays don't have base type, so use isArray.
        if (!(value is Collection<*> || value is CollectionAndType || value::class.java.isArray))
            return false

        // Check that there is IN operator right before the value.
        val inKind = finishesWithIN(str)
        if (inKind == InKind.None)
            return false

        val items = getItems(if (value is CollectionAndType) value.list else value)

        builder.append('(')
        if (items.size > 0) {
            for (item in items.iterator) {
                builder.append("?,")
                bindParams.add(item)
            }
            builder.deleteCharAt(builder.length - 1) // Remove trailing comma
        } else if (inKind == InKind.In)
            builder.append("NULL") // column IN (NULL) → matches nothing
        // For NOT IN condition "NOT IN ()" will be generated, what will produce SQL error.
        // It's documented that for such case $If condition should be added as we can't handle it universally.
        builder.append(')')
        return true
    }

    private fun finishesWithIN(str: String): InKind {
        var i = str.length - 1
        while (i > 0 && str[i].isWhitespace()) i-- // Skip whitespaces between IN and ()

        val inPresents = i >= 2 && str[i - 2].isWhitespace() &&
                (str[i - 1] == 'I' || str[i - 1] == 'i') &&
                (str[i] == 'N' || str[i] == 'n')
        if (!inPresents) return InKind.None

        i -= 2
        while (i > 0 && str[i].isWhitespace()) i-- // Skip whitespaces between NOT and IN

        return if (i >= 3 && str[i - 3].isWhitespace() &&
                (str[i - 2] == 'N' || str[i - 2] == 'n') &&
                (str[i - 1] == 'O' || str[i - 1] == 'o') &&
                (str[i] == 'T' || str[i] == 't')) InKind.NotIn else InKind.In
    }

    private enum class InKind { In, NotIn, None }
}