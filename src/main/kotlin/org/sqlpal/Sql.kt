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

/** Wrapper for the [List] that also contains information about generic type of the [List]. */
class ListAndType (val list: List<*>, val componentType: KClass<*>)

/** Wraps [List] with an object that also contains information about generic type of the [List].
 * It's necessary to handle empty lists, as unlike [Array], empty [List] does not contain
 * information about its generic type, what makes impossible to map it to appropriate SQL type. */
inline operator fun <reified T> List<T>?.unaryMinus() =
    if (this != null) ListAndType(this, T::class) else null

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
 * To inline value directly into the query string (instead of adding it as a bind parameter), use $[I]$ instead of $. */
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
            // All other values add as a binding parameters.
            else if (!handleInWithCollection(params[i], strings[i], builder, bindParams))
            {
                if (params[i] is List<*>) throw SqlInterpolatorException(
                    "Parameters of the List type, specified in the query, must be prefixed with the '-'. " +
                        "Unary minus operator is overloaded by SqlPal and converts List to typed Array." +
                        "It's necessary to handle empty Lists, because unlike Array, empty List does not contain " +
                        "information about its generic type, what makes impossible to map it to appropriate SQL type." +
                        "The only case when '-' prefix is not required, is when list is specified after IN operator, " +
                        "as list is unfolded into values in this case.")
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
        // Check that value is some kind of collection. Arrays don't have base type, so use isArray.
        if (!(value is List<*> || value is ListAndType || value::class.java.isArray))
            return false

        // Check that there is IN operator right before value.
        if (!finishesWithIN(str))
            return false

        val items = getItems(if (value is ListAndType) value.list else value)

        builder.append('(')
        for (item in items.iterator) {
            builder.append("?,")
            bindParams.add(item)
        }
        if (builder.endsWith(',')) // Check for case of empty collection.
            builder.deleteCharAt(builder.length - 1) // Remove trailing comma
        builder.append(')')
        return true
    }

    private fun finishesWithIN(str: String): Boolean {
        var i = str.length
        while (i > 0)
            if (!str[--i].isWhitespace()) {
                return i > 1 && (str[i - 1] == 'I' || str[i - 1] == 'i') && (str[i] == 'N' || str[i] == 'n')
            }
        return false
    }
}