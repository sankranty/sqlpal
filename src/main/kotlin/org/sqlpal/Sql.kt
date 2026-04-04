package org.sqlpal

import io.exoquery.terpal.Interpolator
import io.exoquery.terpal.InterpolatorFunction
import io.exoquery.terpal.interpolatorBody
import kotlin.reflect.*

/** 'If' means - if value of next param is true, then continue normally, otherwise skip to line break. */
object If

/** 'I' means - inline value of next param directly into string instead of adding it as binding parameter. */
object I

/** Exception that signals incorrect placement of parameters. */
class SqlInterpolatorException(message: String) : Exception(message)

/** Wrapper for [List] that also contains information about generic type of the [List]. */
class ListAndType (val list: List<*>, val componentType: KClass<*>)

/** Wraps [List] with object that also contains information about generic type of the [List].
 * It's necessary to handle empty lists, as unlike [Array], empty [List] does not contain
 * information about its generic type, what makes impossible to map it to appropriate SQL type. */
inline operator fun <reified T> List<T>?.unaryMinus() =
    if (this != null) ListAndType(this, T::class) else null

/** Allows to use more compact -"..." syntax instead of Sql("...") syntax. */
@InterpolatorFunction<Sql>(Sql::class)
operator fun String.unaryMinus(): Query = interpolatorBody()

/** Stores interpolated values from provided String as bind parameters
 * and returns [Query] object that can be used to execute provided query.
 * It can be used with Sql("...") or Sql("""...""") syntax, as well as with more compact -"..." or -"""...""" syntax.
 *
 * To conditionally exclude part of query use $[If] $condition where 'condition' is boolean variable.
 * If 'condition' is false, then rest of content to line break is not included.
 *
 * To inline parameter directly into string (instead of extracting it as bind parameter), use $[I]$ instead of $. */
object Sql: Interpolator<Any, Query> {

    override fun interpolate(parts: () -> List<String>, params: () -> List<Any>): Query {
        val strings = parts()
        val params = params()

        val builder = StringBuilder()
        val bindParams = mutableListOf<Any?>()
        var i = 0

        builder.append(strings[i])
        while (i < params.count()) {
            // 'If' means - if value of next param is true then continue normally, otherwise skip to line break.
            if (params[i] == If) {
                i++ // Skip blank string between 'If' and condition parameter.
                if (paramIsFalse(i, params)) {
                    val (strIndex, strAfterLineBreak) = skipToLineBreak(i, strings)
                    builder.append(strAfterLineBreak)
                    i = strIndex + 1
                    continue
                }
            }
            // 'I' means - inline value of next param directly to string instead of adding it as binding parameter.
            else if (params[i] == I) {
                i++  // Skip blank string between 'I' and next parameter.
                builder.append(params[i])
            }
            // All other values add as binding parameters.
            else {
                if (params[i] is List<*>) throw SqlInterpolatorException(
                    "Parameters of List type, specified in query, must be prefixed with '-'. " +
                        "Unary minus operator is overloaded by SqlPal and converts List to typed Array." +
                        "It's necessary to handle empty Lists, because unlike Array, empty List does not contain " +
                        "information about its generic type, what makes impossible to map it to appropriate SQL type.")
                builder.append('?')
                bindParams.add(params[i])
            }
            i++
            builder.append(strings[i])
        }
        return Query(builder.toString(), bindParams)
    }

    private fun paramIsFalse(paramIndex: Int, params: List<Any>): Boolean {
        if (paramIndex >= params.count())
            throw SqlInterpolatorException("'If' parameter can't be last. Add boolean parameter right after it.")
        if (params[paramIndex] !is Boolean)
            throw SqlInterpolatorException("Next parameter after 'If' must be of Boolean type.")

        return !(params[paramIndex] as Boolean)
    }

    private fun skipToLineBreak(from: Int, strings: List<String>): Pair<Int, String> {
        var i = from
        var breakIndex: Int

        while (i < strings.count()) {
            breakIndex = strings[i].indexOf('\n')
            if (breakIndex >= 0)
                return Pair(i, strings[i].substring(breakIndex + 1))
            i++
        }
        return Pair(i - 1, strings[i - 1])
    }
}