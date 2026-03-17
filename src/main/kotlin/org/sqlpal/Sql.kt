package org.sqlpal

import com.zaxxer.hikari.HikariDataSource
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
operator fun String.unaryMinus(): Cmd = interpolatorBody()

/** Stores interpolated values from provided String as bind parameters
 * and returns [Cmd] object that can be used to execute provided query.
 * It can be used with Sql("...") or Sql("""...""") syntax, as well as with more compact -"..." or -"""...""" syntax.
 *
 * To conditionally exclude part of query use $[If] $condition where 'condition' is boolean variable.
 * If 'condition' is false, then rest of content to line break is not included.
 *
 * To inline parameter directly into string (instead of extracting it as bind parameter), use $[I]$ instead of $. */
object Sql: Interpolator<Any, Cmd> {

    val dataSource = HikariDataSource()

    /** Collection of user defined mappers that are applied across entire application.
     * Mapper is applied if value matches type specified as key.
     * If you need custom processing only for particular property instead of entire application,
     * then annotate it with [Mapper]. */
    val valueMappers = mapOf<KClass<*>, ValueMapper>()

    /** Defines how [List] or [Array] of [enum] is stored.
     * If true and database supports arrays of enums (currently only PostgreSQL),
     * then store as array of enum type defined in database, that has the same name as [enum] class but in snake case.
     * Otherwise, store as array of strings. */
    val useEnumArrays = true

    override fun interpolate(parts: () -> List<String>, params: () -> List<Any>): Cmd {
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
        return Cmd(builder.toString(), bindParams)
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

class Sql2 {
    val params = ArrayList<Any>()

    fun v(value: Any): Char {
        params.add(value)
        return '?'
    }
//
//    operator fun invoke(parts: Array<String>, params: Array<Any?>): PreparedStatement
//    {
//        val statement = conn.prepareStatement(query())
//        for (i in 1 .. params.count()) {
//            val param = params[i-1]
//            if (param is Enum<*>)
//                statement.setObject(i, param, Types.OTHER)
//            else
//                statement.setObject(i, param)
//        }
//        return statement
//    }

    fun on(condition: Boolean, statement: String) = if (condition) statement else ""
}

//fun sql(conn: Connection, query: sqlpal.() -> String): PreparedStatement =
//    with(sqlpal()) {
//        val statement = conn.prepareStatement(query())
//        for (i in 1 .. params.count()) {
//            val param = params[i-1]
//            if (param is Enum<*>)
//                statement.setObject(i, param, Types.OTHER)
//            else
//                statement.setObject(i, param)
//        }
//        return statement
//    }
