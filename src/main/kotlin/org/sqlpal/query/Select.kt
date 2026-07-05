package org.sqlpal.query

import org.sqlpal.*
import java.sql.Connection
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.full.memberProperties

//////////////////////////////////////////////////////////////////////////////////
//--------------------- Contains methods to perform SELECT ---------------------//
//////////////////////////////////////////////////////////////////////////////////

/** Returns true if entity with specified ID exists in the table, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the property that maps to the primary key column is annotated with [Id] and its column datatype is integer or long.
 * @param id ID to look for.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return true if entity with specified id exists in the table, otherwise false. */
inline fun <reified T: Any> exists(id: Long, con: Connection? = null): Boolean {
    val idCol = colName(getIdProperty(T::class))
    val query = "SELECT 1 FROM ${entityName(T::class)} WHERE $idCol = ?"
    return readValueOrNull<Int>(Query(query, mutableListOf(id)), con) != null
}

/** Returns true if entity with corresponding ID exists in the table, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the property that maps to the primary key column is annotated with [Id] and its column datatype is integer or long.
 * @param entity to look for. The search is performed by the value of property annotated with [Id].
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return true if entity with specified id exists in the table, otherwise false. */
inline fun <reified T: Any> exists(entity: T, con: Connection? = null): Boolean {
    val prop = getIdProperty(T::class)
    val query = "SELECT 1 FROM ${entityName(T::class)} WHERE ${colName(prop)} = ?"
    val bindParams = mutableListOf<Any?>()
    addPropToBindParams(entity, prop, bindParams)
    return readValueOrNull<Int>(Query(query, bindParams), con) != null
}

/** Selects a single row with the specified ID, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the property that maps to the primary key column is annotated with [Id] and its column datatype is integer or long.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param id ID to look for.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return object of specified type, created from query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * If property doesn't have corresponding column, then annotate it with [SqlIgnore]. */
inline fun <reified T: Any> selectById(id: Long, con: Connection? = null) =
    selectByIdOrNull<T>(id, con) ?: throw IllegalArgumentException("Record with ID $id was not found.")

/** Selects a single row with the specified ID, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the property that maps to the primary key column is annotated with [Id] and its column datatype is integer or long.
 * Returns null if nothing found.
 * @param id ID to look for.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return object of specified type, created from query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * If property doesn't have corresponding column, then annotate it with [SqlIgnore].*/
inline fun <reified T: Any> selectByIdOrNull(id: Long, con: Connection? = null): T? {
    val idCol = colName(getIdProperty(T::class))
    val query = buildSelectQuery(T::class, Query("$idCol = ?", mutableListOf(id)))
    return readOneOrNull(query, con)
}

/** Executes SELECT with columns specified from primary constructor parameters and mutable properties,
 * and WHERE clause content from [where] parameter, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After the conditions, any clause that goes after WHERE (e.g. ORDER BY or LIMIT) can be specified.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @param includeOptional true (the default) to include in the SELECT clause constructor parameters
 * that have default values and mutable properties declared in class body,
 * otherwise, only primary constructor parameters that do not have default values are included.
 * @return object of specified type, created from the first row of query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * If property doesn't have corresponding column, then annotate it with [SqlIgnore] or set [includeOptional] to false. */
inline fun <reified T: Any> selectOne(where: Query, con: Connection? = null, includeOptional: Boolean = true) =
    selectOneOrNull<T>(where, con, includeOptional) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Executes SELECT with columns specified from primary constructor parameters and mutable properties,
 * and WHERE clause content from [where] parameter, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After the conditions, any clause that goes after WHERE (e.g. ORDER BY or LIMIT) can be specified.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @param includeOptional true (the default) to include in the SELECT clause constructor parameters
 * that have default values and mutable properties declared in class body,
 * otherwise, only primary constructor parameters that do not have default values are included.
 * @return object of specified type, created from the first row of query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters),
 * or null if nothing was found.
 * If property doesn't have corresponding column, then annotate it with [SqlIgnore] or set [includeOptional] to false. */
inline fun <reified T: Any> selectOneOrNull(where: Query, con: Connection? = null, includeOptional: Boolean = true) =
    select<T>(where, con, -1, includeOptional).firstOrNull()

/** Executes SELECT with columns specified from primary constructor parameters and mutable properties,
 * and WHERE clause content from [where] parameter, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After the conditions, any clause that goes after WHERE (e.g. ORDER BY or LIMIT) can be specified.
 * @param includeOptional true (the default) to include in the SELECT clause constructor parameters
 * that have default values and mutable properties declared in class body,
 * otherwise, only primary constructor parameters that do not have default values are included.
 * @return [ArrayList] with objects of specified type, created from query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * If property doesn't have corresponding column, then annotate it with [SqlIgnore] or set [includeOptional] to false. */
inline fun <reified T: Any> select(where: Query, includeOptional: Boolean = true) =
    select<T>(where, null, -1, includeOptional)

/** Executes SELECT with columns specified from primary constructor parameters and mutable properties,
 * and WHERE clause content from [where] parameter, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After the conditions, any clause that goes after WHERE (e.g. ORDER BY or LIMIT) can be specified.
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit the number of rows fetched from database. You can add LIMIT in [where] query.
 * @param includeOptional true (the default) to include in the SELECT clause constructor parameters
 * that have default values and mutable properties declared in class body,
 * otherwise, only primary constructor parameters that do not have default values are included.
 * @return [ArrayList] with objects of specified type, created from query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * If property doesn't have corresponding column, then annotate it with [SqlIgnore] or set [includeOptional] to false. */
inline fun <reified T: Any> select(where: Query, capacity: Int = -1, includeOptional: Boolean = true) =
    select<T>(where, null, capacity, includeOptional)

/** Executes SELECT with columns specified from primary constructor parameters and mutable properties,
 * with WHERE clause content from [where] parameter, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the primary constructor parameters in accordance with [SqlPal.convertNamesToSnakeCase] option.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After the conditions, any clause that goes after WHERE (e.g. ORDER BY or LIMIT) can be specified.
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit the number of rows fetched from database. You can add LIMIT in [where] query.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @param includeOptional true (the default) to include in the SELECT clause constructor parameters
 * that have default values and mutable properties declared in class body,
 * otherwise, only primary constructor parameters that do not have default values are included.
 * @return [ArrayList] with objects of specified type, created from query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * If property doesn't have corresponding column, then annotate it with [SqlIgnore] or set [includeOptional] to false. */
inline fun <reified T: Any> select(where: Query, con: Connection? = null, capacity: Int = -1, includeOptional: Boolean = true): ArrayList<T> {
    // Implementation is moved to separate method, that receives generic type just as parameter
    // (and thus does not need to be inline), because this method will be called in many places in client code,
    // so it will blow app work set if implementation will be inlined.
    val query = buildSelectQuery(T::class, where, includeOptional)
    return read(query, capacity, con)
}

@PublishedApi
internal fun <T: Any> buildSelectQuery(type: KClass<T>, where: Query, includeOptional: Boolean = true): Query
{
    val sb = StringBuilder("SELECT ")
    val params = getConstructor(type).parameters
    val customNames = getParamsCustomNames(type, params)

    val props = mutableMapOf<String, KProperty<*>>()
    for (p in type.memberProperties) props[p.name] = p

    for (p in params) {
        val prop = props.remove(p.name!!) // remove instead of get to process further only props that don't correspond to params
        if ((!p.isOptional || includeOptional) && (prop == null || !prop.hasAnnotation<SqlIgnore>()))
            sb.append(customNames[p] ?: toDbCase(p.name!!), ',')
    }
    if (includeOptional)
        for (p in props.values)
            if (p is KMutableProperty<*> && !p.hasAnnotation<SqlIgnore>())
                sb.append(colName(p), ',')

    if (!sb.endsWith(',')) throw SqlPalException("Can't generate SELECT statement " +
            "for the class ${type.qualifiedName} as it has no suitable properties.")
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma

    sb.append(" FROM ")
    sb.append(entityName(type))
    sb.append(" WHERE ")
    sb.append(where.sql)

    return Query(sb.toString(), where.bindParams)
}

/** Runs the specified query and returns a single value from the first column of the first returned row.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValue(query: Query, con: Connection? = null) =
    readValueOrNull<T>(query, con) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Runs the specified query and returns a single value from the first column of the first returned row,
 * or null if query returned no rows.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValueOrNull(query: Query, con: Connection? = null) =
    query.readValues(T::class, 1, con).firstOrNull()

/** Runs the specified query and returns [ArrayList] with values from the first column of the result set.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValues(query: Query, con: Connection? = null) =
    query.readValues(T::class, -1, con)

/** Runs the specified query and returns an object of specified type, created from the first row of the query result
 * by mapping names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readOne(query: Query, con: Connection? = null) =
    readOneOrNull<T>(query, con) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Runs the specified query and returns an object of specified type, created from the first row of the query result
 * by mapping names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * Returns null if query returned no rows.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readOneOrNull(query: Query, con: Connection? = null) =
    query.read(T::class, 1, con).firstOrNull()

/** Runs the specified query and returns [ArrayList] with objects of specified type, created from query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> read(query: Query, con: Connection? = null) =
    query.read(T::class, -1, con)

/** Runs the specified query and returns [ArrayList] with objects of specified type, created from query results by mapping
 * names of constructor parameters and properties to column names (case-insensitive, ignoring word delimiters).
 * @param query SELECT query specified with -"..." or -"""...""" syntax.
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit the number of rows fetched from database.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> read(query: Query, capacity: Int, con: Connection? = null) =
    query.read(T::class, capacity, con)

/** Runs the specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param createItem callback that is called for each fetched row.
 * SqlPal provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Query, createItem: (r: ResultSet) -> T) =
    read(query, -1, null, createItem)

/** Runs the specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit the number of rows fetched from database.
 * @param createItem callback that is called for each fetched row.
 * SqlPal provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Query, capacity: Int, createItem: (r: ResultSet) -> T) =
    read(query, capacity, null, createItem)

/** Runs the specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @param createItem callback that is called for each fetched row.
 * SqlPal provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Query, con: Connection, createItem: (r: ResultSet) -> T) =
    read(query, -1, con, createItem)

/** Runs the specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit the number of rows fetched from database.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @param createItem callback that is called for each fetched row.
 * SqlPal provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Query, capacity: Int, con: Connection? = null, createItem: (r: ResultSet) -> T) = query.doAction(con) {
    val rs = it.executeQuery()

    val results = if (capacity >= 0) ArrayList<T>(capacity) else ArrayList()
    while (rs.next())
        results.add(createItem(rs))
    results
}
