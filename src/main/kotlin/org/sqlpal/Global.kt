package org.sqlpal

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.*
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.full.memberProperties

//////////////////////////////////////////////////////////////////////////////////
//--------------------- Contains public methods of SqlPal ----------------------//
//////////////////////////////////////////////////////////////////////////////////

/** Runs specified method, providing connection that is committed after method returns or rolled back on exception.
 * @param connection If specified, then transaction is executed on it, otherwise connection is obtained from pool.
 * @param block method to execute within transaction. */
inline fun <T> transaction(connection: Connection? = null, block: (Connection) -> T): T {
    val con = connection ?: Sql.dataSource.connection
    try {
        con.autoCommit = false
        val result = block(con)
        con.commit()
        return result
    }
    catch (ex: Exception) {
        con.rollback()
        throw ex
    }
}

/** Exception that signals incorrect usage of SqlPal or its limitations. */
class SqlPalException(message: String) : Exception(message)

//------------------------------------------------------------------------------
//------------------------------- SELECT methods -------------------------------
//------------------------------------------------------------------------------

/** Selects single row with specified id, considering that:
 * - table is named as class but in snake case,
 * - property that maps to primary key column is annotated with [Id] and its column datatype is integer or long.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param id ID to look for.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return object of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body. */
inline fun <reified T: Any> selectById(id: Long, con: Connection? = null) =
    selectByIdOrNll<T>(id, con) ?: throw IllegalArgumentException("Record with ID $id was not found.")

/** Selects single row with specified id, considering that:
 * - table is named as class but in snake case,
 * - property that maps to primary key column is annotated with [Id] and its column datatype is integer or long.
 * Null is returned if nothing found.
 * @param id ID to look for.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return object of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body. */
inline fun <reified T: Any> selectByIdOrNll(id: Long, con: Connection? = null) =
    readOneOrNull<T>(Cmd.createFindByIdCmd<T>(id), con)

/** Executes SELECT with columns specified from primary constructor parameters
 * and WHERE clause content from [where] parameter, considering that:
 * - table is named as class, but in snake case,
 * - columns are named as primary constructor parameters, but in snake case.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After conditions can be specified any clause that goes after WHERE (e.g. ORDER BY or LIMIT).
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit number of rows fetched from database. You can add LIMIT in [where] query.
 * @param includeOptional true (the default) to add to SELECT clause parameters that has default values, otherwise false.
 * @return [ArrayList] with objects of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body. */
inline fun <reified T: Any> select(where: Cmd, capacity: Int = -1, includeOptional: Boolean = true) =
    select<T>(where, null, capacity, includeOptional)

/** Executes SELECT with columns specified from primary constructor parameters
 * and WHERE clause content from [where] parameter, considering that:
 * - table is named as class, but in snake case,
 * - columns are named as primary constructor parameters, but in snake case.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After conditions can be specified any clause that goes after WHERE (e.g. ORDER BY or LIMIT).
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit number of rows fetched from database. You can add LIMIT in [where] query.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param includeOptional true (the default) to add to SELECT clause parameters that has default values, otherwise false.
 * @return [ArrayList] with objects of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body. */
inline fun <reified T: Any> select(where: Cmd, con: Connection? = null, capacity: Int = -1, includeOptional: Boolean = true): ArrayList<T> {
    // Public inline function can't access private members, while it must be inline to get generic type.
    // So implementation is moved to separate internal method, that receives type just as parameter.
    val query = buildSelectQuery(T::class, where, includeOptional)
    return read(query, capacity, con)
}

@PublishedApi
internal fun <T: Any> buildSelectQuery(type: KClass<T>, where: Cmd, includeOptional: Boolean = true): Cmd
{
    val sb = StringBuilder("SELECT ")
    val params = Cmd.getConstructor(type).parameters
    val customNames = getParamsCustomNames(type, params)
    for (p in params)
        if (!p.isOptional || includeOptional)
            sb.append(customNames[p] ?: toDbCase(p.name!!), ',')
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma

    sb.append(" FROM ")
    sb.append(entityName(type))
    sb.append(" WHERE ")
    sb.append(where.sql)

    return Cmd(sb.toString(), where.bindParams)
}

/** Runs specified query and returns single value from the first column of the first returned row.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValue(query: Cmd, con: Connection? = null) =
    readValueOrNull<T>(query, con) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Runs specified query and returns single value from the first column of the first returned row,
 * or null if query returned no rows.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValueOrNull(query: Cmd, con: Connection? = null) =
    query.readValues(T::class, 1, con).firstOrNull()

/** Runs specified query and returns [ArrayList] with values from the first column of the result set.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValues(query: Cmd, con: Connection? = null) =
    query.readValues(T::class, -1, con)

/** Runs specified query and returns object of specified type, created from the first row of query result
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readOne(query: Cmd, con: Connection? = null) =
    readOneOrNull<T>(query, con) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Runs specified query and returns object of specified type, created from the first row of query result,
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * Returns null if query returned no rows.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readOneOrNull(query: Cmd, con: Connection? = null) =
    query.read(T::class, 1, con).firstOrNull()

/** Runs specified query and returns [ArrayList] with objects of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> read(query: Cmd, con: Connection? = null) =
    query.read(T::class, -1, con)

/** Runs specified query and returns [ArrayList] with objects of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * @param query SELECT query specified with -"..." or -"""...""" syntax.
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit number of rows fetched from database.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> read(query: Cmd, capacity: Int, con: Connection? = null) =
    query.read(T::class, capacity, con)

/** Runs specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param createItem callback that is called for each fetched row.
 * SqlPay provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Cmd, createItem: (r: ResultSet) -> T) =
    read(query, -1, null, createItem)

/** Runs specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit number of rows fetched from database.
 * @param createItem callback that is called for each fetched row.
 * SqlPay provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Cmd, capacity: Int, createItem: (r: ResultSet) -> T) =
    read(query, capacity, null, createItem)

/** Runs specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param createItem callback that is called for each fetched row.
 * SqlPay provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Cmd, con: Connection, createItem: (r: ResultSet) -> T) =
    read(query, -1, con, createItem)

/** Runs specified query and returns [ArrayList] with objects created from query results by [createItem] callback.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit number of rows fetched from database.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param createItem callback that is called for each fetched row.
 * SqlPay provides extension methods on [ResultSet] like [enum] or [intVal] for all basic types
 * to read values from [ResultSet] with less code. */
fun <T> read(query: Cmd, capacity: Int, con: Connection? = null, createItem: (r: ResultSet) -> T) = query.doAction(con) {
    val rs = it.executeQuery()

    val results = if (capacity >= 0) ArrayList<T>(capacity) else ArrayList()
    while (rs.next())
        results.add(createItem(rs))
    results
}

//------------------------------------------------------------------------------
//-------------------------------- DML methods ---------------------------------
//------------------------------------------------------------------------------

/** Inserts specified entity to the table, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case.
 * Properties annotated with [AutoGen] are not included into INSERT,
 * but are read from INSERT results if [updateAutoGenValues] is true.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param updateAutoGenValues true (the default) to update properties with values of autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated. */
fun insert(entity: Any, con: Connection? = null, updateAutoGenValues: Boolean = true) =
    execInsertOrUpdate(entity, null, con, updateAutoGenValues,
        "INSERT INTO %s (", "") { _, sb, params -> appendValuesClause(sb, params.size) }

/** Inserts multiple items in single batch, considering:
 * - all items are of the same type,
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case.
 * @param items any iterable source of items to insert.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return number of inserted rows. */
inline fun <reified T: Any> insertMany(items: Iterable<T>, con: Connection? = null) =
    // Public inline function can't access private members, but it must be inline to get generic type.
    // So implementation is moved to separate internal method, that receives type just as parameter.
    insertMany(T::class, items, con)

@PublishedApi
internal fun <T: Any> insertMany(itemClass: KClass<T>, items: Iterable<T>, con: Connection? = null): Int
{
    val tableName = entityName(itemClass::class)
    val sb = StringBuilder("INSERT INTO $tableName (")

    // Get props from javaClass.kotlin, as props obtained from ::class does not allow to get prop value.
    val props = itemClass.memberProperties.filter { !it.hasAnnotation<AutoGen>() }

    for (p in props)  {
        sb.append(colName(p))
        sb.append(',')
    }
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma
    appendValuesClause(sb, props.size)

    val insertedCounts = Cmd(sb.toString(), ArrayList(props.size))
        .doBatch(items, con) { item, params ->
            for (p in props) addPropToBindParams(item, p, params)
        }
    return insertedCounts.sum()
}

private fun appendValuesClause(sb: StringBuilder, bindParamsCount: Int) {
    sb.append(") VALUES (")
    repeat(bindParamsCount) { sb.append("?,") }
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma
    sb.append(')')
}

/** Updates values in the corresponding table from the specified entity, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case,
 * - if [where] parameter is not specified, then property that maps to primary key column must be annotated with [Id].
 * Properties annotated with [AutoGen] are not included into UPDATE,
 * but are read from UPDATE results if [updateAutoGenValues] is true.
 * @param entity object to update.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * After conditions can be specified any clause, that goes after WHERE (e.g. ORDER BY or LIMIT).
 * If the parameter is specified, then it's appended to UPDATE command,
 * otherwise table is filtered by value from property annotated with [Id].
 * @param updateAutoGenValues true (the default) to update properties with values from autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated. */
fun update(entity: Any, where: Cmd? = null, updateAutoGenValues: Boolean = false) =
    update(entity, null, where, updateAutoGenValues, null)

/** Updates values in the corresponding table from the specified entity, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case,
 * - if [where] parameter is not specified, then property that maps to primary key column must be annotated with [Id].
 * Properties annotated with [AutoGen] are not included into UPDATE,
 * but are read from UPDATE results if [updateAutoGenValues] is true.
 * @param entity object to update.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * After conditions can be specified any clause, that goes after WHERE (e.g. ORDER BY or LIMIT).
 * If the parameter is specified, then it's appended to UPDATE command,
 * otherwise table is filtered by value from property annotated with [Id].
 * @param updateAutoGenValues true (the default) to update properties with values from autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated.*/
fun update(entity: Any, con: Connection? = null, where: Cmd? = null, updateAutoGenValues: Boolean = false) =
    update(entity, con, where, updateAutoGenValues, null)

/** Updates only specified columns in the corresponding table from the specified entity, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case,
 * - if [where] parameter is not specified, then property that maps to primary key column must be annotated with [Id].
 * Properties annotated with [AutoGen] are not included into UPDATE,
 * but are read from UPDATE results if [updateAutoGenValues] is true.
 * @param entity object to update.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * After conditions can be specified any clause, that goes after WHERE (e.g. ORDER BY or LIMIT).
 * If the parameter is specified, then it's appended to UPDATE command,
 * otherwise table is filtered by value from property annotated with [Id].
 * @param updateAutoGenValues true (the default) to update properties with values from autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated.
 * @param propList Provide lambda with call to one of next functions:
 * - [only] - provides list of properties to update in database, e.g.:
 *
 * update(person) { only(::name) } - to update only 'name' column.
 * - [except] - provides list of properties to exclude, while all other properties will be updated in database, e.g.:
 *
 * update(person) { except(::name, ::city) } - to update all columns except 'name' and 'city'.
 * - [set] - same as [only], but also sets specified properties to specified values before update, e.g.:
 *
 * update(person) { set (::position to "Developer") } - to set value in both 'position' property and column.*/
fun <T: Any> update(entity: T, where: Cmd? = null, updateAutoGenValues: Boolean = false, propList: T.() -> PropsToUpdate) =
    update(entity, null, where, updateAutoGenValues, propList)

/** Updates only specified columns in the corresponding table from the specified entity, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case,
 * - if [where] parameter is not specified, then property that maps to primary key column must be annotated with [Id].
 * Properties annotated with [AutoGen] are not included into UPDATE,
 * but are read from UPDATE results if [updateAutoGenValues] is true.
 * @param entity object to update.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * After conditions can be specified any clause, that goes after WHERE (e.g. ORDER BY or LIMIT).
 * If the parameter is specified, then it's appended to UPDATE command,
 * otherwise table is filtered by value from property annotated with [Id].
 * @param updateAutoGenValues true (the default) to update properties with values from autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated.
 * @param propList Provide lambda with call to one of next functions:
 * - [only] - provides list of properties to update in database, e.g.:
 *
 * update(person) { only(::name) } - to update only 'name' column.
 * - [except] - provides list of properties to exclude, while all other properties will be updated in database, e.g.:
 *
 * update(person) { except(::name, ::city) } - to update all columns except 'name' and 'city'.
 * - [set] - same as [only], but also sets specified properties to specified values before update, e.g.:
 *
 * update(person) { set (::position to "Developer") } - to set value in both 'position' property and column.*/
fun <T: Any> update(entity: T, con: Connection? = null, where: Cmd? = null, updateAutoGenValues: Boolean = false, propList: (T.() -> PropsToUpdate)?) =
    execInsertOrUpdate(entity, propList?.let { entity.it() }, con, updateAutoGenValues, "UPDATE %s SET ", " = ?",
        if (where == null) ::buildWhereWithId
        else { _, sb, params ->
            sb.append(" WHERE ")
            sb.append(where.sql)
            params.addAll(where.bindParams)
            Unit
        })

/** Is created by [only], [except] and [set] functions to return as lambda result
 * of 'propList' parameter of [update] function. Don't use it directly. */
class PropsToUpdate internal constructor(
    val include: Array<out KProperty0<*>>?,
    val exclude: Array<out KProperty0<*>>?
)

/** Provides list of properties to update in database, see [update] for description. */
fun only(vararg items: KProperty0<*>) = PropsToUpdate(items, null)

/** Provides list of properties to exclude from update, see [update] for description. */
fun except(vararg items: KProperty0<*>) = PropsToUpdate(null, items)

/** Same as [only], but also to sets specified values to specified properties, see [update] for description. */
@Suppress("UNCHECKED_CAST")
fun set(vararg items: Pair<KProperty0<*>, Any?>) = PropsToUpdate(
    Array(items.size) {
        (items[it].first as KMutableProperty0<Any?>).set(items[it].second)
        items[it].first
    }, null)

/** Updates specified columns in the corresponding table, considering that:
 * - table is named as [entity] class but in snake case,
 * - property that maps to primary key column is annotated with [Id].
 * This overload is useful when need to update columns for which there are no corresponding properties in the class.
 * @param entity Entity to update.
 * @param params list of pairs <column name - value to set>.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return number of rows affected. */
fun update(entity: Any, params: List<Pair<String, Any?>>, con: Connection? = null): Int {
    val sb = StringBuilder("UPDATE ${entityName(entity::class)} SET ")

    val bindParams = ArrayList<Any?>(params.size)
    params.forEach {
        sb.append(it.first)
        sb.append(" = ?,")
        bindParams.add(it.second)
    }
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma

    buildWhereWithId(entity, sb, bindParams)
    return exec(Cmd(sb.toString(), bindParams), con)
}

/** Deletes row in the corresponding table for the specified entity, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case
 * - property that maps to primary key column is annotated with [Id].
 * @param entity Entity to delete.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return number of rows deleted. */
fun delete(entity: Any, con: Connection? = null): Int {
    val sb = StringBuilder("DELETE FROM ${entityName(entity::class)}")
    val bindParams = ArrayList<Any?>(1)
    buildWhereWithId(entity, sb, bindParams)
    return exec(Cmd(sb.toString(), bindParams), con)
}

/** Deletes rows that meet [where] conditions, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return number of rows deleted. */
inline fun <reified T: Any> delete(where: Cmd, con: Connection? = null): Int {
    val sb = StringBuilder("DELETE FROM ${entityName(T::class)} WHERE ${where.sql}")
    return exec(Cmd(sb.toString(), where.bindParams), con)
}

/** Executes INSERT, UPDATE, DELETE or command with no results.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param autoGenColumns array of column names for witch to return values after execution.
 * Note that unlike [read] and [select] methods where
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return map of colName - value for inserted/updated row. Map contains columns specified in [autoGenColumns].
 * It is useful to get values of auto-generated columns (e.g. ID). Returns null if no rows are updated. */
fun execWithResults(query: Cmd, autoGenColumns: Array<String>? = null, con: Connection? = null) = query.doAction(autoGenColumns, con) {
    if (it.executeUpdate() == 0) return@doAction null
    it.generatedKeys.use {  rs ->
        rs.next()
        val generatedValues = mutableMapOf<String, Any?>()
        for (i in 1 .. rs.metaData.columnCount)
            generatedValues[rs.metaData.getColumnLabel(i)] = rs.getObject(i)
        generatedValues
    }
}

/** Executes INSERT, UPDATE, DELETE or command with no results.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return value from the first column of the first row of returned result set, or null if result set is empty.
 * Note that if RETURNING clause was not specified in the query,
 * then driver returns all columns, and first one can be any of them. */
fun execWithResult(query: Cmd, con: Connection? = null) = query.doAction(con) {
    if (it.executeUpdate() == 0) return@doAction null
    it.generatedKeys.next()
    it.generatedKeys.getObject(1)
}

/** Executes INSERT, UPDATE, DELETE or command with no results, and returns number of rows affected.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
fun exec(query: Cmd, con: Connection? = null) = query.doAction(con) { it.executeUpdate() }

//------------------------------------------------------------------------------
//------------------------------ private methods -------------------------------
//------------------------------------------------------------------------------

private typealias RefreshMap = MutableMap<String, KMutableProperty1<Any, Any?>?>
private val emptyMap: RefreshMap = mutableMapOf() // static value to avoid creation on each call if not needed.

private inline fun execInsertOrUpdate(entity: Any, propsToUpdate: PropsToUpdate?,
                                      con: Connection?, updateAutoGenValues: Boolean,
                                      statement: String, paramPlaceholder: String,
                                      buildParamsClause: (Any, StringBuilder, ArrayList<Any?>) -> Unit)
{
    val tableName = entityName(entity::class)
    val sb = StringBuilder(statement.format(tableName))

    // Get props from javaClass.kotlin, as props obtained from ::class does not allow to get prop value.
    val props = entity.javaClass.kotlin.memberProperties
    val bindParams = ArrayList<Any?>(props.size)
    val autoGenColumns: RefreshMap = if (updateAutoGenValues) mutableMapOf() else emptyMap
    when {
        propsToUpdate == null ->
            for (p in props)
                processProp(entity, p, bindParams, sb, paramPlaceholder, updateAutoGenValues, autoGenColumns)

        propsToUpdate.include != null -> {
            for (p in propsToUpdate.include) {
                appendCol(sb, colName(p), paramPlaceholder)
                addPropToBindParams(entity, p, bindParams)
            }
            if (updateAutoGenValues)
                for (p in props)
                    addToRefreshListIfAutoGen(p, true, autoGenColumns, colName(p))
        }
        propsToUpdate.exclude != null ->
            for (p in props)
                propsToUpdate.exclude.find { p.name == it.name }
                    ?: processProp(entity, p, bindParams, sb, paramPlaceholder, updateAutoGenValues, autoGenColumns)
    }
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma

    buildParamsClause(entity, sb, bindParams)

    val autoGenArr = if (updateAutoGenValues) autoGenColumns.keys.toTypedArray() else null
    val generatedValues = execWithResults(Cmd(sb.toString(), bindParams), autoGenArr, con)
        ?: throw SQLException("INSERT/UPDATE command on $tableName table affected no rows.")
    // TODO: Remove exception as it's normal that UPDATE can affect no rows if nothing matches criteria

    if (updateAutoGenValues)
        for ((colName, value) in generatedValues)
            autoGenColumns[colName]?.set(entity, value)
}

private fun processProp(entity: Any, p: KProperty<*>, bindParams: ArrayList<Any?>,
                        sb: StringBuilder, paramPlaceholder: String,
                        updateAutoGenValues: Boolean, autoGenColumns: RefreshMap) {
    val colName = colName(p)
    if (!addToRefreshListIfAutoGen(p, updateAutoGenValues, autoGenColumns, colName)) {
        appendCol(sb, colName, paramPlaceholder)
        addPropToBindParams(entity, p, bindParams)
    }
}

private fun addToRefreshListIfAutoGen(p: KProperty<*>, updateAutoGenValues: Boolean, autoGenColumns: RefreshMap, colName: String) =
    if (p.hasAnnotation<AutoGen>()) {
        if (updateAutoGenValues)
            @Suppress("UNCHECKED_CAST")
            autoGenColumns[colName] = p as? KMutableProperty1<Any, Any?>
        true
    }
    else false

private fun appendCol(sb: StringBuilder, colName: String, paramPlaceholder: String) {
    sb.append(colName)
    sb.append(paramPlaceholder)
    sb.append(',')
}

private fun buildWhereWithId(entity: Any, sb: StringBuilder, bindParams:ArrayList<Any?>) {
    val id = Cmd.getIdProperty(entity.javaClass.kotlin)
    sb.append(" WHERE ")
    sb.append(colName(id))
    sb.append(" = ?")
    addPropToBindParams(entity, id, bindParams)
}

private fun addPropToBindParams(entity: Any, p: KProperty<*>, bindParams: MutableList<Any?>) {
    @Suppress("UNCHECKED_CAST")
    var value = when (p) {
        is KProperty0<*> -> p.get() // property obtained via myObject::myProp (receiver object is already bound).
        is KProperty1<*, *> -> (p as KProperty1<Any, *>).get(entity) // property from myObject.javaClass.kotlin.memberProperties.
        else -> throw SqlPalException("Property '${p.name}' of '${entity::class.qualifiedName}' class " +
                "has more than one receiver. Such properties " +
                "(as an extension property declared in a class) are not supported.")
    }
    if (value is List<*>) {
        // Wrap List with object that also contains information about generic type of the List.
        // It's necessary to handle empty Lists, because unlike Array, empty List does not contain
        // information about its generic type, what makes impossible to map it to appropriate SQL type.
        val componentType = p.returnType.arguments[0].type?.classifier as? KClass<*>
            ?: throw SqlPalException("Can't determine generic type of List for " +
                    "property ${p.name} of ${entity::class.qualifiedName} class to map it to SQL type. " +
                    "Only Lists of primitive types are supported " +
                    "and generic type must be specified explicitly, not List<*>.")
        value = ListAndType(value, componentType)
    }
    bindParams.add(value)
}

@PublishedApi
internal fun entityName(type: KClass<*>) = customName(type) ?: toDbCase(type.simpleName!!)
@PublishedApi
internal fun colName(prop: KProperty<*>) = customName(prop) ?: toDbCase(prop.name)

// Until version 2.2 Kotlin did not support applying single annotation on both constructor parameter and property.
// Thus, to check that parameter is annotated we need to check property with the same name.
// So added this method to get custom name for all parameters at once.
@PublishedApi
internal fun getParamsCustomNames(classType: KClass<*>, params: List<KParameter>): Map<KParameter, String> {
    val customNames = mutableMapOf<KParameter, String>()
    for (prop in classType.memberProperties) {
        val name = customName(prop)
        if (name != null) {
            val param = params.first { prop.name == it.name }
            customNames[param] = name
        }
    }
    return customNames
}

@PublishedApi
internal fun customName(type: KAnnotatedElement) = type.findAnnotation<SqlName>()?.name
@PublishedApi
internal fun toDbCase(name: String) = Cmd.camel2Snake(name)
