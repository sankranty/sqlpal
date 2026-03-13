package org.sqlpal

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KProperty1
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

//------------------------------------------------------------------------------
//------------------------------- SELECT methods -------------------------------
//------------------------------------------------------------------------------

/** Selects single row with specified id, considering that:
 * - table is named as class but in snake case,
 * - property that maps to primary key column is annotated with [Id] and its column datatype is integer or long.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param id ID to look for.
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
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
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return object of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body. */
inline fun <reified T: Any> selectByIdOrNll(id: Long, con: Connection? = null) =
    readOneOrNull<T>(Cmd.createFindByIdCmd<T>(id), con)

/** Executes SELECT query with WHERE clause content from [where] parameter.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * After conditions can be specified any clause that goes after WHERE (e.g. ORDER BY or LIMIT).
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit number of rows fetched from database. You can add LIMIT in [where] query.
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return [ArrayList] with objects of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body. */
inline fun <reified T: Any> select(where: Cmd, capacity: Int = -1, con: Connection? = null): ArrayList<T>
{
    val sb = StringBuilder("SELECT ")
    Cmd.getConstructor(T::class).parameters.forEach { sb.append(Cmd.camel2Snake(it.name!!), ',') }
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma
    sb.append(" FROM ", Cmd.camel2Snake(T::class.simpleName!!), " WHERE ", where.sql)
    return read(Cmd(sb.toString(), where.bindParams), capacity, con)
}

/** Runs specified query and returns single value from the first column of the first returned row.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValue(query: Cmd, con: Connection? = null) =
    readValueOrNull<T>(query, con) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Runs specified query and returns single value from the first column of the first returned row,
 * or null if query returned no rows.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValueOrNull(query: Cmd, con: Connection? = null) =
    query.readValues(T::class, 1, con).firstOrNull()

/** Runs specified query and returns [ArrayList] with values from the first column of the result set.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readValues(query: Cmd, con: Connection? = null) =
    query.readValues(T::class, -1, con)

/** Runs specified query and returns object of specified type, created from the first row of query result
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * If query returns no rows, then [IllegalArgumentException] is thrown.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readOne(query: Cmd, con: Connection? = null) =
    readOneOrNull<T>(query, con) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Runs specified query and returns object of specified type, created from the first row of query result,
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * Returns null if query returned no rows.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> readOneOrNull(query: Cmd, con: Connection? = null) =
    query.read(T::class, 1, con).firstOrNull()

/** Runs specified query and returns [ArrayList] with objects of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * @param query SELECT query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
inline fun <reified T: Any> read(query: Cmd, con: Connection? = null) =
    query.read(T::class, -1, con)

/** Runs specified query and returns [ArrayList] with objects of specified type, created from query results
 * by mapping names of constructor parameters to column names (case-insensitive, ignoring _ symbol).
 * If some property doesn't have corresponding column in result set, then move it from constructor to class body.
 * @param query SELECT query specified with -"..." or -"""...""" syntax.
 * @param capacity If specified, sets initial capacity of [ArrayList] where results are stored.
 * It does not limit number of rows fetched from database.
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
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
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
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
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
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
 * Properties annotated with [AutoGen] are not included into INSERT, but are read from INSERT results.
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param updateAutoGenValues true (the default) to update properties with values of autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated. */
fun insert(entity: Any, con: Connection? = null, updateAutoGenValues: Boolean = true) =
    execInsertOrUpdate(entity, con, updateAutoGenValues, "INSERT INTO %s (", "") { _, sb, params ->
        sb.append(") VALUES (")
        repeat(params.size) { sb.append("?,") }
        sb.deleteCharAt(sb.length - 1) // Remove trailing comma
        sb.append(')')
    }

/** Updates all values in the corresponding table from the specified entity, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case
 * - if [where] parameter is not specified, then property that maps to primary key column must be annotated with [Id].
 * Properties annotated with [AutoGen] are not included into UPDATE, but are read from UPDATE results.
 * @param entity object to update.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * After conditions can be specified any clause, that goes after WHERE (e.g. ORDER BY or LIMIT).
 * If the parameter is specified, then it's appended to UPDATE command,
 * otherwise entity is filtered by value from property annotated with [Id].
 * @param updateAutoGenValues true (the default) to update properties with values from autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated. */
fun update(entity: Any, where: Cmd? = null, updateAutoGenValues: Boolean = false) =
    update(entity, null, where, updateAutoGenValues)

/** Updates all values in the corresponding table from the specified entity, considering that:
 * - table is named as class but in snake case,
 * - columns are named as properties but in snake case
 * - if [where] parameter is not specified, then property that maps to primary key column must be annotated with [Id].
 * Properties annotated with [AutoGen] are not included into UPDATE, but are read from UPDATE results.
 * @param entity object to update.
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * After conditions can be specified any clause, that goes after WHERE (e.g. ORDER BY or LIMIT).
 * If the parameter is specified, then it's appended to UPDATE command,
 * otherwise entity is filtered by value from property annotated with [Id].
 * @param updateAutoGenValues true (the default) to update properties with values from autogenerated columns (e.g. ID).
 * Only values of properties annotated with [AutoGen] are updated.*/
fun update(entity: Any, con: Connection? = null, where: Cmd? = null, updateAutoGenValues: Boolean = false) =
    execInsertOrUpdate(entity, con, updateAutoGenValues, "UPDATE %s SET ", " = ?",
        if (where == null) ::buildWhereWithId
        else { _, sb, params ->
            sb.append(" WHERE ", where.sql)
            params.addAll(where.bindParams)
            Unit
        }
    )

/** Updates specified values in the corresponding table, considering that:
 * - table is named as [entity] class but in snake case,
 * - property that maps to primary key column is annotated with [Id].
 * @param entity Entity to update.
 * @param params list of pairs <column name - value to set>.
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return number of rows affected. */
fun update(entity: Any, params: List<Pair<String, Any?>>, con: Connection? = null): Int {
    val sb = StringBuilder("UPDATE ${tableName(entity)} SET ")

    val bindParams = ArrayList<Any?>(params.size)
    params.forEach {
        sb.append(it.first, " = ?,")
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
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return number of rows affected. */
fun delete (entity: Any, con: Connection? = null): Int {
    val sb = StringBuilder("DELETE FROM ${tableName(entity)}")
    val bindParams = ArrayList<Any?>(1)
    buildWhereWithId(entity, sb, bindParams)
    return exec(Cmd(sb.toString(), bindParams), con)
}

/** Executes INSERT, UPDATE, DELETE or command with no results.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param autoGenColumns array of column names for witch to return values after execution.
 * Note that unlike [read] and [select] methods where
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
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
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
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
 * @param con If specified, then command is executed on it, otherwise connection is obtained from pool.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
fun exec(query: Cmd, con: Connection? = null) = query.doAction(con) { it.executeUpdate() }

//------------------------------------------------------------------------------
//------------------------------ private methods -------------------------------
//------------------------------------------------------------------------------

private inline fun execInsertOrUpdate(entity: Any, con: Connection?, updateAutoGenValues: Boolean,
                                      statement: String, strAfterColName: String,
                                      buildParamsClause: (Any, StringBuilder, ArrayList<Any?>) -> Unit)
{
    val tableName = tableName(entity)
    val sb = StringBuilder(statement.format(tableName))

    // Get props from javaClass.kotlin, as props obtained from ::class does not allow to get prop value.
    val props = entity.javaClass.kotlin.memberProperties
    val bindParams = ArrayList<Any?>(props.size)
    val autoGenColumns = mutableMapOf<String, KProperty1<Any, Any?>>()
    for (p in props) {
        val colName = Cmd.camel2Snake(p.name)
        if (p.hasAnnotation<AutoGen>())
            autoGenColumns[colName] = p
        else {
            sb.append(colName, strAfterColName, ',')
            addPropValueToBindParams(entity, p, bindParams)
        }
    }
    sb.deleteCharAt(sb.length - 1) // Remove trailing comma

    buildParamsClause(entity, sb, bindParams)

    val autoGenArr = if (updateAutoGenValues) autoGenColumns.keys.toTypedArray() else null
    val generatedValues = execWithResults(Cmd(sb.toString(), bindParams), autoGenArr, con)
        ?: throw SQLException("INSERT/UPDATE command on $tableName table affected no rows.")

    if (updateAutoGenValues)
        for ((colName, value) in generatedValues)
            (autoGenColumns[colName] as? KMutableProperty1<Any, Any?>)?.set(entity, value)
}

private fun buildWhereWithId(entity: Any, sb: StringBuilder, bindParams:ArrayList<Any?>) {
    val id = Cmd.getIdProperty(entity.javaClass.kotlin)
    sb.append(" WHERE ", Cmd.camel2Snake(id.name), " = ?")
    addPropValueToBindParams(entity, id, bindParams)
}

private fun addPropValueToBindParams(entity: Any, p: KProperty1<Any, *>, bindParams: ArrayList<Any?>) {
    var value = p.get(entity)
    if (value is List<*>) {
        // Wrap List with object that also contains information about generic type of the List.
        // It's necessary to handle empty Lists, because unlike Array, empty List does not contain
        // information about its generic type, what makes impossible to map it to appropriate SQL type.
        val componentType = p.returnType.arguments[0].type?.classifier as? KClass<*>
            ?: throw Exception("Can't determine generic type of List for " +
                    "property ${p.name} of ${entity::class.qualifiedName} class to map it to SQL type. " +
                    "Only Lists of primitive types are supported " +
                    "and generic type must be specified explicitly, not List<*>.")
        value = ListAndType(value, componentType)
    }
    bindParams.add(value)
}

private fun tableName(entity: Any) = Cmd.camel2Snake(entity::class.simpleName!!)
