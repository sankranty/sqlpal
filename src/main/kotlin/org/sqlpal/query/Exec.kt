package org.sqlpal.query

import org.sqlpal.*
import java.sql.Connection

//////////////////////////////////////////////////////////////////////////////////
//------------------- Contains methods to execute DML queries ------------------//
//////////////////////////////////////////////////////////////////////////////////

/** Executes INSERT, UPDATE, DELETE or a command with no results.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param autoGenColumns array of column names for which to return values after execution.
 * Note that unlike [read] and [select] methods, where types of values are known and thus all supported types
 * are mapped correctly, here values are provided by JDBC driver, so it will not produce values
 * of complex types like enums or lists.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return map of column name - value for inserted/updated row. Map contains columns specified in [autoGenColumns].
 * It is useful to get values of auto-generated columns (e.g. ID). Returns null if no rows are updated. */
fun execWithResults(query: Query, autoGenColumns: Array<String>? = null, con: Connection? = null) = query.doAction(con, autoGenColumns) { stmt ->
    if (stmt.executeUpdate() == 0) null
    else stmt.generatedKeys.use { rs ->
        if (!rs.next()) null
        else mutableMapOf<String, Any?>().also {
            for (i in 1..rs.metaData.columnCount)
                it[rs.metaData.getColumnLabel(i)] = rs.getObject(i)
        }
    }
}

/** Executes INSERT, UPDATE, DELETE or a command with no results.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return value from the first column of the first row of returned result set, or null if result set is empty.
 * Note that if RETURNING clause was not specified in the query,
 * then driver returns all columns, and the first one can be any of them. */
fun execWithResult(query: Query, con: Connection? = null) = query.doAction(con, null, true) { stmt ->
    if (stmt.executeUpdate() == 0) null
    else stmt.generatedKeys.use { if (!it.next()) null else it.getObject(1) }
}

/** Executes INSERT, UPDATE, DELETE (or other command that generates results),
 * and creates object of specified type from the generated results.
 * Method requests database driver to retrieve all generated results, so no RETURNING clause needed in the query.
 * But if results are not retrieved, it means your database driver does not support this option.
 * Throws [IllegalArgumentException] if execution did not return generated results.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return Object created from the generated results of provided query. */
inline fun <reified T: Any> execToOne(query: Query, con: Connection? = null): T =
    execToOneOrNull(query, con) ?: throw IllegalArgumentException("Can't read first value as query returned no rows.")

/** Executes INSERT, UPDATE, DELETE (or other command that generates results),
 * and creates object of specified type from the generated results.
 * Method requests database driver to retrieve all generated results, so no RETURNING clause needed in the query.
 * But if results are not retrieved, it means your database driver does not support this option.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return Object created from the generated results of provided query or null if no results. */
inline fun <reified T: Any> execToOneOrNull(query: Query, con: Connection? = null): T? =
    query.read(T::class, 1, con, true).firstOrNull()

/** Executes INSERT, UPDATE, DELETE or a command with no results, and returns number of rows affected.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience. */
fun exec(query: Query, con: Connection? = null) = query.doAction(con) { it.executeUpdate() }