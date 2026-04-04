package org.sqlpal.query

import org.sqlpal.*
import java.sql.Connection

//////////////////////////////////////////////////////////////////////////////////
//-------------------- Contains methods to execute DML query -------------------//
//////////////////////////////////////////////////////////////////////////////////


/** Executes INSERT, UPDATE, DELETE or command with no results.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param autoGenColumns array of column names for witch to return values after execution.
 * Note that unlike [read] and [select] methods where
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience.
 * @return map of colName - value for inserted/updated row. Map contains columns specified in [autoGenColumns].
 * It is useful to get values of auto-generated columns (e.g. ID). Returns null if no rows are updated. */
fun execWithResults(query: Query, autoGenColumns: Array<String>? = null, con: Connection? = null) = query.doAction(con, autoGenColumns) {
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
fun execWithResult(query: Query, con: Connection? = null) = query.doAction(con) {
    if (it.executeUpdate() == 0) return@doAction null
    it.generatedKeys.next()
    it.generatedKeys.getObject(1)
}

/** Executes INSERT, UPDATE, DELETE or command with no results, and returns number of rows affected.
 * @param query Query specified with -"..." or -"""...""" syntax (see [Sql] for details).
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when need to execute in transaction, use [transaction] method for convenience. */
fun exec(query: Query, con: Connection? = null) = query.doAction(con) { it.executeUpdate() }