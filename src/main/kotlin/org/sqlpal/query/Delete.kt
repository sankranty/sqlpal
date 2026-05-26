package org.sqlpal.query

import org.sqlpal.*
import java.sql.Connection

//////////////////////////////////////////////////////////////////////////////////
//--------------------- Contains methods to perform DELETE ---------------------//
//////////////////////////////////////////////////////////////////////////////////

/** Deletes the row in the corresponding table for the specified entity, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the properties in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the property that maps to the primary key column is annotated with [Id].
 * @param entity Entity to delete.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return number of rows deleted. */
fun delete(entity: Any, con: Connection? = null): Int {
    val sb = StringBuilder("DELETE FROM ${entityName(entity::class)}")
    val bindParams = ArrayList<Any?>(1)
    buildWhereWithId(entity, sb, bindParams)
    return exec(Query(sb.toString(), bindParams), con)
}

/** Deletes rows that meet [where] conditions, considering that:
 * - the table is named as the class in accordance with [SqlPal.convertNamesToSnakeCase] option,
 * - the columns are named as the properties in accordance with [SqlPal.convertNamesToSnakeCase] option.
 * @param where WHERE clause content specified with -"..." or -"""...""" syntax.
 * @param con If specified, then command is executed on it, and it is not closed after use.
 * Otherwise, connection is obtained from pool and released after use.
 * Specifying connection is useful when you need to execute in a transaction, use [transaction] method for convenience.
 * @return number of rows deleted. */
inline fun <reified T: Any> delete(where: Query, con: Connection? = null): Int {
    val sb = StringBuilder("DELETE FROM ${entityName(T::class)} WHERE ${where.sql}")
    return exec(Query(sb.toString(), where.bindParams), con)
}