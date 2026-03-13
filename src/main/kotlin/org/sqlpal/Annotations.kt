package org.sqlpal

import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.reflect.KClass

/** Indicates that column value is auto-generated,
 * so it's not included into INSERT/UPDATE statement, but is read from INSERT/UPDATE results. */
@Target(AnnotationTarget.PROPERTY)
annotation class AutoGen

/** Indicates that column is primary key. Is used to determine ID for UPDATE statement. */
@Target(AnnotationTarget.PROPERTY)
annotation class Id

/** Provides value mapper for particular property. If you need to set mapping for some type globally,
 * then add it to [Sql.valueMappers] with corresponding [ValueMapper].
 * @param mapper Specify type of object or class that implements [ValueMapper].
 * If class is specified, then it must have parameterless constructor. */
@Target(AnnotationTarget.PROPERTY)
annotation class Mapper(val mapper: KClass<out ValueMapper>)

/** Provides methods to read and write value instead of SqlPal standard processing,
 * or for type that is not supported by SqlPal itself. */
interface ValueMapper {
    /** Method is called on SELECT queries for each value in corresponding column.
     * Read value from [resultSet] using one of its getXXX methods, passing [colIndex] to it,
     * and then process obtained value to create value of desired type.
     * Don't call getXXX method with index other than [colIndex], as it may affect reading data from other columns.
     * To check if value is null call [ResultSet.wasNull] after call to getXXX method. */
    fun readValue(resultSet: ResultSet, colIndex: Int): Any?

    /** Method is called on DML queries and can be called on SELECT query if there is binding parameter of
     * Set value using one of setXXX methods of [PreparedStatement] passing [paramIndex] to it.
     * Don't call setXXX method with index other than [paramIndex], as it will affect setting of other parameters.
     * @param componentType type of component (generic type) for [List] or [Array].
     * @return - true - to indicate that value is written and no further processing for it is necessary.
     * - false - to perform standard processing for this value. */
    fun writeValue(value: Any?, statement: PreparedStatement, paramIndex: Int, componentType: KClass<*>?): Boolean
}