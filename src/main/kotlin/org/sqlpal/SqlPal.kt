package org.sqlpal

import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import javax.sql.DataSource
import kotlin.reflect.KClass
import kotlin.reflect.KType

/** Provides methods to set up SqlPal. */
object SqlPal
{
    @Volatile
    @PublishedApi
    internal var internalDataSource: DataSource? = null

    /** Sets the datasource that will be used to get a connection
     * when calling methods without explicitly providing a connection. */
    fun setDataSource(dataSource: DataSource) {
        this.internalDataSource = dataSource
    }

    /** Creates a Hikari datasource that will be used to get a connection
     * when calling SqlPal methods without explicitly providing a connection.
     * @param jdbcUrl database connection string in the format "jdbc:driver-name://address:port/database-name"
     * @param username username to connect to the database.
     * @param password password for specified user.
     * @param schema default schema name to be set on connections.
     * @param isReadOnly specifies if the connections will be created as read-only connections.
     * @param isAutoCommit sets the default auto-commit behavior of connections in the pool.
     * @param maximumPoolSize the maximum size that the connection pool is allowed to reach,
     * including both idle and in-use connections.
     * Basically this value will determine the maximum number of actual connections to the database backend.
     * @param minimumIdle the minimum number of idle connections to maintain in the pool,
     * including both idle and in-use connections.
     * If the idle connections dip below this value, the pool will create additional connections. */
    fun setDataSource(jdbcUrl: String, username: String, password: String,
                      schema: String = "", isReadOnly: Boolean = false, isAutoCommit: Boolean = true,
                      maximumPoolSize: Int = -1, minimumIdle: Int = -1) {
        internalDataSource = HikariDataSource().also {
            it.jdbcUrl = jdbcUrl
            it.username = username
            it.password = password
            if (schema.isNotBlank()) it.schema = schema
            it.isReadOnly = isReadOnly
            it.isAutoCommit = isAutoCommit
            if (maximumPoolSize >= 0) it.maximumPoolSize = maximumPoolSize
            if (minimumIdle >= 0) it.minimumIdle = minimumIdle
        }
    }

    /** Executes the specified function block, providing a connection from the pool,
     * and returning it to the pool after execution. */
    inline fun <T> withConnection (block: (Connection) -> T): T {
        val ds = internalDataSource ?: throw SqlPalException("Attempt to get a connection from the SqlPal datasource " +
                "while datasource is not set. Call 'Sql.setDataSource' method before using any SqlPal methods, " +
                "or use overloads that accept a connection as parameter.")
        return ds.connection.use(block)
    }

    internal val valueMappers = mutableMapOf<KClass<*>, ValueMapper>()

    /** Adds user-defined mapper that is applied across the entire application.
     * Mapper is applied to read and write values of the specified type.
     * If custom processing is needed only for a particular property instead of the entire application,
     * then annotate the property with [Mapper].
     * @param type for which to read and write values with the specified mapper.
     * @param mapper user-defined mapper to apply. */
    fun addTypeMapper(type: KClass<*>, mapper: ValueMapper) = synchronized(valueMappers) { valueMappers[type] = mapper }

    /** Defines how [List] or [Array] of [enum] is stored.
     * Has effect only when [storeArraysAs] is [ArrayStorageType.Array] (the default).
     * If true and database supports arrays of enums (is currently supported only by PostgreSQL),
     * then store as an array of the enum type defined in the database,
     * that has the same name as [enum] class in accordance with [convertNamesToSnakeCase] option.
     * Otherwise, store as an array of strings. */
    @Volatile
    var useEnumArrays = true

    /** Defines how [List] or [Array] is stored. See [ArrayStorageType] for details. */
    @Volatile
    var storeArraysAs = ArrayStorageType.Array

    internal fun storeAsJson(componentType: KType) = storeAsJson(componentType.kClass == ByteArray::class)
    internal fun storeAsJson(isByteArray: Boolean) =
        storeArraysAs == ArrayStorageType.Json || storeArraysAs == ArrayStorageType.JsonExceptByteArray && !isByteArray

    /** true (the default) - convert names of classes and properties from camelCase (or PascalCase)
     * to snake_case to map them to database objects (tables, columns, user-defined types).
     *
     * false - assume that names of database objects are the same as names of classes and properties.
     *
     * Value affects only generation of SQL queries, as reading of query results
     * already considers all possible differences in naming. */
    @Volatile
    var convertNamesToSnakeCase: Boolean = true
}

/** Supported options of how arrays can be stored in the database. */
enum class ArrayStorageType {
    /** Store as a database array (database must support columns of array type). */
    Array,
    /** Store as JSON string. In this case [SqlPal.useEnumArrays] has no effect. */
    Json,
    /** Store as a JSON string except for ByteArray. The option is useful when ByteArray is used for BLOB columns.
     * In this case [SqlPal.useEnumArrays] has no effect. */
    JsonExceptByteArray
}

/** Exception that signals incorrect usage of SqlPal or its limitations. */
class SqlPalException(message: String) : Exception(message)