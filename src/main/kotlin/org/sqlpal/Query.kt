package org.sqlpal

import java.math.BigDecimal
import java.sql.*
import java.time.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.*
import kotlin.reflect.full.*

/** Encapsulates SQL-query with bind parameters and provides methods to execute it.
 *
 * Instances of this class are created by -"..." or -"""...""" syntax.
 * Except rare cases, there is no need to work with this class directly,
 * general workflow is to just pass it to methods, that accept it. */
class Query @PublishedApi internal constructor(
    val sql: String,
    val bindParams: MutableList<Any?>,
) {
    private data class Reader(
        val read: ResultSet.(Int, KType) -> Any?,
        val colIndex: Int,
        val type: KType,
        val param: KParameter?
    )

    // Public functions, that we provide for reading, are marked as inline, to allow to specify generic type
    // (because call to T::class requires method to be inline), what is not desired,
    // as implementation is pretty large, and it will be called in many places in client code,
    // so it will blow app work set if inlined.
    // Thus, implementation is moved to this function (that accepts generic type already as a parameter,
    // instead of someFunc<Generic>() notation, and so it doesn't need to be inline).
    // It's also marked with @PublishedApi due to public inline functions can't call private or internal methods,
    // as their code is embedded at call site.
    @PublishedApi
    internal fun <T: Any> read(classType: KClass<T>, capacity: Int, con: Connection?) = doAction(con) { stmt ->
        val rs = stmt.executeQuery()

        // Create mapping of column names (without delimiters to find it further by property name) to column indices
        val colIndices = mutableMapOf<String, Int>()
        for (i in 1 .. rs.metaData.columnCount) {
            val name = rs.metaData.getColumnLabel(i).toPlainName()
            colIndices[name] = i
        }

        // Create reader for each constructor parameter and properties (if there are corresponding columns)
        val constr = getConstructor(classType)
        val (hasUnmappedOptionalParams, paramReaders, propReaders) = createReaders(constr.parameters, colIndices, classType)
        val values = arrayOfNulls<Any>(paramReaders.count()) // Array where to read values for each row

        // If primary constructor has optional params, for which there are no corresponding columns,
        // then also create map, to specify only parameters, that have columns in result set.
        val createObject = if (hasUnmappedOptionalParams) {
            val map = mutableMapOf<KParameter, Any?>()
            for (r in paramReaders) map[r.param!!] = null
            {
                var i = 0
                for (entry in map) entry.setValue(values[i++])
                constr.callBy(map)
            }
        } else
            fun () = constr.call(*values)

        // For each row in ResultSet read values by readers and pass them to primary constructor.
        val results = if (capacity >= 0) ArrayList<T>(capacity) else ArrayList()
        while (rs.next()) {
            for (i in paramReaders.indices) {
                val (read, colIndex, type) = paramReaders[i]
                values[i] = rs.read(colIndex, type)
            }
            val obj = createObject()
            results.add(obj)
            if (propReaders != null)
                for ((prop, reader) in propReaders) {
                    val (read, colIndex, type) = reader
                    val value = rs.read(colIndex, type)
                    prop.set(obj, value)
                }
        }
        results
    }

    private fun <T: Any> createReaders(params: List<KParameter>, colIndices: MutableMap<String, Int>, classType: KClass<T>):
            Triple<Boolean, List<Reader>, List<Pair<KMutableProperty1<T, Any?>, Reader>>?>
    {
        val customNames = getParamsCustomNames(classType, params)
        val paramReaders = ArrayList<Reader>(params.size)
        var hasUnmappedOptionalParams = false
        for (param in params) {
            val paramName = customNames[param]?.toPlainName() ?: param.name!!.lowercase()
            val colIndex = colIndices.remove(paramName) // remove instead of get to check further if there are any columns left
            if (colIndex != null)
                paramReaders.add(createReader(param.type, colIndex, param, classType.qualifiedName))
            else
                if (param.isOptional) hasUnmappedOptionalParams = true
                else throw SQLException("ResultSet doesn't has column that maps to required parameter " +
                        "'${param.name}' of '${classType.qualifiedName}' primary constructor. " +
                        "If it's not necessary to read value for this parameter from database, " +
                        "then just provide default value in its declaration. If column name differs from " +
                        "parameter name (besides case and delimiters), then annotate parameter with @SqlName.")
        }
        // If there are columns in result set besides that correspond to primary constructor parameters,
        // then try to map them to class properties.
        val propReaders = if (colIndices.isNotEmpty()) {
            val readers = mutableListOf<Pair<KMutableProperty1<T, Any?>, Reader>>()
            for (prop in classType.memberProperties) {
                val propName = customName(prop)?.toPlainName() ?: prop.name.lowercase()
                val colIndex = colIndices[propName]
                if (colIndex != null) {
                    @Suppress("UNCHECKED_CAST")
                    val p = prop as? KMutableProperty1<T, Any?>
                        ?: throw SqlPalException("Result set contains column that corresponds to property " +
                                "'${prop.name}' of '${classType.qualifiedName}' class, but property is not mutable." +
                                "Change property declaration from val to var.")
                    val reader = createReader(prop.returnType, colIndex, null, classType.qualifiedName)
                    readers.add(p to reader)
                }
            }
            readers
        } else
            null
        return Triple(hasUnmappedOptionalParams, paramReaders, propReaders)
    }

    @PublishedApi
    internal fun <T: Any> readValues(valueType: KClass<T>, capacity: Int, con: Connection?) = doAction(con) { stmt ->
        val rs = stmt.executeQuery()
        val results = if (capacity >= 0) ArrayList<T>(capacity) else ArrayList()
        val (read, colIndex, type) = createReader(valueType.createType(),1, null, "value")
        while (rs.next())
            @Suppress("UNCHECKED_CAST")
            results.add(rs.read(colIndex, type) as T)
        results
    }

    internal fun execAndReadResults(entity: Any, con: Connection?, autoGenColumns: RefreshMap): Int {
        val autoGenArr = if (autoGenColumns.isNotEmpty()) autoGenColumns.keys.toTypedArray() else null

        return doAction(con, autoGenArr) { cmd ->
            val rowsAffected = cmd.executeUpdate()
            if (rowsAffected > 0 && autoGenArr != null)
                cmd.generatedKeys.use { rs ->
                    rs.next()
                    val className = entity::class.qualifiedName
                    for (i in 1..rs.metaData.columnCount) {
                        val prop = autoGenColumns[rs.metaData.getColumnLabel(i)] ?: continue
                        val (read, _, type) = createReader(prop.returnType, i, null, className)
                        val value = rs.read(i, type)
                        prop.set(entity, value)
                    }
                }
            rowsAffected
        }
    }

    private fun createReader(type: KType, colIndex: Int, param: KParameter?, className: String?): Reader
    {
        val isList = type.kClass?.isSubclassOf(List::class) == true
        val isArray = type.kClass?.java?.isArray == true // arrays don't have base type (there are IntArray, ByteArray, etc.), so use isArray prop.

        // For List and Array, we need type of its generic.
        val valueType = if (isList || isArray) type.arguments[0].type!! else type

        val customReader = getCustomReader(type)

        val reader = if (customReader != null) { i, _ -> customReader(this, i) }
        else if (type.isEnum) { i, t -> getString(i)?.toEnum(t) }
        else if (isList)
            if (SqlPal.storeAsJson(type.kClass == ByteArray::class)) ResultSet::readJsonToList
            else if (valueType.isEnum) ResultSet::readEnumList
            else fun ResultSet.(i, _) = (getArray(i)?.array as Array<*>?)?.toList()
        else if (isArray)
            if (SqlPal.storeAsJson(type.kClass == ByteArray::class)) { i, t -> readJsonToList(i, t)?.toArrayOfType(valueType) }
            else if (type.kClass?.java?.componentType?.isEnum == true) ResultSet::readEnumArray
            else if (type.classifier == ByteArray::class) fun ResultSet.(i, _) = getBytes(i)
            else fun ResultSet.(i, _) = getArray(i)?.array
        else when (type.classifier) {
            String::class -> { i, _ -> getString(i) }
            Int::class -> valueTypeReader(type, ResultSet::getInt)
            Long::class -> valueTypeReader(type, ResultSet::getLong)
            Byte::class -> valueTypeReader(type, ResultSet::getByte)
            Short::class -> valueTypeReader(type, ResultSet::getShort)
            Float::class -> valueTypeReader(type, ResultSet::getFloat)
            Double::class -> valueTypeReader(type, ResultSet::getDouble)
            Boolean::class -> valueTypeReader(type, ResultSet::getBoolean)

            // Read JSR-310 standard types via getObject
            // (it's preferable than getTimestamp as getTimestamp implicitly alters zone
            // if local default time zone differs from the database session time zone.)
            LocalDate::class, LocalTime::class, LocalDateTime::class,
            OffsetTime::class, OffsetDateTime::class -> { i, t -> getObject(i, (t.classifier as KClass<*>).java) }
            ZonedDateTime::class -> { i, _ -> getObject(i, OffsetDateTime::class.java)?.toZonedDateTime() }
            Instant::class -> { i, _ -> getObject(i, OffsetDateTime::class.java)?.toInstant() }

            BigDecimal::class -> { i, _ -> getBigDecimal(i) }
            Currency::class -> { i, _ -> getString(i)?.let { Currency.getInstance(it) } }

            Blob::class -> { i, _ -> getBlob(i) }
            Clob::class -> { i, _ -> getClob(i) }
            SQLXML::class -> { i, _ -> getSQLXML(i) }
            UUID::class -> { i, _ -> getObject(i) } // Not guaranteed for all DB, but supported at least by Postgres.

            else -> throw SqlPalException("Property '${param?.name}' of $className class has type '${type.classifier}', " +
                    "for witch mapping to SQL type is not implemented. " +
                    "To provide mapper for '${type.classifier}' add it to Sql.valueMappers " +
                    "to support it across the entire app, or annotate this property with @Mapper annotation."
            )
        }
        return Reader(reader, colIndex, valueType, param)
    }

    private fun getCustomReader(type: KType): KFunction2<ResultSet, Int, Any?>? {
        var mapper = type.findAnnotation<Mapper>()?.mapper?.run { objectInstance ?: createInstance() }
        if (mapper == null)
            mapper = SqlPal.valueMappers[type.classifier]
        return if (mapper != null) mapper::readValue else null
    }

    private inline fun <T> valueTypeReader(valueType: KType, crossinline getValue: ResultSet.(Int) -> T): ResultSet.(Int, KType) -> Any? =
        if (valueType.isMarkedNullable)
            { i, _ -> valueOrNull { getValue(i) } }
        else
            { i, _ -> getValue(i) }

    /** Calls [fillItemParams] for each item in [items] and to set bind parameters and executes query as batch.
     * @param con If specified, then command is executed on it, and it is not closed after use.
     * Otherwise, connection is obtained from pool and released after use.
     * @param items iterable source of items for bach processing.
     * @param fillItemParams is called for each item in [items].
     * First argument is item to process, second is list where to add values that will be set as bind parameters.
     * @return array where each element is number of affected rows by each item.*/
    fun doBatch(con: Connection?, items: Iterable<Any>, fillItemParams: (Any, MutableList<Any?>) -> Unit): IntArray =
        doAction(con, null, true) {
            for (item in items) {
                bindParams.clear()
                fillItemParams(item, bindParams)
                setBindParams(it)
                it.addBatch()
            }
            it.executeBatch()
        }

    /** Runs specified action with [PreparedStatement].
     * @param con If specified, then command is executed on it, and it is not closed after use.
     * Otherwise, connection is obtained from pool and released after use.
     * @param action to execute with [PreparedStatement].
     * @return number of rows affected. */
    fun <T> doAction(con: Connection?, action: (PreparedStatement) -> T) =
        doAction(con, null, false, action)

    /** Runs specified action with [PreparedStatement] and specified columns which values should be returned.
     * @param con If specified, then command is executed on it, and it is not closed after use.
     * Otherwise, connection is obtained from pool and released after use.
     * @param autoGenColumns array where to store values from auto-generated columns.
     * @param isBatch if true, then bind parameters are not set from [bindParams] as batch assumes multiple statements.
     * @param action to execute with [PreparedStatement].
     * @return number of rows affected. */
    fun <T> doAction(con: Connection?, autoGenColumns: Array<String>?, isBatch: Boolean = false, action: (PreparedStatement) -> T) =
        if (con != null)
            doActionOnConnection(con, autoGenColumns, isBatch, action)
        else
            SqlPal.withConnection { doActionOnConnection(it, autoGenColumns, isBatch, action) }

    private inline fun <T> doActionOnConnection(con: Connection, autoGenColumns: Array<String>?,
                                                isBatch: Boolean, action: (PreparedStatement) -> T) =
        con.prepareStatement(sql, autoGenColumns).use {
            // For batch, params will be set inside action.
            if (!isBatch) setBindParams(it)
            action(it)
        }

    private fun setBindParams(statement: PreparedStatement) {
        for (index in 1..bindParams.count()) {
            val paramValue = bindParams[index - 1]

            if (paramValue == null) {
                statement.setObject(index, null)
                return
            }
            val (value, componentType) = if (paramValue is ListAndType)
                paramValue.list to paramValue.componentType
            else
                paramValue to paramValue::class.java.componentType?.kotlin

            if (SqlPal.valueMappers[value::class]?.writeValue(value, statement, index, componentType) == true)
                return

            when (value) {
                is Enum<*> -> statement.setObject(index, value, Types.OTHER)
                is ZonedDateTime -> statement.setObject(index, value.toOffsetDateTime())
                is Instant -> statement.setObject(index, value.atOffset(ZoneOffset.UTC))
                is Currency -> statement.setString(index, value.toString())
                else -> if (value is List<*> || value::class.java.isArray) // Arrays don't have base type, so use isArray.
                    setArray(statement, index, value, componentType)
                else
                    statement.setObject(index, value) // Other primitive types are directly supported by JDBC.
            }
        }
    }

    // Represents values necessary to uniformly process any kind of iterable source
    // regardless of its type (List, Array<*>, ByteArray, etc.).
    private class Items(val iterator: Iterator<*>, val size: Int, val isTypedArray: Boolean = true)

    private fun setArray(statement: PreparedStatement, index: Int, value: Any, componentType: KClass<out Any>?) {
        componentType ?: throw IllegalArgumentException("Query has parameter of type List<*> that is not wrapped " +
                "with ListAndType object, what indicates a bug or incorrect use of SqlPal.")

        val items = getItems(value)

        if (SqlPal.storeAsJson(value is ByteArray)) {
            if (componentType == Any::class) throw SqlPalException("Parameter $index is List/Array of Any. " +
                    "It can't be serialized to JSON. Only lists/arrays of certain type are supported.")

            val json = JsonMapper.serialize(items.isTypedArray, items.iterator, componentType)
            statement.setString(index, json)
        }
        else if (componentType.java.isEnum)
            setEnumArray(statement, index, items, componentType)
        else {
            // JDBC supports arrays but not lists, so if it's List, then convert it to Array.
            // Array must be of certain type, not array of Any, otherwise database driver would not be able
            // to figure out to what SQL type map it to. So create it via reflection to explicitly specify type.
            val array = if (value is List<*>) value.toArrayOfType(componentType) else value
            statement.setObject(index, array, Types.ARRAY)
        }
    }

    private fun getItems(value: Any) =
        // There is no base class for arrays, but all arrays and lists have iterator, so get it to iterate over array.
        when (value) {
            is List<*> -> Items(value.iterator(), value.size, false)
            is Array<*> -> Items(value.iterator(), value.size, false)
            is ByteArray -> Items(value.iterator(), value.size)
            is ShortArray -> Items(value.iterator(), value.size)
            is IntArray -> Items(value.iterator(), value.size)
            is LongArray -> Items(value.iterator(), value.size)
            is FloatArray -> Items(value.iterator(), value.size)
            is DoubleArray -> Items(value.iterator(), value.size)
            is BooleanArray -> Items(value.iterator(), value.size)
            else -> Items((value as CharArray).iterator(), value.size)
        }

    private fun setEnumArray(statement: PreparedStatement, index: Int, items: Items, componentType: KClass<*>) {
        // Convert enum values to strings.
        val array = Array(items.size) { (items.iterator.next() as Enum<*>).name }

        val con = statement.connection
        if (SqlPal.useEnumArrays && con.metaData.databaseProductName.lowercase() == "postgresql") {
            val sqlArray = con.createArrayOf(entityName(componentType), array)
            statement.setArray(index, sqlArray)
        } else
            statement.setObject(index, array, Types.ARRAY)
    }
}

private fun ResultSet.readJsonToList(colIndex: Int, componentType: KType): List<*>? {
    val json = getString(colIndex) ?: return null
    return JsonMapper(json, colIndex, componentType).parse()
}

@Suppress("UNCHECKED_CAST")
private fun ResultSet.readEnumArray(colIndex: Int, enumType: KType): Array<Enum<*>>?
{
    val sqlArr = getArray(colIndex) ?: return null
    val arr = sqlArr.array as Array<String>

    // Enum array must be typed by certain enum, not Enum<*>,
    // otherwise type mismatch will occur on assigning it to appropriate property.
    // So create it via reflection to explicitly specify type.
    val enumArray = java.lang.reflect.Array.newInstance(enumType.javaClass, arr.size) as Array<Enum<*>>
    for (i in arr.indices) enumArray[i] = arr[i].toEnum(enumType)
    return enumArray
}

@Suppress("UNCHECKED_CAST")
private fun ResultSet.readEnumList(colIndex: Int, enumType: KType): List<Enum<*>>?
{
    val sqlArr = getArray(colIndex) ?: return null
    val arr = sqlArr.array as Array<String>
    return arr.map { it.toEnum(enumType) }
}

private fun List<*>.toArrayOfType(componentType: KType) = componentType.kClass?.let { toArrayOfType(it) }
@Suppress("UNCHECKED_CAST")
private fun List<*>.toArrayOfType(componentType: KClass<*>): Array<Any?> {
    // Using reflection to create array of specified type,
    // as using List.toTypedArray will produce Array<Any> due to generic type erasure.
    val array = (java.lang.reflect.Array.newInstance(componentType.java, size) as Array<Any?>)
    for (i in indices) array[i] = this[i]
    return array
}

/** Does next:
 * - removes quotes (if quoted),
 * - removes chars that can be used as delimiters in names in database,
 * - converts to lowercase. */
private fun String.toPlainName(): String {
    if (isEmpty()) return this

    val sb = StringBuilder()
    val f = this[0]
    val l = this[length - 1]
    val isQuoted = (f == '"' && l == '"') || (f == '[' && l == ']') || (f == '`' && l == '`')

    var i = if (isQuoted) 1 else 0
    val len = length - i
    while (i < len) {
        when (this[i]) {
            '_', '-', '.', ' ' -> {}
            else -> sb.append(this[i].lowercaseChar())
        }
        i++
    }
    return sb.toString()
}


