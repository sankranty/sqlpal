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
        val (hasUnmappedOptionalParams, paramReaders, propReaders) = createReaders(constr.parameters, colIndices, classType, stmt)
        val values = arrayOfNulls<Any>(paramReaders.count()) // Array where to read values for each row

        // If primary constructor has optional params, for which there are no corresponding columns,
        // then also create map, to specify only parameters, that have columns in result set.
        val createObject = if (hasUnmappedOptionalParams) {
            val map = mutableMapOf<KParameter, Any?>()
            for (r in paramReaders) map[r.param!!] = null // add parameters to the map
            val paramEntries = map.entries.toTypedArray(); // create array of map entries as it's faster to iterate over an array than a map
            {
                var i = 0
                while (i < paramEntries.size) paramEntries[i].setValue(values[i++])
                constr.callBy(map)
            }
        } else
            fun () = constr.call(*values)

        // For each row in ResultSet read values by readers and pass them to primary constructor.
        val paramIndices = paramReaders.indices
        val results = if (capacity >= 0) ArrayList<T>(capacity) else ArrayList()
        while (rs.next()) {
            for (i in paramIndices) {
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

    private fun <T: Any> createReaders(params: List<KParameter>, colIndices: MutableMap<String, Int>,
                                       classType: KClass<T>, stmt: PreparedStatement):
            Triple<Boolean, List<Reader>, List<Pair<KMutableProperty1<T, Any?>, Reader>>?>
    {
        val customNames = getParamsCustomNames(classType, params)
        val paramReaders = ArrayList<Reader>(params.size)
        var hasUnmappedOptionalParams = false
        for (param in params) {
            val paramName = customNames[param]?.toPlainName() ?: param.name!!.lowercase()
            val colIndex = colIndices.remove(paramName) // remove instead of get to check further if there are any columns left
            if (colIndex != null)
                paramReaders.add(createReader(param.type, colIndex, param, classType.qualifiedName, stmt))
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
                    val reader = createReader(prop.returnType, colIndex, null, classType.qualifiedName, stmt)
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
        val (read, colIndex, type) = createReader(valueType.createType(),1, null, "value", stmt)
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
                    if (!rs.next()) throw SQLException("INSERT/UPDATE command affected non-zero rows, " +
                            "but generated keys for requested columns were not returned by driver/database.")
                    val className = entity::class.qualifiedName
                    for (i in 1..rs.metaData.columnCount) {
                        val prop = autoGenColumns[rs.metaData.getColumnLabel(i).lowercase()] ?: continue
                        val (read, _, type) = createReader(prop.returnType, i, null, className, cmd)
                        val value = rs.read(i, type)
                        prop.set(entity, value)
                    }
                }
            rowsAffected
        }
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    private fun createReader(type: KType, colIndex: Int, param: KParameter?, className: String?, stmt: PreparedStatement): Reader
    {
        var valueType = type
        val customReader = getCustomReader(type)

        val reader = if (customReader != null) { i, _ -> customReader(this, i) }
        else if (type.isEnum) { i, t -> getString(i)?.toEnum(t) }
        else when (type.classifier) {
            String::class -> { i, _ -> getString(i) }
            Int::class -> valueTypeReader(type, param, className, ResultSet::getInt)
            Long::class -> valueTypeReader(type, param, className, ResultSet::getLong)
            Byte::class -> valueTypeReader(type, param, className, ResultSet::getByte)
            Short::class -> valueTypeReader(type, param, className, ResultSet::getShort)
            Float::class -> valueTypeReader(type, param, className, ResultSet::getFloat)
            Double::class -> valueTypeReader(type, param, className, ResultSet::getDouble)
            Boolean::class -> valueTypeReader(type, param, className, ResultSet::getBoolean)

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

            UInt::class -> valueTypeReader(type, param, className) { getInt(it).toUInt() }
            ULong::class -> valueTypeReader(type, param, className) { getLong(it).toULong() }
            UByte::class -> valueTypeReader(type, param, className) { getByte(it).toUByte() }
            UShort::class -> valueTypeReader(type, param, className) { getShort(it).toUShort() }
            else -> { // Handle collections and arrays
                val isList = type.kClass?.isSubclassOf(List::class) == true
                val isSet = type.kClass?.isSubclassOf(Set::class) == true
                val isArray = type.kClass?.java?.isArray == true // arrays don't have base type (there are IntArray, ByteArray, etc.), so use isArray prop.
                val unsignedArrayType = getUnsignedArrayType(type.kClass!!)

                if (isList || isSet || isArray || unsignedArrayType != null) {
                    // For collection or array, get type of its elements.
                    // Type of unboxed array (e.g. IntArray) does not have generic arguments,
                    // in this case obtain type of elements via componentType property.
                    valueType = if (type.arguments.isNotEmpty()) type.arguments[0].type!!
                    else if (isArray) type.kClass!!.java.componentType.kotlin.createType()
                    else unsignedArrayType!!.createType()

                    val jsonMapper = if (SqlPal.storeAsJson(type)) JsonMapper(colIndex, valueType) else null
                    if (isList)
                        if (jsonMapper != null) { i, _ -> jsonMapper.parseList(getString(i)) }
                        else if (valueType.isEnum) ResultSet::readEnumList
                        else fun ResultSet.(i, _) = readArray(i)?.toList()
                    else if (isSet)
                        if (jsonMapper != null) { i, _ -> jsonMapper.parseSet(getString(i)) }
                        else if (valueType.isEnum) ResultSet::readEnumSet
                        else fun ResultSet.(i, _) = readArray(i)?.toSet()
                    else // It's array
                        if (jsonMapper != null) { i, _ -> jsonMapper.parseList(getString(i))?.toArrayOfType(type) }
                        else if (valueType.isEnum) ResultSet::readEnumArray
                        else if (type.classifier == ByteArray::class) { i, _ -> getBytes(i) }
                        else if (type.classifier == UByteArray::class) { i, _ -> getBytes(i)?.toArrayOfType(type) }
                        // Due to Kotlin bug https://github.com/Kotlin/dataframe/issues/678
                        // type.classifier returns IntArray for Array<Int> (similarly for other primitive types),
                        // so distinguish Array<*> from unboxed arrays by not empty generic arguments.
                        // Also, only Postgres returns typed arrays. H2 returns Array<Any>, what is handled in next branch.
                        else if (type.arguments.isNotEmpty() && stmt.isPostgres) fun ResultSet.(i, _) = getArray(i)?.array
                        // If type is unboxed array then convert Array<*> to unboxed array
                        else fun ResultSet.(i, _) = readArray(i)?.toArrayOfType(type)
                }
                else if (type.kClass?.isSubclassOf(Map::class) == true) {
                    val jsonMapper = JsonMapper(colIndex, type.arguments[1].type!!, type.arguments[0].type!!);
                    { i, _ -> jsonMapper.parseMap(getString(i)) }
                }
                else throw SqlPalException("Property '${param?.name}' of $className class has type '${type.classifier}', " +
                            "for witch mapping to SQL type is not implemented. " +
                            "To provide mapper for '${type.classifier}' use SqlPal.addTypeMapper method " +
                            "to support it across the entire app, or annotate this property with @Mapper annotation."
                    )
            }
        }
        return Reader(reader, colIndex, valueType, param)
    }

    private fun getCustomReader(type: KType): KFunction2<ResultSet, Int, Any?>? {
        var mapper = type.findAnnotation<Mapper>()?.mapper?.run { objectInstance ?: createInstance() }
        if (mapper == null)
            mapper = SqlPal.valueMappers[type.classifier]
        return if (mapper != null) mapper::readValue else null
    }

    private inline fun <T> valueTypeReader(valueType: KType, param: KParameter?, className: String?,
                                           crossinline getValue: ResultSet.(Int) -> T): ResultSet.(Int, KType) -> Any? =
        if (valueType.isMarkedNullable)
            { i, _ -> valueOrNull { getValue(i) } }
        else
            { i, _ -> valueOrNull { getValue(i) } ?:
            throw SQLException("NULL is read from column that maps to property '${param?.name}' of '$className' class, " +
                    "that has not nullable type '${valueType.classifier}'. " +
                    "Mark property nullable or set values in the column to non-null values.") }

    /** Calls [fillItemParams] for each item in [items] and to set bind parameters and executes query as batch.
     * @param con If specified, then command is executed on it, and it is not closed after use.
     * Otherwise, connection is obtained from pool and released after use.
     * @param items iterable source of items for bach processing.
     * @param fillItemParams is called for each item in [items].
     * First argument is item to process, second is list where to add values that will be set as bind parameters.
     * @return array where each element is number of affected rows by each item.*/
    fun doBatch(con: Connection?, items: Iterable<Any>, fillItemParams: (Any, MutableList<Any?>) -> Unit): IntArray =
        doAction(con, null, isBatch = true) {
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
        doAction(con, null, action = action)

    /** Runs specified action with [PreparedStatement] and specified columns which values should be returned.
     * @param con If specified, then command is executed on it, and it is not closed after use.
     * Otherwise, connection is obtained from pool and released after use.
     * @param generatedColumns array of column names for witch to return values after execution.
     * @param requestGeneratedColumns true to request database to return any changed values after INSERT or UPDATE.
     * Value is ignored if [generatedColumns] is not null.
     * @param isBatch if true, then bind parameters are not set from [bindParams] as batch assumes multiple statements.
     * @param action to execute with [PreparedStatement].
     * @return number of rows affected. */
    fun <T> doAction(con: Connection?, generatedColumns: Array<String>?,
                     requestGeneratedColumns: Boolean = false,
                     isBatch: Boolean = false, action: (PreparedStatement) -> T) =
        if (con != null)
            doActionOnConnection(con, generatedColumns, requestGeneratedColumns, isBatch, action)
        else
            SqlPal.withConnection { doActionOnConnection(it, generatedColumns, requestGeneratedColumns, isBatch, action) }

    private inline fun <T> doActionOnConnection(con: Connection, generatedColumns: Array<String>?,
                                                requestGeneratedColumns: Boolean,
                                                isBatch: Boolean, action: (PreparedStatement) -> T) =
        when {
            generatedColumns != null -> con.prepareStatement(sql, generatedColumns)
            requestGeneratedColumns -> con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
            else -> con.prepareStatement(sql)
        }.use {
            // For batch, params will be set inside action.
            if (!isBatch) setBindParams(it)
            action(it)
        }

    private fun setBindParams(statement: PreparedStatement) {
        for (index in 1..bindParams.count()) {
            val paramValue = bindParams[index - 1]

            if (paramValue == null) {
                statement.setObject(index, null)
                continue
            }
            val (value, componentType) = if (paramValue is CollectionAndType)
                paramValue.list to paramValue.componentType
            else
                paramValue to paramValue::class.java.componentType?.kotlin

            if (SqlPal.valueMappers[value::class]?.writeValue(value, statement, index, componentType) == true)
                continue

            when (value) {
                is Enum<*> ->
                    if (statement.isPostgres) statement.setObject(index, value, Types.OTHER)
                    else statement.setString(index, value.name)
                is ZonedDateTime -> statement.setObject(index, value.toOffsetDateTime())
                is Instant -> statement.setObject(index, value.atOffset(ZoneOffset.UTC))
                is Currency -> statement.setString(index, value.toString())
                is UInt -> statement.setInt(index, value.toInt())
                is ULong -> statement.setLong(index, value.toLong())
                is UByte -> statement.setByte(index, value.toByte())
                is UShort -> statement.setShort(index, value.toShort())
                is Map<*, *> -> {
                    val json = JsonMapper.serialize(value, componentType?: throwNotWrapped(index))
                    statement.setString(index, json)
                }
                else -> if (value is Collection<*> || value::class.java.isArray) // Arrays don't have base type, so use isArray.
                    setArray(statement, index, value, componentType)
                else
                    statement.setObject(index, value) // Other primitive types are directly supported by JDBC.
            }
        }
    }

    private fun throwNotWrapped(index: Int): Nothing =
        throw IllegalArgumentException("Query parameter at index $index is of Collection type " +
                "but is not wrapped with the CollectionAndType object, what indicates a bug or incorrect use of SqlPal.")

    private fun setArray(statement: PreparedStatement, index: Int, value: Any, componentType: KClass<out Any>?) {
        fun Any.toBoxedArray(): Any = when (this) {
            is IntArray     -> toTypedArray()
            is LongArray    -> toTypedArray()
            is ShortArray   -> toTypedArray()
            is FloatArray   -> toTypedArray()
            is DoubleArray  -> toTypedArray()
            is BooleanArray -> toTypedArray()
            is CharArray    -> toTypedArray()
            else            -> this
        }
        componentType ?: throwNotWrapped(index)

        if (SqlPal.storeAsJson(value is ByteArray)) {
            if (componentType == Any::class) throw SqlPalException("Parameter $index is List/Array of Any. " +
                    "It can't be serialized to JSON. Only lists/arrays of certain type are supported.")

            val items = getItems(value)
            val json = JsonMapper.serialize(items.isTypedArray, items.iterator, componentType)
            statement.setString(index, json)
        }
        else if (componentType.java.isEnum)
            setEnumArray(statement, index, getItems(value), componentType)
        else {
            // Kotlin unsigned arrays (e.g. UIntArray) are not recognized by JDBC,
            // thus considering that they are value classes, get their underlying value that is normal array.
            // Check for if before check for Collection as unsigned arrays implement Collection interface.
            var array = if (getUnsignedArrayType(value) != null) unwrapValueClass(value)
            // JDBC supports arrays but not collections, so if it's a Collection, then convert it to Array.
            // Array must be of certain type, not array of Any, otherwise database driver would not be able
            // to figure out to what SQL type map it to. So create it via reflection to explicitly specify type.
            else if (value is Collection<*>) value.toArrayOfType(componentType)
            else value

            // Unlike Postgres, H2 driver supports only boxed arrays, so box it if it's unboxed array.
            if (!statement.isPostgres) array = array.toBoxedArray()

            // Don't specify Types.ARRAY for the setObject, as if it's a ByteArray,
            // then it should be stored as a binary object, not as an array.
            statement.setObject(index, array)
        }
    }

    private fun setEnumArray(statement: PreparedStatement, index: Int, items: Items, componentType: KClass<*>) {
        // Convert enum values to strings.
        val array = Array(items.size) { (items.iterator.next() as Enum<*>?)?.name }

        if (SqlPal.useEnumArrays && statement.isPostgres) {
            val sqlArray = statement.connection.createArrayOf(entityName(componentType), array)
            statement.setArray(index, sqlArray)
        } else
            statement.setObject(index, array, Types.ARRAY)
    }

    private val PreparedStatement.isPostgres get() =
        connection.metaData.databaseProductName.lowercase() == "postgresql"
}

private fun ResultSet.readArray(colIndex: Int) = getArray(colIndex)?.array as Array<*>?

@Suppress("UNCHECKED_CAST")
private fun ResultSet.readEnumArray(colIndex: Int, enumType: KType): Array<Enum<*>>?
{
    val sqlArr = getArray(colIndex) ?: return null
    val arr = sqlArr.array as Array<String>

    // Enum array must be typed by certain enum, not Enum<*>,
    // otherwise type mismatch will occur on assigning it to appropriate property.
    // So create it via reflection to explicitly specify type.
    val enumClass = (enumType.classifier as? KClass<*>)?.java
    val enumArray = java.lang.reflect.Array.newInstance(enumClass, arr.size) as Array<Enum<*>>
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

@Suppress("UNCHECKED_CAST")
private fun ResultSet.readEnumSet(colIndex: Int, enumType: KType): Set<Enum<*>>?
{
    val sqlArr = getArray(colIndex) ?: return null
    val arr = sqlArr.array as Array<String>
    val set = mutableSetOf<Enum<*>>()
    arr.forEach { set.add(it.toEnum(enumType)) }
    return set
}

private fun ByteArray.toArrayOfType(type: KType) = iterator().toArrayOfType(type, size)

private fun Array<*>.toArrayOfType(type: KType) = iterator().toArrayOfType(type, size)

private fun Collection<*>.toArrayOfType(type: KType) = iterator().toArrayOfType(type, size)

@OptIn(ExperimentalUnsignedTypes::class)
private fun Iterator<*>.toArrayOfType(type: KType, size: Int) = when (type.kClass) {
    UIntArray::class -> UIntArray(size) { next().let { if (it is Int) it.toUInt() else it as UInt } }
    ULongArray::class -> ULongArray(size) { next().let { if (it is Long) it.toULong() else it as ULong } }
    UShortArray::class -> UShortArray(size) { next().let { if (it is Short) it.toUShort() else it as UShort } }
    UByteArray::class -> UByteArray(size) { next().let { if (it is Byte) it.toUByte() else it as UByte } }
    else -> toArrayOfType(type.componentType!!, size)
}

private fun Collection<*>.toArrayOfType(componentType: KClass<*>) = iterator().toArrayOfType(componentType.java, size)

private fun Iterator<*>.toArrayOfType(componentType: Class<*>, size: Int) =
    // Using reflection to create array of specified type,
    // as using Collection.toTypedArray will produce Array<Any> due to generic type erasure.
    java.lang.reflect.Array.newInstance(componentType, size).also { array ->
        var i = 0
        // Unboxed arrays (e.g. IntArray) don't have a base type, so using Array.set to set values.
        forEach { java.lang.reflect.Array.set(array, i++, it) }
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


