package org.sqlpal

import java.math.BigDecimal
import java.sql.*
import java.time.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure

/** Encapsulates SQL-query with bind parameters and provides methods to execute it.
 * If connection is specified, then command is executed on it, otherwise connection is obtained from pool.
 *
 * Instances of this class are created by -"..." or -"""...""" syntax.
 * It is not recommended to work with it directly, just pass it to methods that accept it. */
class Cmd @PublishedApi internal constructor(
    val sql: String,
    val bindParams: MutableList<Any?>,
) {
    @PublishedApi
    internal companion object
    {
        inline fun <reified T: Any> createFindByIdCmd(id: Long): Cmd {
            val idCol = colName(getIdProperty(T::class))
            return Sql("SELECT * FROM $I${entityName(T::class)} WHERE $I$idCol = $id")
        }

        fun <T: Any> getConstructor(type: KClass<T>): KFunction<T> {
            val error = "Class must have primary constructor where are declared all properties that should be read from database."
            val constr = type.primaryConstructor ?: throw SqlPalException(error)
            if (constr.parameters.isEmpty()) throw SqlPalException(error)
            return constr
        }

        fun <T: Any> getIdProperty(type: KClass<T>) = type.memberProperties.find { it.hasAnnotation<Id>() }
            ?: throw SqlPalException("Unable to generate WHERE clause for statement on ${type.simpleName} as it does not have field annotated with @Id")

        /** Converts String from camelCase to snake_case. */
        fun camel2Snake(name: String): String {
            val sb = StringBuilder()
            for (i in name.indices) {
                if (i > 0 && name[i].isUpperCase())
                    sb.append('_')
                sb.append(name[i].lowercaseChar())
            }
            return sb.toString()
        }

        /** Converts String from snake_case to camelCase.*/
        fun snake2Camel(name: String): String {
            var i = 0
            val sb = StringBuilder()
            while (i < name.length) {
                if (name[i] != '_') sb.append(name[i])
                else if (i++ < name.length) sb.append(name[i].uppercase())
                i++
            }
            return sb.toString()
        }

        /** Does next:
         * - removes quotes (if quoted),
         * - removes chars that can be used as delimiters in names in database,
         * - converts to lowercase. */
        fun String.toPlainName(): String {
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
    }

    private data class Reader(
        val read: ResultSet.(Int, KType) -> Any?,
        val colIndex: Int,
        val type: KType,
        val param: KParameter?
    )

    private var hasUnmappedOptionalParams = false

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

        // Create reader for each constructor parameter
        val constr = getConstructor(classType)
        val readers = createReaders(constr.parameters, colIndices, classType)
        val values = arrayOfNulls<Any>(readers.count()) // Array where to read values for each row

        // If primary constructor has optional params, for which there are no corresponding columns,
        // then also create map, to specify only parameters, that have columns in result set.
        val createObject = if (hasUnmappedOptionalParams) {
            val map = mutableMapOf<KParameter, Any?>()
            for (r in readers) map[r.param!!] = null
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
            for (i in readers.indices) {
                val (read, colIndex, type) = readers[i]
                values[i] = rs.read(colIndex, type)
            }
            results.add(createObject())
        }
        results
    }

    private fun createReaders(params: List<KParameter>, colIndices: Map<String, Int>, classType: KClass<*>): List<Reader>
    {
        val customNames = getParamsCustomNames(classType, params)
        val readers = ArrayList<Reader>(params.size)
        hasUnmappedOptionalParams = false
        for (param in params) {
            val paramName = customNames[param]?.toPlainName() ?: param.name!!.lowercase()
            val colIndex = colIndices[paramName]
            if (colIndex != null)
                readers.add(createReader(param.type, colIndex, param, classType.qualifiedName))
            else
                if (param.isOptional) hasUnmappedOptionalParams = true
                else throw SQLException("ResultSet doesn't has column that maps to required parameter " +
                        "'${param.name}' of '${classType.qualifiedName}' primary constructor. " +
                        "If it's not necessary to read value for this parameter from database, " +
                        "then just provide default value in its declaration. If column name differs from " +
                        "parameter name (besides case and delimiters), then annotate parameter with @SqlName.")
        }
        return readers
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

    private fun createReader(type: KType, colIndex: Int, param: KParameter?, className: String?): Reader
    {
        val kClassType = (type.classifier as? KClass<*>)
        val classType = kClassType?.java

        val isList = kClassType?.isSubclassOf(List::class) == true
        val isArray = classType?.isArray == true // arrays does not have base type (e.g. IntArray, ByteArray, etc.), so use isArray prop.

        // For List and Array, we need type of its generic.
        val valueType = if (isList || isArray) type.arguments[0].type!! else type

        val customReader = getCustomReader(type)

        val reader = if (customReader != null) { i, _ -> customReader(this, i) }
        else if (classType?.isEnum == true) { i, t -> getString(i)?.toEnum(t) }
        else if (isList)
            if ((valueType.classifier as? KClass<*>)?.java?.isEnum == true) ResultSet::readEnumList
            else fun ResultSet.(i, _) = (getArray(i)?.array as Array<*>?)?.toList()
        else if (isArray)
            if (classType?.componentType?.isEnum == true) ResultSet::readEnumArray
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
            mapper = Sql.valueMappers[type.classifier]
        return if (mapper != null) mapper::readValue else null
    }

    private inline fun <T> valueTypeReader(valueType: KType, crossinline getValue: ResultSet.(Int) -> T): ResultSet.(Int, KType) -> Any? =
        if (valueType.isMarkedNullable)
            { i, _ -> valueOrNull { getValue(i) } }
        else
            { i, _ -> getValue(i) }

    fun doBatch(items: Iterable<Any>, con: Connection?, fillItemParams: (Any, MutableList<Any?>) -> Unit): IntArray =
        doAction(null, con, true) {
            for (item in items) {
                bindParams.clear()
                fillItemParams(item, bindParams)
                for (i in 1..bindParams.count())
                    setBindParam(bindParams[i - 1], i, it)
                it.addBatch()
            }
            it.executeBatch()
        }

    /** Runs specified action with [PreparedStatement]. */
    fun <T> doAction(con: Connection?, action: (PreparedStatement) -> T) =
        doAction(null, con, false, action)

    /** Runs specified action with [PreparedStatement] and specified columns which values should be returned. */
    fun <T> doAction(autoGenColumns: Array<String>?, con: Connection?, isBatch: Boolean = false, action: (PreparedStatement) -> T) =
        if (con != null)
            doActionOnConnection(con, autoGenColumns, isBatch, action)
        else
            Sql.dataSource.connection.use { doActionOnConnection(it, autoGenColumns, isBatch, action) }

    private inline fun <T> doActionOnConnection(con: Connection, autoGenColumns: Array<String>?,
                                                isBatch: Boolean, action: (PreparedStatement) -> T) =
        con.prepareStatement(sql, autoGenColumns).use {
            if (!isBatch) // For batch params will be set inside action.
                for (i in 1..bindParams.count())
                    setBindParam(bindParams[i - 1], i, it)
            action(it)
        }

    private fun setBindParam(paramValue: Any?, index: Int, statement: PreparedStatement) {
        if (paramValue == null) {
            statement.setObject(index, null)
            return
        }
        val (value, componentType) = if (paramValue is ListAndType)
            paramValue.list to paramValue.componentType
        else
            paramValue to paramValue::class.java.componentType?.kotlin

        if (Sql.valueMappers[value::class]?.writeValue(value, statement, index, componentType) == true)
            return

        when (value) {
            is Enum<*> -> statement.setObject(index, value, Types.OTHER)
            is List<*> -> {
                if (componentType == null) throw IllegalArgumentException(
                    "List is not wrapped with ListAndType object, what indicates a bug or incorrect use of SqlPal.")

                if (componentType.java.isEnum)
                    setEnumArray({ value[it] }, value.size, componentType, index, statement)
                else {
                    // JDBC supports arrays but not lists, so convert List to Array.
                    // Array must be of certain type, not array of Any, otherwise driver would not be able to figure out
                    // to what SQL type map it to. So create it via reflection to explicitly specify type.
                    @Suppress("UNCHECKED_CAST")
                    val array = java.lang.reflect.Array.newInstance(componentType.javaObjectType, value.size) as Array<Any?>
                    for (i in value.indices)
                        array[i] = value[i]
                    statement.setObject(index, array, Types.ARRAY)
                }
            }
            is Array<*> ->
                if (componentType!!.java.isEnum)
                    setEnumArray({ value[it] }, value.size, componentType, index, statement)
                else
                    statement.setObject(index, value, Types.ARRAY)
            is ZonedDateTime -> statement.setObject(index, value.toOffsetDateTime())
            is Instant -> statement.setObject(index, value.atOffset(ZoneOffset.UTC))
            is Currency -> statement.setString(index, value.toString())
            else -> statement.setObject(index, value) // Other primitive types are directly supported by JDBC.
        }
    }

    private inline fun setEnumArray(getValue: (Int) -> Any?, itemCount: Int, componentType: KClass<*>,
                                    colIndex: Int, statement: PreparedStatement) {
        // Convert enum values to strings.
        val array = Array(itemCount) { (getValue(it) as Enum<*>).name }

        val con = statement.connection
        if (Sql.useEnumArrays && con.metaData.databaseProductName.lowercase() == "postgresql") {
            val sqlArray = con.createArrayOf(entityName(componentType), array)
            statement.setArray(colIndex, sqlArray)
        } else
            statement.setObject(colIndex, array, Types.ARRAY)
    }
}

@Suppress("UNCHECKED_CAST")
private fun ResultSet.readEnumArray(colIndex: Int, enumType: KType): Array<Enum<*>>?
{
    val sqlArr = getArray(colIndex) ?: return null
    val arr = sqlArr.array as Array<String>

    // Enum array must be typed by certain enum, not Enum<*>,
    // otherwise type mismatch will occur on assigning it to appropriate property.
    // So create it via reflection to explicitly specify type.
    val enumClass = (enumType.classifier as? KClass<*>)?.javaObjectType
    val enumArray = java.lang.reflect.Array.newInstance(enumClass, arr.size) as Array<Enum<*>>
    for (i in arr.indices)
        enumArray[i] = arr[i].toEnum(enumType)
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
private fun String.toEnum(enumType: KType) =
    java.lang.Enum.valueOf(enumType.jvmErasure.java as Class<out Enum<*>>, this)

