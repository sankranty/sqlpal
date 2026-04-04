package org.sqlpal

import org.sqlpal.query.PropsToUpdate
import java.sql.Connection
import kotlin.reflect.*
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

//////////////////////////////////////////////////////////////////////////////////
//--------------------- Contains internal utility methods ----------------------//
//////////////////////////////////////////////////////////////////////////////////

internal typealias RefreshMap = MutableMap<String, KMutableProperty1<Any, Any?>?>
private val emptyMap: RefreshMap = mutableMapOf() // static value to avoid creation on each call if not needed.

internal inline fun execInsertOrUpdate(entity: Any, propsToUpdate: PropsToUpdate?,
                                       con: Connection?, updateAutoGenValues: Boolean,
                                       statement: String, paramPlaceholder: String,
                                       buildParamsClause: (Any, StringBuilder, ArrayList<Any?>) -> Unit): Int
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

    return Query(sb.toString(), bindParams).execAndReadResults(entity, con, autoGenColumns)
}

private fun processProp(entity: Any, p: KProperty<*>, bindParams: ArrayList<Any?>,
                        sb: StringBuilder, paramPlaceholder: String,
                        updateAutoGenValues: Boolean, autoGenColumns: RefreshMap) {
    val colName = colName(p)
    if (!p.hasAnnotation<SqlIgnore>() && !addToRefreshListIfAutoGen(p, updateAutoGenValues, autoGenColumns, colName)) {
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

internal fun appendCol(sb: StringBuilder, colName: String, paramPlaceholder: String) {
    sb.append(colName)
    sb.append(paramPlaceholder)
    sb.append(',')
}

internal fun buildWhereWithId(entity: Any, sb: StringBuilder, bindParams:ArrayList<Any?>) {
    val id = getIdProperty(entity.javaClass.kotlin)
    sb.append(" WHERE ")
    sb.append(colName(id))
    sb.append(" = ?")
    addPropToBindParams(entity, id, bindParams)
}

internal fun addPropToBindParams(entity: Any, p: KProperty<*>, bindParams: MutableList<Any?>) {
    @Suppress("UNCHECKED_CAST")
    val value = when (p) {
        is KProperty0<*> -> p.get() // property obtained via myObject::myProp (receiver object is already bound).
        is KProperty1<*, *> -> (p as KProperty1<Any, *>).get(entity) // property from myObject.javaClass.kotlin.memberProperties.
        else -> throw SqlPalException("Property '${p.name}' of '${entity::class.qualifiedName}' class " +
                "has more than one receiver. Such properties " +
                "(as an extension property declared in a class) are not supported.")
    }
    addValueToBindParams(value, entity::class, p, bindParams)
}

internal fun addValueToBindParams(value: Any?, classType: KClass<*>, p: KProperty<*>, bindParams: MutableList<Any?>) {
    val paramValue = if (value is List<*>) {
        // Wrap List with object that also contains information about generic type of the List.
        // It's necessary to handle empty Lists, because unlike Array, empty List does not contain
        // information about its generic type, what makes impossible to map it to appropriate SQL type.
        val componentType = p.returnType.arguments[0].type?.classifier as? KClass<*>
            ?: throw SqlPalException("Can't determine generic type of List for " +
                    "property ${p.name} of ${classType.qualifiedName} class to map it to SQL type. " +
                    "Only Lists of primitive types are supported " +
                    "and generic type must be specified explicitly, not List<*>.")
        ListAndType(value, componentType)
    } else
        value
    bindParams.add(paramValue)
}

@PublishedApi
internal fun <T: Any> getConstructor(type: KClass<T>): KFunction<T> {
    val error = "Class must have primary constructor where are declared all properties that should be read from database."
    val constr = type.primaryConstructor ?: throw SqlPalException(error)
    if (constr.parameters.isEmpty()) throw SqlPalException(error)
    return constr
}

@PublishedApi
internal fun <T: Any> getIdProperty(type: KClass<T>) = type.memberProperties.find { it.hasAnnotation<Id>() }
    ?: throw SqlPalException("Unable to generate WHERE clause with ID condition for ${type.qualifiedName} class, " +
            "as it does not have property annotated with @Id.")

@PublishedApi
internal fun entityName(type: KClass<*>) = customName(type) ?: toDbCase(type.simpleName!!)
@PublishedApi
internal fun colName(prop: KProperty<*>) = customName(prop) ?: toDbCase(prop.name)

// Until version 2.2 Kotlin did not support applying single annotation on both constructor parameter and property.
// Thus, to check that parameter is annotated we need to check property with the same name.
// So added this method to get custom name for all parameters at once.
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

internal fun customName(type: KAnnotatedElement) = type.findAnnotation<SqlName>()?.name

internal fun toDbCase(name: String) = if (SqlPal.convertNamesToSnakeCase) camel2Snake(name) else name

/** Converts String from camelCase to snake_case. */
private fun camel2Snake(name: String): String {
    val sb = StringBuilder()
    for (i in name.indices) {
        if (i > 0 && name[i].isUpperCase())
            sb.append('_')
        sb.append(name[i].lowercaseChar())
    }
    return sb.toString()
}

/** Converts String from snake_case to camelCase.*/
private fun snake2Camel(name: String): String {
    var i = 0
    val sb = StringBuilder()
    while (i < name.length) {
        if (name[i] != '_') sb.append(name[i])
        else if (i++ < name.length) sb.append(name[i].uppercase())
        i++
    }
    return sb.toString()
}

@Suppress("UNCHECKED_CAST")
internal fun String.toEnum(enumType: KType) =
    java.lang.Enum.valueOf(enumType.jvmErasure.java as Class<out Enum<*>>, this)

internal val KType.isEnum get() = kClass?.java?.isEnum == true

internal val KType.kClass get() = classifier as? KClass<*>
