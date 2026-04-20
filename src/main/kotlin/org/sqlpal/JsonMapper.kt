package org.sqlpal

import java.math.BigDecimal
import java.time.*
import kotlin.reflect.KClass
import kotlin.reflect.KType

// Provides serialization of Array or Collection to JSON and parsing from it.
internal class JsonMapper(
    private val colIndex: Int,
    componentType: KType,
    keyComponentType: KType? = null) // Is used for parsing of Map content.
{
    private lateinit var json: String
    private var index: Int = 0

    private val extractItem: () -> String
    private val parseValue: (String) -> Any
    private lateinit var parseKey: (String) -> Any

    init {
        extractItem = if (componentType.isQuotedInJson) ::extractQuotedItem else ::extractUnquotedItem
        parseValue = getParser(componentType)
        if (keyComponentType != null) parseKey = getParser(keyComponentType)
    }

    companion object {
        fun serialize(isTypedArray: Boolean, iterator: Iterator<*>, componentType: KClass<*>): String {
            val sb = StringBuilder("[")

            if (isTypedArray)
                // Typed array (e.g. ByteArray, IntArray) can't contain nulls, so don't check for null to speed up.
                iterator.forEach { sb.append(it).append(',') }
            else if (componentType.isQuotedInJson)
                iterator.forEach {
                    if (it == null) sb.append("null")
                    else sb.append('"').append(it).append('"')
                    sb.append(',')
                }
            else
                iterator.forEach { sb.append(it ?: "null").append(',') }

            if (sb.length > 1) sb.deleteCharAt(sb.length - 1) // Remove trailing comma
            sb.append(']')
            return sb.toString()
        }

        fun serialize(map: Map<*, *>, componentType: KClass<*>): String {
            val needQuotes = componentType.isQuotedInJson // Moved out of loop to speed up.
            val sb = StringBuilder("{")
            map.forEach { (key, value) ->
                sb.append('"').append(key ?: "null").append('"') // Key is always quoted according to JSON format.
                sb.append(':')
                if (value == null) sb.append("null")
                else if (needQuotes) sb.append('"').append(value).append('"')
                else sb.append(value)
                sb.append(',')
            }
            if (sb.length > 1) sb.deleteCharAt(sb.length - 1) // Remove trailing comma
            sb.append('}')
            return sb.toString()
        }
    }

    fun parseMap(jsonString: String?): Map<*, *>? {
        json = jsonString ?: return null

        val map = mutableMapOf<Any?, Any?>()
        if (!parseStart('{', '}')) return map

        while (true) {
            val keyStr = extractQuotedItem()
            val key = if (keyStr == "null") null else parseKey(keyStr)
            skipWhitespace()
            if (json[index++] != ':') throwJsonParseError(index - 1)
            skipWhitespace()

            val value = if (parseNull()) null else parseValue(extractItem())
            map[key] = value
            if (!parseDelimiter('}')) break
        }
        return map
    }

    fun parseList(jsonString: String?): List<*>? {
        json = jsonString ?: return null

        val list = mutableListOf<Any?>()
        if (!parseStart('[', ']')) return list

        while (true) {
            val value = if (parseNull()) null else parseValue(extractItem())
            list.add(value)
            if (!parseDelimiter(']')) break
        }
        return list
    }

    private fun parseStart(opening: Char, closing: Char): Boolean {
        index = 0
        skipWhitespace()
        if (json[index++] != opening) throwJsonParseError(index - 1)
        skipWhitespace()
        return json[index] != closing
    }

    private fun parseDelimiter(closing: Char): Boolean {
        skipWhitespace()
        if (json[index] == closing) return false
        if (json[index++] != ',') throwJsonParseError(index - 1)
        skipWhitespace()
        return true
    }

    private fun getParser(type: KType): (String) -> Any = when (type.classifier) {
        String::class -> { c -> c }

        Integer::class -> Integer::parseInt
        Long::class -> java.lang.Long::parseLong
        Byte::class -> java.lang.Byte::parseByte
        Short::class -> java.lang.Short::parseShort

        Float::class -> java.lang.Float::parseFloat
        Double::class -> java.lang.Double::parseDouble

        Boolean::class -> java.lang.Boolean::parseBoolean
        BigDecimal::class -> { c -> c.toBigDecimal() }

        LocalDate::class -> LocalDate::parse
        LocalTime::class -> LocalTime::parse
        LocalDateTime::class -> LocalDateTime::parse
        OffsetTime::class -> OffsetTime::parse
        OffsetDateTime::class -> OffsetDateTime::parse
        ZonedDateTime::class -> ZonedDateTime::parse
        Instant::class -> Instant::parse
        else -> if (type.isEnum) { c -> c.toEnum(type) }
        else throw SqlPalException("Parsing from JSON for type $type is not implemented.")
    }

    private fun parseNull() = if (json.length > index + 3 &&
        json[index] == 'n' && json[index + 1] == 'u' && json[index + 2] == 'l' && json[index + 3] == 'l')
    {
        index += 4; true
    } else
        false

    private fun extractQuotedItem(): String {
        if (json[index] != '"') throwJsonParseError(index)

        val startIndex = index + 1 // Move after opening quote.
        do {
            index++
            index = json.indexOf('"', index)
            if (index < 0) throwJsonParseError( startIndex)
        } while (json[index - 1] == '\\') // If it's escaped quote, the search further.

        index++ // Move index to next char after closing quote.
        return json.substring(startIndex, index - 1)
    }

    private fun extractUnquotedItem(): String {
        val startIndex = index

        index = json.indexOf(',', index)
        if (index < 0) throwJsonParseError(startIndex)
        while (json[--index].isWhitespace()) Unit

        index++ // Move index to next char after item.
        return json.substring(startIndex, index)
    }

    private fun skipWhitespace() {
        while (true) {
            if (index >= json.length) throwJsonParseError(index)
            if (!json[index].isWhitespace()) return
            index++
        }
    }

    private fun throwJsonParseError(position: Int): Nothing = throw SqlPalException(
        "Incorrect format of JSON array at position $position in column at index $colIndex. " +
                "Unable to convert JSON string to List or Array.")
}

private val KType.isQuotedInJson get() = kClass?.isQuotedInJson == true
private val KClass<*>.isQuotedInJson get() = when (this) {
    String::class,
    LocalDate::class, LocalTime::class, LocalDateTime::class,
    OffsetTime::class, OffsetDateTime::class, ZonedDateTime::class,
    Instant::class -> true
    else -> java.isEnum
}