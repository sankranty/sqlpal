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

    // For unquoted items we need also to check in case of array for ']' and for '}' in case of map,
    // as there is no ',' after the last item.
    private val unquotedItemDelimiters: CharArray

    init {
        extractItem = if (componentType.isQuotedInJson) ::extractQuotedItem else ::extractUnquotedItem
        parseValue = getParser(componentType)
        if (keyComponentType == null)
            unquotedItemDelimiters = charArrayOf(',', ']')
        else {
            unquotedItemDelimiters = charArrayOf(',', '}')
            parseKey = getParser(keyComponentType)
        }
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
                    else {
                        sb.append('"')
                        if (it is String) escapeAndAppend(it, sb) else sb.append(it)
                        sb.append('"')
                    }
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
                // Key is always quoted according to JSON format.
                sb.append('"')
                escapeAndAppend((key ?: "null").toString(), sb)
                sb.append('"')
                sb.append(':')

                if (value == null) sb.append("null")
                else if (!needQuotes) sb.append(value)
                else {
                    sb.append('"')
                    if (value is String) escapeAndAppend(value, sb) else sb.append(value)
                    sb.append('"')
                }
                sb.append(',')
            }
            if (sb.length > 1) sb.deleteCharAt(sb.length - 1) // Remove trailing comma
            sb.append('}')
            return sb.toString()
        }

        private fun escapeAndAppend(str: String, sb: StringBuilder) {
            for (c in str) when (c) {
                '\\' -> sb.append("\\\\")
                '"' -> sb.append("\\\"")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                '\b' -> sb.append("\\b")
                '\u000C' -> sb.append("\\f")
                else -> if (c.code < 0x20)
                    sb.append("\\u").append(c.code.toString(16).padStart(4, '0'))
                else
                    sb.append(c)
            }
        }
    }

    fun parseList(jsonString: String?) = parse(jsonString, '[', ']',
        { mutableListOf<Any?>() },
        { list, value, _ -> list.add(value) }
    )

    fun parseSet(jsonString: String?) = parse(jsonString, '[', ']',
        { mutableSetOf<Any?>() },
        { set, value, _ -> set.add(value) }
    )

    fun parseMap(jsonString: String?) = parse(jsonString, '{', '}',
        { mutableMapOf<Any?, Any?>() },
        { map, value, key -> map[key] = value },
        {
            val keyStr = extractQuotedItem()
            val key = if (keyStr == "null") null else parseKey(keyStr)
            skipWhitespace()
            if (json[index++] != ':') throwJsonParseError(index - 1)
            skipWhitespace()
            key
        }
    )

    private inline fun <T> parse(jsonString: String?, opening: Char, closing: Char,
                                 createContainer: () -> T,
                                 addValue: (T, Any?, Any?) -> Unit,
                                 getKey: () -> Any? = { null }
    ): T? {
        json = jsonString ?: return null

        val container = createContainer()
        if (!parseStart(opening, closing)) return container

        while (true) {
            val key = getKey()
            val value = if (parseNull()) null else parseValue(extractItem())
            addValue(container, value, key)
            if (!parseDelimiter(closing)) break
        }
        return container
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

        Int::class -> Integer::parseInt
        Long::class -> java.lang.Long::parseLong
        Byte::class -> java.lang.Byte::parseByte
        Short::class -> java.lang.Short::parseShort

        UInt::class -> { c -> c.toUInt() }
        ULong::class -> { c -> c.toULong() }
        UByte::class -> { c -> c.toUByte() }
        UShort::class -> { c -> c.toUShort() }

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
        index++ // skip opening quote
        val sb = StringBuilder()

        while (true) {
            if (index >= json.length) throwJsonParseError(index)
            when (val c = json[index++]) {
                '"' -> return sb.toString() // closing quote
                '\\' -> {
                    if (index >= json.length) throwJsonParseError(index)
                    when (val esc = json[index++]) {
                        '"', '\\', '/' -> sb.append(esc)
                        'n' -> sb.append('\n')
                        'r' -> sb.append('\r')
                        't' -> sb.append('\t')
                        'b' -> sb.append('\b')
                        'f' -> sb.append('\u000C')
                        'u' -> {
                            if (index + 4 > json.length) throwJsonParseError(index - 2)
                            val hex = json.substring(index, index + 4)
                            val code = hex.toIntOrNull(16) ?: throwJsonParseError(index - 2)
                            sb.append(code.toChar())
                            index += 4
                        }
                        else -> throwJsonParseError(index - 1)
                    }
                }
                else -> sb.append(c)
            }
        }
    }

    private fun extractUnquotedItem(): String {
        val startIndex = index

        index = json.indexOfAny(unquotedItemDelimiters, index)
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
                "Unable to convert JSON string to Collection or Array.")
}

private val KType.isQuotedInJson get() = kClass?.isQuotedInJson == true
private val KClass<*>.isQuotedInJson get() = when (this) {
    String::class,
    LocalDate::class, LocalTime::class, LocalDateTime::class,
    OffsetTime::class, OffsetDateTime::class, ZonedDateTime::class,
    Instant::class -> true
    else -> java.isEnum
}