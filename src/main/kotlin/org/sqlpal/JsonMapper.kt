package org.sqlpal

import java.math.BigDecimal
import java.time.*
import kotlin.reflect.KClass
import kotlin.reflect.KType

// Provides serialization of Array or Collection to JSON and parsing from it.
internal class JsonMapper(
    private val json: String,
    private val colIndex: Int,
    private val componentType: KType)
{
    companion object {
        fun serialize(isTypedArray: Boolean, iterator: Iterator<*>, componentType: KClass<*>): String {
            val sb = StringBuilder("[")

            if (isTypedArray)
                // Typed array (e.g. ByteArray, IntArray) can't contain nulls, so don't check for null to speed up.
                iterator.forEach { sb.append(it).append(',') }
            else if (componentType.isQuotedInJson)
                iterator.forEach {
                    if (it == null) sb.append("null")
                    else sb.append("\"").append(it).append("\"")
                    sb.append(',')
                }
            else
                iterator.forEach { sb.append(it ?: "null").append(',') }

            if (sb.length > 1) sb.deleteCharAt(sb.length - 1) // Remove trailing comma
            sb.append(']')
            return sb.toString()
        }
    }

    private var index: Int = 0

    fun parse(): List<*> {
        val list = mutableListOf<Any?>()

        skipWhitespace()
        if (json[index++] != '[') throwJsonParseError(index - 1)
        skipWhitespace()
        if (json[index] == ']') return list

        val extractItem = if (componentType.isQuotedInJson) ::extractQuotedItem else ::extractUnquotedItem
        val parse = getParser()

        while (true) {
            val value = if (parseNull()) null else parse(extractItem())
            list.add(value)
            skipWhitespace()
            if (json[index] == ']') break
            if (json[index++] != ',') throwJsonParseError(index - 1)
            skipWhitespace()
        }
        return list
    }

    private fun getParser(): (String) -> Any = when (componentType.classifier) {
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
        else -> if (componentType.isEnum) { c -> c.toEnum(componentType) }
        else throw SqlPalException("Parsing from JSON for type $componentType is not implemented.")
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