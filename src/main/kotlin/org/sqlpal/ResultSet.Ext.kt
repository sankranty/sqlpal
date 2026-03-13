package org.sqlpal

import java.math.BigDecimal
import java.sql.ResultSet
import java.time.*

// Public ResultSet extension methods

inline fun <T> ResultSet.valueOrNull(getValue: () -> T): T? {
    val value: T = getValue()
    return if (wasNull()) null else value
}

// Methods that return value for NULL (should be used only for NOT NULL columns)
infix fun ResultSet.intVal(str: String) = getInt(str)
infix fun ResultSet.longVal(str: String) = getLong(str)
infix fun ResultSet.byteVal(str: String) = getByte(str)
infix fun ResultSet.shortVal(str: String) = getShort(str)
infix fun ResultSet.doubleVal(str: String) = getDouble(str)
infix fun ResultSet.floatVal(str: String) = getFloat(str)
infix fun ResultSet.boolVal(str: String) = getBoolean(str)

// Methods that return null for NULL
infix fun ResultSet.int(str: String) = valueOrNull { this intVal str }
infix fun ResultSet.long(str: String) = valueOrNull { this longVal str }
infix fun ResultSet.byte(str: String) = valueOrNull { this byteVal str }
infix fun ResultSet.short(str: String) = valueOrNull { this shortVal str }
infix fun ResultSet.double(str: String) = valueOrNull { this doubleVal str }
infix fun ResultSet.float(str: String) = valueOrNull { this floatVal str }
infix fun ResultSet.bool(str: String) = valueOrNull { this boolVal str }

infix fun ResultSet.str(str: String): String? = getString(str)
infix fun ResultSet.dec(str: String): BigDecimal? = getBigDecimal(str)
infix fun ResultSet.date(str: String): LocalDate? = getObject(str, LocalDate::class.java)
infix fun ResultSet.time(str: String): LocalTime? = getObject(str, LocalTime::class.java)
infix fun ResultSet.dt(str: String): LocalDateTime? = getObject(str, LocalDateTime::class.java)
infix fun ResultSet.dto(str: String): OffsetDateTime? = getObject(str, OffsetDateTime::class.java)
infix fun ResultSet.dtz(str: String): ZonedDateTime? = getObject(str, OffsetDateTime::class.java)?.toZonedDateTime()

// Methods for enum type
inline infix fun <reified T: Enum<T>> ResultSet.enum(str: String): T? = getString(str)?.let { enumValueOf<T>(it) }
inline infix fun <reified T: Enum<T>> ResultSet.enumVal(str: String): T = enumValueOf(getString(str))