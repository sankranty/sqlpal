package org.sqlpal

import java.math.BigDecimal
import java.sql.ResultSet
import java.time.*

// Public ResultSet extension methods

/** Returns value provided by [getValue], or null if actual values is null.
 * Is used for nullable columns as JDBC methods does not return null for not-nullable Java types. */
inline fun <T> ResultSet.valueOrNull(getValue: () -> T): T? {
    val value: T = getValue()
    return if (wasNull()) null else value
}

// Methods that return value for NULL (should be used only for NOT NULL columns)

/** Reads Int value from NOT NULL column with specified name. */
infix fun ResultSet.intVal(colName: String) = getInt(colName)

/** Reads Long value from NOT NULL column with specified name. */
infix fun ResultSet.longVal(colName: String) = getLong(colName)

/** Reads Byte value from NOT NULL column with specified name. */
infix fun ResultSet.byteVal(colName: String) = getByte(colName)

/** Reads Short value from NOT NULL column with specified name. */
infix fun ResultSet.shortVal(colName: String) = getShort(colName)

/** Reads Double value from NOT NULL column with specified name. */
infix fun ResultSet.doubleVal(colName: String) = getDouble(colName)

/** Reads Float value from NOT NULL column with specified name. */
infix fun ResultSet.floatVal(colName: String) = getFloat(colName)

/** Reads Boolean value from NOT NULL column with specified name. */
infix fun ResultSet.boolVal(colName: String) = getBoolean(colName)

// Methods that return null for NULL

/** Reads Int value from nullable column with specified name. */
infix fun ResultSet.int(colName: String) = valueOrNull { this intVal colName }

/** Reads Long value from nullable column with specified name. */
infix fun ResultSet.long(colName: String) = valueOrNull { this longVal colName }

/** Reads Byte value from nullable column with specified name. */
infix fun ResultSet.byte(colName: String) = valueOrNull { this byteVal colName }

/** Reads Short value from nullable column with specified name. */
infix fun ResultSet.short(colName: String) = valueOrNull { this shortVal colName }

/** Reads Double value from nullable column with specified name. */
infix fun ResultSet.double(colName: String) = valueOrNull { this doubleVal colName }

/** Reads Float value from nullable column with specified name. */
infix fun ResultSet.float(colName: String) = valueOrNull { this floatVal colName }

/** Reads Boolean value from nullable column with specified name. */
infix fun ResultSet.bool(colName: String) = valueOrNull { this boolVal colName }


/** Reads String value from column with specified name. */
infix fun ResultSet.str(colName: String): String? = getString(colName)

/** Reads BigDecimal value from column with specified name. */
infix fun ResultSet.dec(colName: String): BigDecimal? = getBigDecimal(colName)

/** Reads LocalDate value from column with specified name. */
infix fun ResultSet.date(colName: String): LocalDate? = getObject(colName, LocalDate::class.java)

/** Reads LocalTime value from column with specified name. */
infix fun ResultSet.time(colName: String): LocalTime? = getObject(colName, LocalTime::class.java)

/** Reads LocalTime value from column with specified name. */
infix fun ResultSet.otime(colName: String): OffsetTime? = getObject(colName, OffsetTime::class.java)

/** Reads LocalDateTime value from column with specified name. */
infix fun ResultSet.dt(colName: String): LocalDateTime? = getObject(colName, LocalDateTime::class.java)

/** Reads OffsetDateTime value from column with specified name. */
infix fun ResultSet.odt(colName: String): OffsetDateTime? = getObject(colName, OffsetDateTime::class.java)

/** Reads ZonedDateTime value from column with specified name. */
infix fun ResultSet.zdt(colName: String): ZonedDateTime? = getObject(colName, OffsetDateTime::class.java)?.toZonedDateTime()

// Methods for enum type

/** Reads enum value from nullable column with specified name. */
inline infix fun <reified T: Enum<T>> ResultSet.enum(colName: String): T? = getString(colName)?.let { enumValueOf<T>(it) }

/** Reads enum value from NOT NUL column with specified name. */
inline infix fun <reified T: Enum<T>> ResultSet.enumVal(colName: String): T = enumValueOf(getString(colName))