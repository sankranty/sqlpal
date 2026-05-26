package org.sqlpal

/** Is produced by [includes], [includesIgnoreCase], [beginsWith], [beginsWithIgnoreCase],
 * [finishesWith], [finishesWithIgnoreCase] extension functions to be passed as a parameter to [Sql] string. */
class Like (
    private val columnName: String,
    private val ignoreCase: Boolean,
    private val openingWildcard: Char?,
    private val text: String,
    private val closingWildcard: Char?,
) {
    private val escapeChar = '!'
    private val processChar: (Char) -> Char = if (ignoreCase) { c -> c.lowercaseChar() } else { c -> c }

    internal fun appendLikeCondition(sb: StringBuilder) {
        if (ignoreCase) sb.append("lower(")
        sb.append(columnName)
        if (ignoreCase) sb.append(")")
        sb.append(" like ? escape '")
        sb.append(escapeChar)
        sb.append("' ")
    }

    internal fun getLikePattern(): String {
        val sb = StringBuilder()
        if (openingWildcard != null) sb.append(openingWildcard)
        for (c in text) {
            when (c) {
                escapeChar -> sb.append(escapeChar).append(escapeChar)
                '%', '_'   -> sb.append(escapeChar).append(c)
                else       -> sb.append(processChar(c))
            }
        }
        if (closingWildcard != null) sb.append(closingWildcard)
        return sb.toString()
    }
}

/** Generates LIKE condition that checks that the column contains [text].
 * Should be called on a string that represents column name for which LIKE condition needs to be specified.
 * ATTENTION! Case-sensitivity of LIKE varies across RDBMS.
 * For guaranteed case-insensitive comparison use [includesIgnoreCase].
 * For case-sensitive comparison check documentation of target RDBMS.
 * @param text string that will be wrapped with %. % and _ wildcards will be escaped.
 * @return [Like] object to be passed as a parameter to [Sql] string. */
infix fun String.includes(text: String) =
    Like(this, false, '%', text, '%')

/** Generates LIKE condition that checks that the column begins with [text].
 * Should be called on a string that represents column name for which LIKE condition needs to be specified.
 * ATTENTION! Case-sensitivity of LIKE varies across RDBMS.
 * For guaranteed case-insensitive comparison use [beginsWithIgnoreCase].
 * For case-sensitive comparison check documentation of target RDBMS.
 * @param text string that will be appended with %. % and _ wildcards will be escaped.
 * @return [Like] object to be passed as a parameter to [Sql] string. */
infix fun String.beginsWith(text: String) =
    Like(this, false, null, text, '%')

/** Generates LIKE condition that checks that the column finishes with [text].
 * Should be called on a string that represents column name for which LIKE condition needs to be specified.
 * ATTENTION! Case-sensitivity of LIKE varies across RDBMS.
 * For guaranteed case-insensitive comparison use [finishesWithIgnoreCase].
 * For case-sensitive comparison check documentation of target RDBMS.
 * @param text string that will be preceded by %. % and _ wildcards will be escaped.
 * @return [Like] object to be passed as a parameter to [Sql] string. */
infix fun String.finishesWith(text: String) =
    Like(this, false, '%', text, null)

/** Generates case-insensitive LIKE condition that checks that the column contains [text].
 * Both column and [text] are cast to lowercase to guarantee case-insensitivity across all RDBMS.
 * Should be called on a string that represents column name for which LIKE condition needs to be specified.
 * @param text string that will be cast to lowercase and wrapped with %. % and _ wildcards will be escaped.
 * @return [Like] object to be passed as a parameter to [Sql] string. */
infix fun String.includesIgnoreCase(text: String) =
    Like(this, true, '%', text, '%')

/** Generates case-insensitive LIKE condition that checks that the column begins with [text].
 * Both column and [text] are cast to lowercase to guarantee case-insensitivity across all RDBMS.
 * Should be called on a string that represents column name for which LIKE condition needs to be specified.
 * @param text string that will be cast to lowercase and appended with %. % and _ wildcards will be escaped.
 * @return [Like] object to be passed as a parameter to [Sql] string. */
infix fun String.beginsWithIgnoreCase(text: String) =
    Like(this, true, null, text, '%')

/** Generates case-insensitive LIKE condition that checks that the column finishes with [text].
 * Both column and [text] are cast to lowercase to guarantee case-insensitivity across all RDBMS.
 * Should be called on a string that represents column name for which LIKE condition needs to be specified.
 * @param text string that will be cast to lowercase and preceded by %. % and _ wildcards will be escaped.
 * @return [Like] object to be passed as a parameter to [Sql] string. */
infix fun String.finishesWithIgnoreCase(text: String) =
    Like(this, true, '%', text, null)