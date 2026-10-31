package org.sqlpal

import java.sql.Connection

/** Runs specified block, providing connection from the pool to it.
 * Connection is committed after the block is executed or rolled back on exception.
 * @param block to execute within transaction. */
inline fun <T> transaction(block: (Connection) -> T) = SqlPal.withConnection { transaction(it, block) }

/** Runs specified block, providing specified [connection] to it, and
 * - if [connection].autoCommit is true, then sets it to false, executes the block,
 * sets it back to true, and commits the connection or rolls it back on exception;
 * - if [connection].autoCommit is false, then assumes that it's nested transaction,
 * so sets savepoint before the execution and releases or rolls it back after.
 * Connection is not committed in this case.
 * @param connection to pass to the [block] and releaseSavepoint/commit or rollback after it.
 * @param block to execute within transaction. */
inline fun <T> transaction(connection: Connection, block: (Connection) -> T): T {
    val autoCommit = connection.autoCommit
    val sp = if (autoCommit) null else connection.setSavepoint()
    try {
        connection.autoCommit = false
        val result = block(connection)
        if (sp != null) connection.releaseSavepoint(sp) else connection.commit()
        return result
    }
    catch (ex: Throwable) { // Catch Throwable (not Exception) as otherwise on an Error the rollback would be skipped
        if (sp != null) connection.rollback(sp) else connection.rollback()
        throw ex
    }
    finally {
        connection.autoCommit = autoCommit
    }
}