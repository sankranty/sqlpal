package org.sqlpal

import java.sql.Connection


/** Runs specified block, providing connection, that is committed after block is executed or rolled back on exception.
 * @param block to execute within transaction. */
inline fun <T> transaction(block: (Connection) -> T) = SqlPal.withConnection { transaction(it, block) }

/** Runs specified block and commit provided connection after block is executed or roll it back on exception.
 * @param connection that is committed or rolled back after execution.
 * @param block to execute within transaction. */
inline fun <T> transaction(connection: Connection, block: (Connection) -> T): T {
    try {
        connection.autoCommit = false
        val result = block(connection)
        connection.commit()
        return result
    }
    catch (ex: Exception) {
        connection.rollback()
        throw ex
    }
}