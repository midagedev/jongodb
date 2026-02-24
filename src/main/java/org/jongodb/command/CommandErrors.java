package org.jongodb.command;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;

/**
 * Catalog of wire-compatible command errors used by command handlers.
 */
final class CommandErrors {
    private static final int CODE_COMMAND_NOT_FOUND = 59;
    private static final int CODE_INVALID_ARGUMENT = 14;
    private static final int CODE_NOT_IMPLEMENTED = 238;
    private static final int CODE_CURSOR_NOT_FOUND = 43;
    private static final int CODE_NO_SUCH_TRANSACTION = 251;
    private static final int CODE_TRANSACTION_COMMITTED = 256;
    private static final int CODE_WRITE_CONFLICT = 112;
    private static final int CODE_DUPLICATE_KEY = 11000;

    private CommandErrors() {}

    static BsonDocument commandNotFound(final String commandName) {
        return error("no such command: " + commandName, CODE_COMMAND_NOT_FOUND, "CommandNotFound");
    }

    static BsonDocument badValue(final String message) {
        return error(message, CODE_INVALID_ARGUMENT, "BadValue");
    }

    static BsonDocument typeMismatch(final String message) {
        return error(message, CODE_INVALID_ARGUMENT, "TypeMismatch");
    }

    static BsonDocument notImplemented(final String message) {
        return errorWithLabels(message, CODE_NOT_IMPLEMENTED, "NotImplemented", List.of("UnsupportedFeature"));
    }

    static BsonDocument noSuchTransaction(final String commandName) {
        return error(commandName + " requires an active transaction", CODE_NO_SUCH_TRANSACTION, "NoSuchTransaction");
    }

    static BsonDocument noSuchTransactionWithTransientLabel(final String commandName) {
        return errorWithLabels(
                commandName + " requires an active transaction",
                CODE_NO_SUCH_TRANSACTION,
                "NoSuchTransaction",
                List.of("TransientTransactionError"));
    }

    static BsonDocument noSuchTransactionWithUnknownCommitResultLabel(final String commandName) {
        return errorWithLabels(
                commandName + " requires an active transaction",
                CODE_NO_SUCH_TRANSACTION,
                "NoSuchTransaction",
                List.of("UnknownTransactionCommitResult"));
    }

    static BsonDocument transactionCommitted(final String message) {
        return error(message, CODE_TRANSACTION_COMMITTED, "TransactionCommitted");
    }

    static BsonDocument writeConflict(final String message) {
        return error(message, CODE_WRITE_CONFLICT, "WriteConflict");
    }

    static BsonDocument duplicateKey(final String message) {
        return error(message, CODE_DUPLICATE_KEY, "DuplicateKey");
    }

    static BsonDocument cursorNotFound(final long cursorId) {
        return error("cursor not found: " + cursorId, CODE_CURSOR_NOT_FOUND, "CursorNotFound");
    }

    private static BsonDocument error(final String message, final int code, final String codeName) {
        return new BsonDocument()
                .append("ok", new BsonDouble(0.0))
                .append("errmsg", new BsonString(message))
                .append("code", new BsonInt32(code))
                .append("codeName", new BsonString(codeName));
    }

    private static BsonDocument errorWithLabels(
            final String message, final int code, final String codeName, final List<String> labels) {
        final BsonArray errorLabels = new BsonArray();
        for (final String label : labels) {
            errorLabels.add(new BsonString(label));
        }
        return error(message, code, codeName).append("errorLabels", errorLabels);
    }
}
