package org.jongodb.txn;

import java.util.Objects;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Validates transaction command fields and applies minimal session transaction state transitions.
 */
public final class TransactionCommandValidator {
    private static final int CODE_INVALID_ARGUMENT = 14;
    private static final int CODE_NO_SUCH_TRANSACTION = 251;
    private static final Set<String> TRANSACTIONAL_COMMANDS =
            Set.of("insert", "update", "delete", "find", "committransaction", "aborttransaction");

    private final SessionTransactionPool sessionPool;

    public TransactionCommandValidator(final SessionTransactionPool sessionPool) {
        this.sessionPool = Objects.requireNonNull(sessionPool, "sessionPool");
    }

    public BsonDocument validateAndApply(final String commandName, final BsonDocument command) {
        if (!TRANSACTIONAL_COMMANDS.contains(commandName)) {
            return null;
        }

        final boolean commitOrAbort = isCommitOrAbort(commandName);
        final boolean hasTransactionFields = hasTransactionFields(command);
        if (!commitOrAbort && !hasTransactionFields) {
            return null;
        }

        final ParsedFields parsedFields = parseFields(commandName, command, commitOrAbort);
        if (parsedFields.error() != null) {
            return parsedFields.error();
        }

        if (parsedFields.startTransaction()) {
            if (sessionPool.hasActiveTransaction(parsedFields.sessionId())) {
                return error("transaction already in progress for this session", CODE_INVALID_ARGUMENT, "BadValue");
            }
            sessionPool.startTransaction(parsedFields.sessionId(), parsedFields.txnNumber());
            return null;
        }

        if (!sessionPool.hasActiveTransaction(parsedFields.sessionId(), parsedFields.txnNumber())) {
            return error(
                    commandName + " requires an active transaction",
                    CODE_NO_SUCH_TRANSACTION,
                    "NoSuchTransaction");
        }

        if (commitOrAbort) {
            sessionPool.clearTransaction(parsedFields.sessionId(), parsedFields.txnNumber());
        }
        return null;
    }

    private static ParsedFields parseFields(
            final String commandName, final BsonDocument command, final boolean commitOrAbort) {
        final BsonValue lsidValue = command.get("lsid");
        if (lsidValue == null || !lsidValue.isDocument()) {
            return ParsedFields.error(error("lsid must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch"));
        }
        final String sessionId = lsidValue.asDocument().toJson();

        final ParsedTxnNumber parsedTxnNumber = parseTxnNumber(command.get("txnNumber"));
        if (parsedTxnNumber.error() != null) {
            return ParsedFields.error(parsedTxnNumber.error());
        }

        final BsonValue autocommitValue = command.get("autocommit");
        if (autocommitValue == null || !autocommitValue.isBoolean()) {
            return ParsedFields.error(error("autocommit must be a boolean", CODE_INVALID_ARGUMENT, "TypeMismatch"));
        }
        if (autocommitValue.asBoolean().getValue()) {
            return ParsedFields.error(error("autocommit must be false", CODE_INVALID_ARGUMENT, "BadValue"));
        }

        final BsonValue startTransactionValue = command.get("startTransaction");
        boolean startTransaction = false;
        if (startTransactionValue != null) {
            if (commitOrAbort) {
                return ParsedFields.error(error(
                        "startTransaction is not allowed for " + commandName, CODE_INVALID_ARGUMENT, "BadValue"));
            }
            if (!startTransactionValue.isBoolean()) {
                return ParsedFields.error(
                        error("startTransaction must be a boolean", CODE_INVALID_ARGUMENT, "TypeMismatch"));
            }
            if (!startTransactionValue.asBoolean().getValue()) {
                return ParsedFields.error(
                        error("startTransaction must be true when provided", CODE_INVALID_ARGUMENT, "BadValue"));
            }
            startTransaction = true;
        }

        return new ParsedFields(sessionId, parsedTxnNumber.value(), startTransaction, null);
    }

    private static ParsedTxnNumber parseTxnNumber(final BsonValue txnNumberValue) {
        if (txnNumberValue == null) {
            return ParsedTxnNumber.error(error("txnNumber must be an integer", CODE_INVALID_ARGUMENT, "TypeMismatch"));
        }
        if (!isNumericType(txnNumberValue)) {
            return ParsedTxnNumber.error(error("txnNumber must be an integer", CODE_INVALID_ARGUMENT, "TypeMismatch"));
        }

        final Long parsedValue = readIntegralLong(txnNumberValue);
        if (parsedValue == null) {
            return ParsedTxnNumber.error(error("txnNumber must be an integer", CODE_INVALID_ARGUMENT, "TypeMismatch"));
        }
        if (parsedValue < 0) {
            return ParsedTxnNumber.error(
                    error("txnNumber must be a non-negative integer", CODE_INVALID_ARGUMENT, "BadValue"));
        }
        return new ParsedTxnNumber(parsedValue, null);
    }

    private static boolean hasTransactionFields(final BsonDocument command) {
        return command.containsKey("lsid")
                || command.containsKey("txnNumber")
                || command.containsKey("autocommit")
                || command.containsKey("startTransaction");
    }

    private static boolean isCommitOrAbort(final String commandName) {
        return "committransaction".equals(commandName) || "aborttransaction".equals(commandName);
    }

    private static boolean isNumericType(final BsonValue value) {
        return value.isInt32() || value.isInt64() || value.isDouble();
    }

    private static Long readIntegralLong(final BsonValue value) {
        if (value.isInt32()) {
            return (long) value.asInt32().getValue();
        }
        if (value.isInt64()) {
            return value.asInt64().getValue();
        }
        if (value.isDouble()) {
            final double doubleValue = value.asDouble().getValue();
            if (!Double.isFinite(doubleValue) || Math.rint(doubleValue) != doubleValue) {
                return null;
            }
            if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                return null;
            }
            return (long) doubleValue;
        }
        return null;
    }

    private static BsonDocument error(final String message, final int code, final String codeName) {
        return new BsonDocument()
                .append("ok", new BsonDouble(0.0))
                .append("errmsg", new BsonString(message))
                .append("code", new BsonInt32(code))
                .append("codeName", new BsonString(codeName));
    }

    private record ParsedTxnNumber(Long value, BsonDocument error) {
        private static ParsedTxnNumber error(final BsonDocument error) {
            return new ParsedTxnNumber(null, error);
        }
    }

    private record ParsedFields(String sessionId, long txnNumber, boolean startTransaction, BsonDocument error) {
        private static ParsedFields error(final BsonDocument error) {
            return new ParsedFields(null, 0L, false, error);
        }
    }
}
