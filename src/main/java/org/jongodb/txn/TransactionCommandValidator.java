package org.jongodb.txn;

import java.util.Objects;
import java.util.Set;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Validates transaction command fields and resolves transaction command intent.
 */
public final class TransactionCommandValidator {
    private static final int CODE_INVALID_ARGUMENT = 14;
    private static final int CODE_NO_SUCH_TRANSACTION = 251;
    private static final String LABEL_TRANSIENT_TRANSACTION_ERROR = "TransientTransactionError";
    private static final String LABEL_UNKNOWN_TRANSACTION_COMMIT_RESULT = "UnknownTransactionCommitResult";
    private static final Set<String> TRANSACTIONAL_COMMANDS =
            Set.of(
                    "insert",
                    "update",
                    "delete",
                    "bulkwrite",
                    "find",
                    "findandmodify",
                    "countdocuments",
                    "replaceone",
                    "findoneandupdate",
                    "findoneandreplace",
                    "committransaction",
                    "aborttransaction");

    private final SessionTransactionPool sessionPool;

    public TransactionCommandValidator(final SessionTransactionPool sessionPool) {
        this.sessionPool = Objects.requireNonNull(sessionPool, "sessionPool");
    }

    public ValidationResult validate(final String commandName, final BsonDocument command) {
        if (!TRANSACTIONAL_COMMANDS.contains(commandName)) {
            return ValidationResult.nonTransactional();
        }

        final boolean commitOrAbort = isCommitOrAbort(commandName);
        final boolean commitTransaction = "committransaction".equals(commandName);
        final boolean abortTransaction = "aborttransaction".equals(commandName);
        if (!hasTransactionEnvelope(command, commitOrAbort)) {
            return ValidationResult.nonTransactional();
        }

        final ParsedFields parsedFields = parseFields(commandName, command, commitOrAbort);
        if (parsedFields.error() != null) {
            return ValidationResult.error(parsedFields.error());
        }

        final BsonDocument envelopeError =
                validateTransactionEnvelope(commandName, command, parsedFields.startTransaction(), commitOrAbort);
        if (envelopeError != null) {
            return ValidationResult.error(envelopeError);
        }

        if (parsedFields.startTransaction()) {
            if (sessionPool.hasActiveTransaction(parsedFields.sessionId())) {
                return ValidationResult.error(
                        error("transaction already in progress for this session", CODE_INVALID_ARGUMENT, "BadValue"));
            }
            if (sessionPool.isTxnNumberReused(parsedFields.sessionId(), parsedFields.txnNumber())) {
                return ValidationResult.error(error(
                        "txnNumber must be strictly increasing for each session",
                        CODE_INVALID_ARGUMENT,
                        "BadValue"));
            }
            return ValidationResult.transactional(
                    parsedFields.sessionId(), parsedFields.txnNumber(), true, false, false);
        }

        if (!sessionPool.hasActiveTransaction(parsedFields.sessionId(), parsedFields.txnNumber())) {
            return ValidationResult.error(noSuchTransactionError(commandName, commitTransaction, abortTransaction));
        }

        return ValidationResult.transactional(
                parsedFields.sessionId(),
                parsedFields.txnNumber(),
                false,
                commitTransaction,
                abortTransaction);
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

    private static BsonDocument validateTransactionEnvelope(
            final String commandName,
            final BsonDocument command,
            final boolean startTransaction,
            final boolean commitOrAbort) {
        final BsonDocument writeConcernError = validateWriteConcernEnvelope(command, commitOrAbort);
        if (writeConcernError != null) {
            return writeConcernError;
        }

        final BsonDocument readConcernError = validateReadConcernEnvelope(commandName, command, startTransaction, commitOrAbort);
        if (readConcernError != null) {
            return readConcernError;
        }

        return validateReadPreferenceEnvelope(commandName, command, commitOrAbort);
    }

    private static BsonDocument validateReadConcernEnvelope(
            final String commandName,
            final BsonDocument command,
            final boolean startTransaction,
            final boolean commitOrAbort) {
        final BsonValue readConcernValue = command.get("readConcern");
        if (readConcernValue == null) {
            return null;
        }
        if (!readConcernValue.isDocument()) {
            return error("readConcern must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }
        if (commitOrAbort) {
            return error("readConcern is not allowed for " + commandName, CODE_INVALID_ARGUMENT, "BadValue");
        }
        if (!startTransaction) {
            return error(
                    "readConcern is only allowed when startTransaction is true",
                    CODE_INVALID_ARGUMENT,
                    "BadValue");
        }

        final BsonValue levelValue = readConcernValue.asDocument().get("level");
        if (levelValue != null && !levelValue.isString()) {
            return error("readConcern.level must be a string", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }
        return null;
    }

    private static BsonDocument validateWriteConcernEnvelope(final BsonDocument command, final boolean commitOrAbort) {
        final BsonValue writeConcernValue = command.get("writeConcern");
        if (writeConcernValue == null) {
            return null;
        }
        if (!commitOrAbort) {
            return error("writeConcern is not allowed in a transaction", CODE_INVALID_ARGUMENT, "BadValue");
        }
        if (!writeConcernValue.isDocument()) {
            return error("writeConcern must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonDocument writeConcern = writeConcernValue.asDocument();
        final BsonValue wValue = writeConcern.get("w");
        if (wValue != null) {
            if (!wValue.isString()) {
                final Long parsedW = readIntegralLong(wValue);
                if (parsedW == null) {
                    return error("writeConcern.w must be a string or integer", CODE_INVALID_ARGUMENT, "TypeMismatch");
                }
                if (parsedW < 0) {
                    return error("writeConcern.w must be non-negative when numeric", CODE_INVALID_ARGUMENT, "BadValue");
                }
            }
        }

        final BsonValue jValue = writeConcern.get("j");
        if (jValue != null && !jValue.isBoolean()) {
            return error("writeConcern.j must be a boolean", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonValue wtimeoutValue = writeConcern.get("wtimeout");
        if (wtimeoutValue != null) {
            final Long parsedWtimeout = readIntegralLong(wtimeoutValue);
            if (parsedWtimeout == null) {
                return error("writeConcern.wtimeout must be an integer", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }
            if (parsedWtimeout < 0) {
                return error("writeConcern.wtimeout must be non-negative", CODE_INVALID_ARGUMENT, "BadValue");
            }
        }
        return null;
    }

    private static BsonDocument validateReadPreferenceEnvelope(
            final String commandName, final BsonDocument command, final boolean commitOrAbort) {
        final BsonValue readPreferenceValue = command.get("readPreference");
        final BsonValue dollarReadPreferenceValue = command.get("$readPreference");
        if (readPreferenceValue != null && dollarReadPreferenceValue != null) {
            return error(
                    "readPreference and $readPreference cannot both be specified",
                    CODE_INVALID_ARGUMENT,
                    "BadValue");
        }

        final BsonValue effectiveReadPreference =
                readPreferenceValue != null ? readPreferenceValue : dollarReadPreferenceValue;
        if (effectiveReadPreference == null) {
            return null;
        }
        if (commitOrAbort) {
            return error("readPreference is not allowed for " + commandName, CODE_INVALID_ARGUMENT, "BadValue");
        }
        if (!effectiveReadPreference.isDocument()) {
            return error("readPreference must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonValue modeValue = effectiveReadPreference.asDocument().get("mode");
        if (modeValue == null || !modeValue.isString()) {
            return error("readPreference.mode must be a string", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }
        if (!"primary".equals(modeValue.asString().getValue())) {
            return error("readPreference in a transaction must be primary", CODE_INVALID_ARGUMENT, "BadValue");
        }
        return null;
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

    private static boolean hasTransactionEnvelope(final BsonDocument command, final boolean commitOrAbort) {
        if (commitOrAbort) {
            return true;
        }

        // Retryable writes can include lsid/txnNumber without transaction semantics.
        // Treat commands as transactional only when transaction envelope fields are present.
        return command.containsKey("autocommit") || command.containsKey("startTransaction");
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

    private static BsonDocument noSuchTransactionError(
            final String commandName, final boolean commitTransaction, final boolean abortTransaction) {
        final BsonDocument noSuchTransaction = error(
                commandName + " requires an active transaction",
                CODE_NO_SUCH_TRANSACTION,
                "NoSuchTransaction");
        if (commitTransaction) {
            noSuchTransaction.append(
                    "errorLabels",
                    new BsonArray(List.of(new BsonString(LABEL_UNKNOWN_TRANSACTION_COMMIT_RESULT))));
            return noSuchTransaction;
        }
        if (!abortTransaction) {
            noSuchTransaction.append(
                    "errorLabels",
                    new BsonArray(List.of(new BsonString(LABEL_TRANSIENT_TRANSACTION_ERROR))));
        }
        return noSuchTransaction;
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

    public record ValidationResult(
            boolean transactional,
            String sessionId,
            long txnNumber,
            boolean startTransaction,
            boolean commitTransaction,
            boolean abortTransaction,
            BsonDocument error) {
        private static ValidationResult nonTransactional() {
            return new ValidationResult(false, null, 0L, false, false, false, null);
        }

        private static ValidationResult transactional(
                final String sessionId,
                final long txnNumber,
                final boolean startTransaction,
                final boolean commitTransaction,
                final boolean abortTransaction) {
            return new ValidationResult(
                    true, sessionId, txnNumber, startTransaction, commitTransaction, abortTransaction, null);
        }

        private static ValidationResult error(final BsonDocument error) {
            return new ValidationResult(false, null, 0L, false, false, false, error);
        }
    }
}
