package org.jongodb.obs;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Evaluates deterministic diagnostics invariants from command journal entries.
 */
public final class CommandJournalInvariantChecker {
    private static final Set<String> TRANSACTIONAL_COMMANDS =
            Set.of("insert", "update", "delete", "find", "committransaction", "aborttransaction");
    private static final List<String> CATEGORY_ORDER = List.of("session", "transaction", "cursor", "index");
    private static final Comparator<Violation> VIOLATION_ORDER = Comparator
            .comparingLong(Violation::sequence)
            .thenComparing(Violation::category)
            .thenComparing(Violation::code)
            .thenComparing(Violation::requestId);

    public InvariantReport check(final CommandJournal journal) {
        Objects.requireNonNull(journal, "journal");

        final List<CommandJournal.Entry> entries = journal.entries();
        final List<Violation> violations = new ArrayList<>();
        final Map<String, SessionTxnState> sessionStates = new LinkedHashMap<>();
        final boolean historyMayBeTruncated = journal.droppedCount() > 0L;

        for (final CommandJournal.Entry entry : entries) {
            checkSessionAndTransaction(entry, violations, sessionStates, historyMayBeTruncated);
            checkCursor(entry, violations);
            checkIndex(entry, violations);
        }

        violations.sort(VIOLATION_ORDER);
        return new InvariantReport(violations);
    }

    private static void checkSessionAndTransaction(
            final CommandJournal.Entry entry,
            final List<Violation> violations,
            final Map<String, SessionTxnState> sessionStates,
            final boolean historyMayBeTruncated) {
        final CorrelationContext correlation = entry.correlationContext();
        final TxnSignals signals = parseTxnSignals(entry.commandInput(), correlation.commandName());
        if (!signals.hasTransactionEnvelope()) {
            return;
        }

        final String sessionId = correlation.sessionId().orElse(null);
        final Long txnNumber = signals.txnNumber();
        if (!signals.hasLsid()) {
            violations.add(violation(
                    entry,
                    "session",
                    "SESSION_MISSING_LSID",
                    "transaction envelope fields require lsid"));
        }

        if (signals.hasTxnNumberField() && txnNumber == null) {
            violations.add(violation(
                    entry,
                    "transaction",
                    "TXN_NUMBER_NOT_INTEGRAL",
                    "txnNumber must be an integral number"));
        }

        if (signals.autocommitPresent() && !Boolean.FALSE.equals(signals.autocommitValue())) {
            violations.add(violation(
                    entry,
                    "transaction",
                    "TXN_AUTOCOMMIT_NOT_FALSE",
                    "autocommit must be false for transaction envelope commands"));
        }

        if (signals.startTransactionPresent() && !Boolean.TRUE.equals(signals.startTransactionValue())) {
            violations.add(violation(
                    entry,
                    "transaction",
                    "TXN_START_FLAG_NOT_TRUE",
                    "startTransaction must be true when the field is present"));
        }

        if (sessionId == null || txnNumber == null) {
            return;
        }

        final SessionTxnState state = sessionStates.computeIfAbsent(sessionId, key -> new SessionTxnState());
        state.onObserved(signals, historyMayBeTruncated);

        if (signals.startTransaction()) {
            if (state.activeTxnNumber != null) {
                violations.add(violation(
                        entry,
                        "transaction",
                        "TXN_START_WHILE_ACTIVE",
                        "startTransaction observed while another transaction is still active for the session"));
            } else if (state.lastClosedTxnNumber != null && txnNumber <= state.lastClosedTxnNumber) {
                violations.add(violation(
                        entry,
                        "transaction",
                        "TXN_NUMBER_NOT_INCREASING",
                        "startTransaction txnNumber must increase after the previously closed transaction"));
            }

            if (isSuccess(entry)) {
                state.start(txnNumber);
            }
            return;
        }

        if (signals.commitCommand() || signals.abortCommand()) {
            if (state.activeTxnNumber == null) {
                if (state.canAssertLifecycle()) {
                    violations.add(violation(
                            entry,
                            "transaction",
                            "TXN_TERMINAL_WITHOUT_ACTIVE",
                            correlation.commandName() + " observed without an active transaction in journal state"));
                }
            } else if (!state.activeTxnNumber.equals(txnNumber)) {
                violations.add(violation(
                        entry,
                        "transaction",
                        "TXN_TERMINAL_NUMBER_MISMATCH",
                        correlation.commandName() + " txnNumber does not match the active transaction"));
            }

            if (isSuccess(entry)) {
                state.end(txnNumber);
            }
            return;
        }

        if (!signals.transactionalCommand()) {
            return;
        }

        if (state.activeTxnNumber == null) {
            if (state.canAssertLifecycle()) {
                violations.add(violation(
                        entry,
                        "transaction",
                        "TXN_COMMAND_WITHOUT_ACTIVE",
                        "transactional command observed without an active transaction in journal state"));
            }
            return;
        }

        if (!state.activeTxnNumber.equals(txnNumber)) {
            violations.add(violation(
                    entry,
                    "transaction",
                    "TXN_COMMAND_NUMBER_MISMATCH",
                    "transactional command txnNumber does not match the active transaction"));
        }
    }

    private static void checkCursor(final CommandJournal.Entry entry, final List<Violation> violations) {
        final CorrelationContext correlation = entry.correlationContext();
        if (!"find".equals(correlation.commandName()) || !isSuccess(entry)) {
            return;
        }

        final BsonDocument output = entry.commandOutput();
        final BsonValue cursorValue = output.get("cursor");
        if (cursorValue == null || !cursorValue.isDocument()) {
            violations.add(violation(
                    entry,
                    "cursor",
                    "CURSOR_MISSING_DOCUMENT",
                    "find success response must include a cursor document"));
            return;
        }

        final BsonDocument cursor = cursorValue.asDocument();
        final BsonValue cursorId = cursor.get("id");
        if (cursorId == null || !cursorId.isNumber()) {
            violations.add(violation(
                    entry,
                    "cursor",
                    "CURSOR_ID_NOT_NUMERIC",
                    "cursor.id must be numeric"));
        }

        final BsonValue namespaceValue = cursor.get("ns");
        if (namespaceValue == null || !namespaceValue.isString()) {
            violations.add(violation(
                    entry,
                    "cursor",
                    "CURSOR_NAMESPACE_NOT_STRING",
                    "cursor.ns must be a string"));
        } else {
            final String expectedNamespace = expectedFindNamespace(entry.commandInput());
            if (expectedNamespace != null && !expectedNamespace.equals(namespaceValue.asString().getValue())) {
                violations.add(violation(
                        entry,
                        "cursor",
                        "CURSOR_NAMESPACE_MISMATCH",
                        "cursor.ns does not match requested namespace " + expectedNamespace));
            }
        }

        final BsonValue firstBatch = cursor.get("firstBatch");
        if (firstBatch == null || !firstBatch.isArray()) {
            violations.add(violation(
                    entry,
                    "cursor",
                    "CURSOR_FIRST_BATCH_NOT_ARRAY",
                    "cursor.firstBatch must be an array"));
        }
    }

    private static void checkIndex(final CommandJournal.Entry entry, final List<Violation> violations) {
        final CorrelationContext correlation = entry.correlationContext();
        if (!"createindexes".equals(correlation.commandName()) || !isSuccess(entry)) {
            return;
        }

        final BsonDocument output = entry.commandOutput();
        final Long before = readIntegralLong(output.get("numIndexesBefore"));
        final Long after = readIntegralLong(output.get("numIndexesAfter"));
        if (before == null || after == null) {
            violations.add(violation(
                    entry,
                    "index",
                    "INDEX_COUNT_FIELDS_INVALID",
                    "createIndexes success response must include integral numIndexesBefore/numIndexesAfter"));
        } else if (after < before) {
            violations.add(violation(
                    entry,
                    "index",
                    "INDEX_COUNT_REGRESSED",
                    "numIndexesAfter must be greater than or equal to numIndexesBefore"));
        }

        final BsonValue createdCollectionAutomatically = output.get("createdCollectionAutomatically");
        if (createdCollectionAutomatically == null || !createdCollectionAutomatically.isBoolean()) {
            violations.add(violation(
                    entry,
                    "index",
                    "INDEX_CREATED_COLLECTION_FLAG_INVALID",
                    "createIndexes success response must include createdCollectionAutomatically boolean"));
        }
    }

    private static TxnSignals parseTxnSignals(final BsonDocument commandInput, final String commandName) {
        final BsonDocument command = commandInput == null ? new BsonDocument() : commandInput;
        final boolean hasLsid = command.containsKey("lsid");
        final boolean hasTxnNumberField = command.containsKey("txnNumber");
        final Long txnNumber = readIntegralLong(command.get("txnNumber"));

        final boolean autocommitPresent = command.containsKey("autocommit");
        final Boolean autocommitValue = readBoolean(command.get("autocommit"));
        final boolean startTransactionPresent = command.containsKey("startTransaction");
        final Boolean startTransactionValue = readBoolean(command.get("startTransaction"));
        final boolean commit = "committransaction".equals(commandName);
        final boolean abort = "aborttransaction".equals(commandName);

        return new TxnSignals(
                hasLsid,
                hasTxnNumberField,
                txnNumber,
                autocommitPresent,
                autocommitValue,
                startTransactionPresent,
                startTransactionValue,
                commit,
                abort,
                TRANSACTIONAL_COMMANDS.contains(commandName));
    }

    private static String expectedFindNamespace(final BsonDocument commandInput) {
        if (commandInput == null) {
            return null;
        }
        final BsonValue collectionValue = commandInput.get("find");
        if (collectionValue == null || !collectionValue.isString()) {
            return null;
        }
        final String database = readDatabase(commandInput);
        final String collection = collectionValue.asString().getValue();
        return database + "." + collection;
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static boolean isSuccess(final CommandJournal.Entry entry) {
        if (entry.failed()) {
            return false;
        }
        final BsonDocument output = entry.commandOutput();
        if (output == null) {
            return false;
        }
        final BsonValue okValue = output.get("ok");
        if (okValue == null || !okValue.isNumber()) {
            return false;
        }
        return okValue.asNumber().doubleValue() == 1.0d;
    }

    private static Boolean readBoolean(final BsonValue value) {
        if (value == null || !value.isBoolean()) {
            return null;
        }
        return value.asBoolean().getValue();
    }

    private static Long readIntegralLong(final BsonValue value) {
        if (value == null || !value.isNumber()) {
            return null;
        }
        if (value.isInt32()) {
            return (long) value.asInt32().getValue();
        }
        if (value.isInt64()) {
            return value.asInt64().getValue();
        }
        if (value.isDouble()) {
            final double number = value.asDouble().getValue();
            if (!Double.isFinite(number) || Math.rint(number) != number) {
                return null;
            }
            if (number < Long.MIN_VALUE || number > Long.MAX_VALUE) {
                return null;
            }
            return (long) number;
        }
        return null;
    }

    private static Violation violation(
            final CommandJournal.Entry entry, final String category, final String code, final String message) {
        final CorrelationContext correlation = entry.correlationContext();
        return new Violation(
                category,
                code,
                "error",
                entry.sequence(),
                correlation.requestId(),
                correlation.commandName(),
                message,
                correlation.sessionId().orElse(null),
                correlation.txnNumber().orElse(null));
    }

    public record InvariantReport(List<Violation> violations) {
        public InvariantReport {
            violations = List.copyOf(Objects.requireNonNull(violations, "violations"));
        }

        public boolean hasViolations() {
            return !violations.isEmpty();
        }

        public BsonDocument toDocument() {
            final BsonArray encodedViolations = new BsonArray(violations.size());
            final Map<String, Integer> counts = new LinkedHashMap<>();
            for (final String category : CATEGORY_ORDER) {
                counts.put(category, 0);
            }

            for (final Violation violation : violations) {
                encodedViolations.add(violation.toDocument());
                counts.compute(violation.category(), (key, value) -> value == null ? 1 : value + 1);
            }

            final BsonDocument byCategory = new BsonDocument();
            for (final String category : CATEGORY_ORDER) {
                byCategory.append(category, new BsonInt32(counts.getOrDefault(category, 0)));
            }

            return new BsonDocument()
                    .append("violationCount", new BsonInt32(violations.size()))
                    .append("byCategory", byCategory)
                    .append("violations", encodedViolations);
        }
    }

    public record Violation(
            String category,
            String code,
            String severity,
            long sequence,
            String requestId,
            String commandName,
            String message,
            String sessionId,
            Long txnNumber) {
        public Violation {
            Objects.requireNonNull(category, "category");
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(severity, "severity");
            Objects.requireNonNull(requestId, "requestId");
            Objects.requireNonNull(commandName, "commandName");
            Objects.requireNonNull(message, "message");
        }

        public BsonDocument toDocument() {
            final BsonDocument encoded = new BsonDocument()
                    .append("sequence", new BsonInt64(sequence))
                    .append("category", new BsonString(category))
                    .append("code", new BsonString(code))
                    .append("severity", new BsonString(severity))
                    .append("requestId", new BsonString(requestId))
                    .append("commandName", new BsonString(commandName))
                    .append("message", new BsonString(message));

            if (sessionId != null) {
                encoded.append("sessionId", new BsonString(sessionId));
            }
            if (txnNumber != null) {
                encoded.append("txnNumber", new BsonInt64(txnNumber));
            }
            return encoded;
        }
    }

    private static final class SessionTxnState {
        private Long activeTxnNumber;
        private Long lastClosedTxnNumber;
        private boolean sawStartInWindow;
        private boolean historyUnknown;
        private boolean observedTransactionEnvelope;

        private void onObserved(final TxnSignals signals, final boolean historyMayBeTruncated) {
            if (!observedTransactionEnvelope) {
                observedTransactionEnvelope = true;
                if (historyMayBeTruncated && !signals.startTransaction()) {
                    historyUnknown = true;
                }
            }
        }

        private boolean canAssertLifecycle() {
            return !historyUnknown || sawStartInWindow || activeTxnNumber != null;
        }

        private void start(final long txnNumber) {
            activeTxnNumber = txnNumber;
            sawStartInWindow = true;
            historyUnknown = false;
        }

        private void end(final long txnNumber) {
            if (activeTxnNumber != null && activeTxnNumber == txnNumber) {
                lastClosedTxnNumber = txnNumber;
                activeTxnNumber = null;
                historyUnknown = false;
                return;
            }

            // Keep deterministic progress for truncated windows where we only observe the terminal command.
            if (!sawStartInWindow) {
                lastClosedTxnNumber = txnNumber;
            }
            activeTxnNumber = null;
            historyUnknown = false;
        }
    }

    private record TxnSignals(
            boolean hasLsid,
            boolean hasTxnNumberField,
            Long txnNumber,
            boolean autocommitPresent,
            Boolean autocommitValue,
            boolean startTransactionPresent,
            Boolean startTransactionValue,
            boolean commitCommand,
            boolean abortCommand,
            boolean transactionalCommand) {
        private boolean hasTransactionEnvelope() {
            return hasTxnNumberField || autocommitPresent || startTransactionPresent || commitCommand || abortCommand;
        }

        private boolean startTransaction() {
            return Boolean.TRUE.equals(startTransactionValue);
        }
    }
}
