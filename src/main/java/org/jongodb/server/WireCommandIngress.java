package org.jongodb.server;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jongodb.command.CommandStore;
import org.jongodb.command.CommandDispatcher;
import org.jongodb.command.EngineBackedCommandStore;
import org.jongodb.engine.InMemoryEngineStore;
import org.jongodb.obs.CommandJournal;
import org.jongodb.obs.CommandJournalInvariantChecker;
import org.jongodb.obs.CorrelationContext;
import org.jongodb.obs.DiagnosticSnapshotDumper;
import org.jongodb.obs.DiagnosticTriageReporter;
import org.jongodb.obs.JsonLinesLogger;
import org.jongodb.obs.ReproExporter;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

public final class WireCommandIngress {
    private static final int DEFAULT_JOURNAL_CAPACITY = 256;

    private static final JsonLinesLogger NOOP_LOGGER = new JsonLinesLogger() {
        @Override
        public void log(
                final String level,
                final String message,
                final CorrelationContext correlationContext,
                final Map<String, ?> fields) {}

        @Override
        public void close() {}
    };

    private final CommandDispatcher dispatcher;
    private final OpMsgCodec codec;
    private final AtomicInteger responseRequestId;
    private final JsonLinesLogger logger;
    private final CommandJournal commandJournal;
    private final DiagnosticSnapshotDumper snapshotDumper;
    private final CommandJournalInvariantChecker invariantChecker;
    private final DiagnosticTriageReporter triageReporter;
    private final ReproExporter reproExporter;

    public WireCommandIngress(final CommandDispatcher dispatcher) {
        this(dispatcher, new OpMsgCodec(), 1, NOOP_LOGGER, new CommandJournal(DEFAULT_JOURNAL_CAPACITY));
    }

    public WireCommandIngress(
            final CommandDispatcher dispatcher, final OpMsgCodec codec, final int initialResponseRequestId) {
        this(
                dispatcher,
                codec,
                initialResponseRequestId,
                NOOP_LOGGER,
                new CommandJournal(DEFAULT_JOURNAL_CAPACITY));
    }

    public WireCommandIngress(
            final CommandDispatcher dispatcher,
            final OpMsgCodec codec,
            final int initialResponseRequestId,
            final JsonLinesLogger logger) {
        this(dispatcher, codec, initialResponseRequestId, logger, new CommandJournal(DEFAULT_JOURNAL_CAPACITY));
    }

    public WireCommandIngress(
            final CommandDispatcher dispatcher,
            final OpMsgCodec codec,
            final int initialResponseRequestId,
            final JsonLinesLogger logger,
            final CommandJournal commandJournal) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.responseRequestId = new AtomicInteger(initialResponseRequestId);
        this.logger = Objects.requireNonNull(logger, "logger");
        this.commandJournal = Objects.requireNonNull(commandJournal, "commandJournal");
        this.snapshotDumper = new DiagnosticSnapshotDumper();
        this.invariantChecker = new CommandJournalInvariantChecker();
        this.triageReporter = new DiagnosticTriageReporter();
        this.reproExporter = new ReproExporter();
    }

    public static WireCommandIngress inMemory() {
        return inMemory(NOOP_LOGGER);
    }

    public static WireCommandIngress inMemory(final JsonLinesLogger logger) {
        return inMemory(logger, new CommandJournal(DEFAULT_JOURNAL_CAPACITY));
    }

    public static WireCommandIngress inMemory(final JsonLinesLogger logger, final CommandJournal commandJournal) {
        final CommandStore commandStore = new EngineBackedCommandStore(new InMemoryEngineStore());
        final CommandDispatcher dispatcher = new CommandDispatcher(commandStore);
        return new WireCommandIngress(dispatcher, new OpMsgCodec(), 1, logger, commandJournal);
    }

    public CommandJournal commandJournal() {
        return commandJournal;
    }

    public BsonDocument dumpDiagnosticSnapshotDocument() {
        return snapshotDumper.dumpDocument(commandJournal);
    }

    public String dumpDiagnosticSnapshotJson() {
        return snapshotDumper.dumpJson(commandJournal);
    }

    public BsonDocument dumpInvariantReportDocument() {
        return invariantChecker.check(commandJournal).toDocument();
    }

    public String dumpInvariantReportJson() {
        return dumpInvariantReportDocument().toJson();
    }

    public BsonDocument dumpFailureTriageReportDocument() {
        final CommandJournalInvariantChecker.InvariantReport invariantReport = invariantChecker.check(commandJournal);
        return triageReporter.report(commandJournal, invariantReport);
    }

    public String dumpFailureTriageReportJson() {
        return dumpFailureTriageReportDocument().toJson();
    }

    public String exportReproJsonLines() {
        return reproExporter.exportJsonLines(commandJournal);
    }

    public byte[] handle(final byte[] requestBytes) {
        final OpMsg request = codec.decode(requestBytes);
        final BsonDocument commandInput = request.body();
        final String commandName = readCommandName(commandInput);
        final CorrelationContext correlation = buildCorrelation(request.requestId(), commandInput, commandName);

        logger.info("command.start", correlation);
        BsonDocument responseBody = null;
        String error = null;
        try {
            responseBody = dispatcher.dispatch(commandInput);
            error = readError(responseBody);
        } catch (RuntimeException exception) {
            error = summarizeException(exception);
            throw exception;
        } finally {
            commandJournal.record(correlation, commandInput, responseBody, error);
            logger.info("command.complete", correlation);
        }

        final OpMsg response =
                new OpMsg(responseRequestId.getAndIncrement(), request.requestId(), 0, responseBody);
        return codec.encode(response);
    }

    private static CorrelationContext buildCorrelation(
            final int requestId, final BsonDocument command, final String commandName) {
        final CorrelationContext.Builder builder =
                CorrelationContext.builder(Integer.toString(requestId), commandName);

        final String sessionId = readSessionId(command);
        if (sessionId != null) {
            builder.sessionId(sessionId);
        }

        final Long txnNumber = readTxnNumber(command);
        if (txnNumber != null) {
            builder.txnNumber(txnNumber);
        }

        return builder.build();
    }

    private static String readSessionId(final BsonDocument command) {
        if (command == null) {
            return null;
        }
        final BsonValue lsid = command.get("lsid");
        if (lsid == null) {
            return null;
        }
        if (lsid.isString()) {
            return lsid.asString().getValue();
        }
        if (!lsid.isDocument()) {
            return lsid.toString();
        }

        final BsonDocument sessionDocument = lsid.asDocument();
        final BsonValue id = sessionDocument.get("id");
        if (id == null) {
            return sessionDocument.toJson();
        }
        if (id.isString()) {
            return id.asString().getValue();
        }
        return id.toString();
    }

    private static Long readTxnNumber(final BsonDocument command) {
        if (command == null) {
            return null;
        }
        final BsonValue txnNumber = command.get("txnNumber");
        if (txnNumber == null || !txnNumber.isNumber()) {
            return null;
        }
        return txnNumber.asNumber().longValue();
    }

    private static String readError(final BsonDocument responseBody) {
        if (responseBody == null) {
            return "command response was null";
        }
        final BsonValue okValue = responseBody.get("ok");
        if (okValue == null || !okValue.isNumber()) {
            return "response missing numeric ok";
        }
        if (okValue.asNumber().doubleValue() == 1.0d) {
            return null;
        }

        final BsonValue errorMessage = responseBody.get("errmsg");
        if (errorMessage != null && errorMessage.isString()) {
            return errorMessage.asString().getValue();
        }
        return "command failed with ok=" + okValue.asNumber().doubleValue();
    }

    private static String summarizeException(final RuntimeException exception) {
        final String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return exception.getClass().getSimpleName() + ": " + message;
    }

    private static String readCommandName(final BsonDocument command) {
        if (command == null || command.isEmpty()) {
            return "unknown";
        }
        return command.getFirstKey().toLowerCase(Locale.ROOT);
    }
}
