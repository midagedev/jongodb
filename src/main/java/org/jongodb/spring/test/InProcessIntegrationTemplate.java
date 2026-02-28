package org.jongodb.spring.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.server.WireCommandIngress;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

/**
 * In-process integration-test template with fixture helpers and optional trace artifact dump.
 */
public final class InProcessIntegrationTemplate {
    private static final String DEFAULT_DATABASE = "test";

    private final OpMsgCodec codec = new OpMsgCodec();
    private final AtomicInteger requestId = new AtomicInteger(1_000);

    private volatile WireCommandIngress ingress;
    private volatile TraceArtifactConfig traceArtifactConfig;

    public InProcessIntegrationTemplate() {
        this.ingress = WireCommandIngress.inMemory();
    }

    public synchronized void reset() {
        this.ingress = WireCommandIngress.inMemory();
    }

    public synchronized void enableFailureTraceArtifacts(final Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        this.traceArtifactConfig = new TraceArtifactConfig(outputDir.toAbsolutePath().normalize());
    }

    public synchronized void disableFailureTraceArtifacts() {
        this.traceArtifactConfig = null;
    }

    public BsonDocument runCommand(final BsonDocument command) {
        Objects.requireNonNull(command, "command");
        final BsonDocument response = dispatch(command);
        if (isFailure(response)) {
            maybeWriteFailureArtifacts(command, response);
        }
        return response;
    }

    public BsonDocument runCommand(final String commandJson) {
        return runCommand(BsonDocument.parse(Objects.requireNonNull(commandJson, "commandJson")));
    }

    public BsonDocument seedDocuments(
            final String database,
            final String collection,
            final List<BsonDocument> documents) {
        Objects.requireNonNull(documents, "documents");
        final BsonArray bsonDocuments = new BsonArray();
        for (final BsonDocument document : documents) {
            bsonDocuments.add(Objects.requireNonNull(document, "documents entries must not be null").clone());
        }
        final BsonDocument command = new BsonDocument()
                .append("insert", new BsonString(requireText(collection, "collection")))
                .append("$db", new BsonString(normalizeDatabase(database)))
                .append("documents", bsonDocuments);
        return runCommand(command);
    }

    public BsonDocument findAll(final String database, final String collection) {
        final BsonDocument command = new BsonDocument()
                .append("find", new BsonString(requireText(collection, "collection")))
                .append("$db", new BsonString(normalizeDatabase(database)))
                .append("filter", new BsonDocument());
        return runCommand(command);
    }

    private BsonDocument dispatch(final BsonDocument command) {
        final WireCommandIngress currentIngress = ingress;
        final OpMsg request = new OpMsg(requestId.getAndIncrement(), 0, 0, command.clone());
        final OpMsg response = codec.decode(currentIngress.handle(codec.encode(request)));
        return response.body().clone();
    }

    private void maybeWriteFailureArtifacts(final BsonDocument command, final BsonDocument response) {
        final TraceArtifactConfig config = traceArtifactConfig;
        if (config == null) {
            return;
        }

        final String commandName = command.isEmpty() ? "unknown" : command.getFirstKey().toLowerCase(Locale.ROOT);
        final String timestamp = Instant.now().toString().replace(":", "-");
        final String baseName = timestamp + "-" + commandName;

        try {
            Files.createDirectories(config.outputDir());
            final Path snapshotPath = config.outputDir().resolve(baseName + "-snapshot.json");
            final Path invariantPath = config.outputDir().resolve(baseName + "-invariant.json");
            final Path triagePath = config.outputDir().resolve(baseName + "-triage.json");
            final Path reproPath = config.outputDir().resolve(baseName + "-repro.jsonl");
            final Path responsePath = config.outputDir().resolve(baseName + "-response.json");

            final WireCommandIngress currentIngress = ingress;
            Files.writeString(snapshotPath, currentIngress.dumpDiagnosticSnapshotJson(), StandardCharsets.UTF_8);
            Files.writeString(invariantPath, currentIngress.dumpInvariantReportJson(), StandardCharsets.UTF_8);
            Files.writeString(triagePath, currentIngress.dumpFailureTriageReportJson(), StandardCharsets.UTF_8);
            Files.writeString(reproPath, currentIngress.exportReproJsonLines(), StandardCharsets.UTF_8);
            Files.writeString(responsePath, response.toJson(), StandardCharsets.UTF_8);
        } catch (final IOException ioException) {
            throw new IllegalStateException("failed to write trace artifacts: " + ioException.getMessage(), ioException);
        }
    }

    private static boolean isFailure(final BsonDocument response) {
        if (response == null) {
            return true;
        }
        final BsonValue ok = response.get("ok");
        if (ok == null || !ok.isNumber()) {
            return true;
        }
        return ok.asNumber().doubleValue() != 1.0d;
    }

    private static String normalizeDatabase(final String database) {
        if (database == null || database.isBlank()) {
            return DEFAULT_DATABASE;
        }
        return database.trim();
    }

    private static String requireText(final String value, final String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    private record TraceArtifactConfig(Path outputDir) {}
}
