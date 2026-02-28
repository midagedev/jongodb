package org.jongodb.testkit;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.Document;

/**
 * Fixture restore/reset/seed utility for local + CI integration tests.
 */
public final class FixtureRestoreTool {
    private static final Pattern NDJSON_FILE = Pattern.compile("^([^.]+)\\.([^.]+)\\.ndjson$");
    private static final String REPORT_FILE = "fixture-restore-report.json";

    private FixtureRestoreTool() {}

    public static void main(final String[] args) {
        final int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(final String[] args, final PrintStream out, final PrintStream err) {
        Objects.requireNonNull(args, "args");
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(err, "err");

        final Config config;
        try {
            config = Config.fromArgs(args);
        } catch (final IllegalArgumentException exception) {
            err.println(exception.getMessage());
            printUsage(err);
            return 2;
        }

        if (config.help()) {
            printUsage(out);
            return 0;
        }

        try {
            Files.createDirectories(config.reportDir());

            final List<String> diagnostics = new ArrayList<>();
            final PayloadSourceResult payloadSource = loadPayloads(config, out, diagnostics);
            final List<RestoreCollectionReport> reports = new ArrayList<>();

            final Instant startedAt = Instant.now();
            try (MongoClient client = MongoClients.create(new ConnectionString(config.mongoUri()))) {
                for (final FixtureCollectionPayload payload : payloadSource.payloads()) {
                    final List<String> schemaWarnings = schemaWarnings(payload.key(), payload.documents());
                    diagnostics.addAll(schemaWarnings);

                    final MongoCollection<Document> collection =
                            client.getDatabase(payload.database()).getCollection(payload.collection());
                    final int restoredCount = restoreCollection(collection, payload.documents(), config.mode());
                    reports.add(new RestoreCollectionReport(
                            payload.key(),
                            payload.source(),
                            payload.file(),
                            payload.documents().size(),
                            restoredCount,
                            schemaWarnings));
                    out.println("Restored " + payload.key() + " docs=" + restoredCount + " mode=" + config.mode().value());
                }
            }
            final Instant finishedAt = Instant.now();

            final RestoreReport report = new RestoreReport(
                    startedAt.toString(),
                    finishedAt.toString(),
                    config.mode().value(),
                    payloadSource.sourceFormat(),
                    reports,
                    diagnostics);
            Files.writeString(config.reportDir().resolve(REPORT_FILE), report.toJson(), StandardCharsets.UTF_8);

            out.println("Fixture restore finished");
            out.println("- mode: " + config.mode().value());
            out.println("- sourceFormat: " + payloadSource.sourceFormat());
            out.println("- collections: " + reports.size());
            out.println("- diagnostics: " + diagnostics.size());
            out.println("- report: " + config.reportDir().resolve(REPORT_FILE));
            return 0;
        } catch (final IOException | RuntimeException exception) {
            err.println("fixture restore failed: " + exception.getMessage());
            return 1;
        }
    }

    static void resetAndRestore(
            final String mongoUri,
            final Path inputDir,
            final RestoreMode mode) {
        final int exitCode = run(
                new String[] {
                    "--input-dir=" + inputDir.toAbsolutePath().normalize(),
                    "--mongo-uri=" + mongoUri,
                    "--mode=" + mode.value(),
                    "--report-dir=" + inputDir.toAbsolutePath().normalize()
                },
                new PrintStream(System.out),
                new PrintStream(System.err));
        if (exitCode != 0) {
            throw new IllegalStateException("fixture restore failed with exitCode=" + exitCode);
        }
    }

    private static PayloadSourceResult loadPayloads(
            final Config config,
            final PrintStream out,
            final List<String> diagnostics) throws IOException {
        final FixtureArtifactBundle.LoadResult bundleResult = FixtureArtifactBundle.tryLoadBundle(
                config.inputDir(),
                config.regenerateFastCache(),
                FixtureArtifactBundle.currentEngineVersion(),
                config.requiredFixtureVersion(),
                out);

        if (bundleResult != null) {
            diagnostics.addAll(bundleResult.diagnostics());
            if (bundleResult.sourceFormat() != FixtureArtifactBundle.SourceFormat.NDJSON) {
                final List<FixtureCollectionPayload> payloads = toCollectionPayloads(bundleResult.collections(), config);
                return new PayloadSourceResult(bundleResult.sourceFormat().name(), payloads);
            }
        }

        final List<FixtureFile> files = discoverFixtureFiles(config.inputDir(), config);
        final List<FixtureCollectionPayload> payloads = new ArrayList<>(files.size());
        for (final FixtureFile file : files) {
            payloads.add(new FixtureCollectionPayload(
                    file.database(),
                    file.collection(),
                    file.key(),
                    file.path().toString(),
                    "ndjson",
                    readDocuments(file.path())));
        }
        return new PayloadSourceResult("NDJSON", List.copyOf(payloads));
    }

    private static List<FixtureCollectionPayload> toCollectionPayloads(
            final Map<String, List<Document>> collections,
            final Config config) {
        final List<FixtureCollectionPayload> payloads = new ArrayList<>();
        final List<String> namespaces = new ArrayList<>(collections.keySet());
        namespaces.sort(String::compareTo);

        for (final String namespace : namespaces) {
            final Matcher matcher = NDJSON_FILE.matcher(namespace + ".ndjson");
            if (!matcher.matches()) {
                continue;
            }
            final String database = matcher.group(1);
            final String collection = matcher.group(2);
            final String key = database + "." + collection;

            if (config.databaseFilter() != null && !config.databaseFilter().equals(database)) {
                continue;
            }
            if (!config.namespaceFilter().isEmpty() && !config.namespaceFilter().contains(key)) {
                continue;
            }

            payloads.add(new FixtureCollectionPayload(
                    database,
                    collection,
                    key,
                    config.inputDir().resolve(key + ".artifact").toString(),
                    "artifact",
                    collections.getOrDefault(namespace, List.of())));
        }
        return List.copyOf(payloads);
    }

    private static int restoreCollection(
            final MongoCollection<Document> collection,
            final List<Document> documents,
            final RestoreMode mode) {
        if (mode == RestoreMode.REPLACE) {
            collection.deleteMany(new Document());
            if (!documents.isEmpty()) {
                collection.insertMany(documents);
            }
            return documents.size();
        }

        int restored = 0;
        for (final Document document : documents) {
            if (document.containsKey("_id")) {
                final Document filter = new Document("_id", document.get("_id"));
                collection.replaceOne(filter, document, new ReplaceOptions().upsert(true));
                restored++;
                continue;
            }
            collection.insertOne(document);
            restored++;
        }
        return restored;
    }

    private static List<Document> readDocuments(final Path file) throws IOException {
        final List<Document> documents = new ArrayList<>();
        for (final String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line == null || line.isBlank()) {
                continue;
            }
            documents.add(Document.parse(line));
        }
        return List.copyOf(documents);
    }

    private static List<String> schemaWarnings(final String collectionKey, final List<Document> documents) {
        final Set<String> signatures = new LinkedHashSet<>();
        for (final Document document : documents) {
            final List<String> keys = new ArrayList<>(document.keySet());
            keys.sort(String::compareTo);
            signatures.add(String.join(",", keys));
        }
        if (signatures.size() <= 1) {
            return List.of();
        }
        return List.of(collectionKey + " has mixed top-level schema signatures: " + signatures.size());
    }

    private static List<FixtureFile> discoverFixtureFiles(final Path inputDir, final Config config) throws IOException {
        if (!Files.exists(inputDir) || !Files.isDirectory(inputDir)) {
            throw new IllegalArgumentException("input dir is missing: " + inputDir);
        }

        final List<FixtureFile> files = new ArrayList<>();
        try (var stream = Files.list(inputDir)) {
            stream.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".ndjson"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .forEach(path -> {
                        final String fileName = path.getFileName().toString();
                        final Matcher matcher = NDJSON_FILE.matcher(fileName);
                        if (!matcher.matches()) {
                            return;
                        }
                        final String database = matcher.group(1);
                        final String collection = matcher.group(2);
                        final String key = database + "." + collection;
                        if (config.databaseFilter() != null && !config.databaseFilter().equals(database)) {
                            return;
                        }
                        if (!config.namespaceFilter().isEmpty() && !config.namespaceFilter().contains(key)) {
                            return;
                        }
                        files.add(new FixtureFile(database, collection, key, path));
                    });
        }
        return List.copyOf(files);
    }

    private static void printUsage(final PrintStream stream) {
        stream.println("Usage: FixtureRestoreTool --input-dir=<dir> --mongo-uri=<uri> [options]");
        stream.println("  --input-dir=<dir>              Fixture ndjson directory or artifact directory");
        stream.println("  --mongo-uri=<uri>              Target MongoDB URI");
        stream.println("  --mode=replace|merge           Restore mode (default: replace)");
        stream.println("  --database=<db>                Restore only one database");
        stream.println("  --namespace=a.b,c.d            Restore only selected namespaces");
        stream.println("  --report-dir=<dir>             Report output directory (default: input-dir)");
        stream.println("  --regenerate-fast-cache        Regenerate fast cache when portable fallback is used (default)");
        stream.println("  --no-regenerate-fast-cache     Do not regenerate fast cache on fallback");
        stream.println("  --required-fixture-version=v   Required fixture version for compatibility check");
        stream.println("  --help                         Show usage");
    }

    private record FixtureFile(
            String database,
            String collection,
            String key,
            Path path) {}

    private record FixtureCollectionPayload(
            String database,
            String collection,
            String key,
            String file,
            String source,
            List<Document> documents) {}

    private record PayloadSourceResult(
            String sourceFormat,
            List<FixtureCollectionPayload> payloads) {}

    private record Config(
            Path inputDir,
            String mongoUri,
            RestoreMode mode,
            String databaseFilter,
            Set<String> namespaceFilter,
            Path reportDir,
            String requiredFixtureVersion,
            boolean regenerateFastCache,
            boolean help) {
        static Config fromArgs(final String[] args) {
            Path inputDir = null;
            String mongoUri = null;
            RestoreMode mode = RestoreMode.REPLACE;
            String databaseFilter = null;
            final Set<String> namespaceFilter = new LinkedHashSet<>();
            Path reportDir = null;
            String requiredFixtureVersion = null;
            boolean regenerateFastCache = true;
            boolean help = false;

            for (final String arg : args) {
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
                    continue;
                }
                if (arg.startsWith("--input-dir=")) {
                    inputDir = Path.of(valueAfterPrefix(arg, "--input-dir="));
                    continue;
                }
                if (arg.startsWith("--mongo-uri=")) {
                    mongoUri = valueAfterPrefix(arg, "--mongo-uri=");
                    continue;
                }
                if (arg.startsWith("--mode=")) {
                    mode = RestoreMode.fromText(valueAfterPrefix(arg, "--mode="));
                    continue;
                }
                if (arg.startsWith("--database=")) {
                    databaseFilter = valueAfterPrefix(arg, "--database=");
                    continue;
                }
                if (arg.startsWith("--namespace=")) {
                    for (final String item : valueAfterPrefix(arg, "--namespace=").split(",")) {
                        final String namespace = item.trim();
                        if (!namespace.isEmpty()) {
                            namespaceFilter.add(namespace);
                        }
                    }
                    continue;
                }
                if (arg.startsWith("--report-dir=")) {
                    reportDir = Path.of(valueAfterPrefix(arg, "--report-dir="));
                    continue;
                }
                if (arg.startsWith("--required-fixture-version=")) {
                    requiredFixtureVersion = valueAfterPrefix(arg, "--required-fixture-version=");
                    continue;
                }
                if ("--regenerate-fast-cache".equals(arg)) {
                    regenerateFastCache = true;
                    continue;
                }
                if ("--no-regenerate-fast-cache".equals(arg)) {
                    regenerateFastCache = false;
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }

            if (!help && inputDir == null) {
                throw new IllegalArgumentException("--input-dir=<dir> is required");
            }
            if (!help && (mongoUri == null || mongoUri.isBlank())) {
                throw new IllegalArgumentException("--mongo-uri=<uri> is required");
            }
            if (reportDir == null && inputDir != null) {
                reportDir = inputDir;
            }
            return new Config(
                    inputDir,
                    mongoUri,
                    mode,
                    databaseFilter,
                    Set.copyOf(namespaceFilter),
                    reportDir,
                    requiredFixtureVersion,
                    regenerateFastCache,
                    help);
        }

        private static String valueAfterPrefix(final String arg, final String prefix) {
            final String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " must have a value");
            }
            return value;
        }
    }

    enum RestoreMode {
        REPLACE("replace"),
        MERGE("merge");

        private final String value;

        RestoreMode(final String value) {
            this.value = value;
        }

        String value() {
            return value;
        }

        static RestoreMode fromText(final String rawValue) {
            final String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
            for (final RestoreMode mode : values()) {
                if (mode.value.equals(normalized)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("mode must be replace|merge");
        }
    }

    private record RestoreCollectionReport(
            String collection,
            String source,
            String file,
            int sourceDocuments,
            int restoredDocuments,
            List<String> warnings) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("collection", collection);
            root.put("source", source);
            root.put("file", file);
            root.put("sourceDocuments", sourceDocuments);
            root.put("restoredDocuments", restoredDocuments);
            root.put("warnings", warnings);
            return root;
        }
    }

    private record RestoreReport(
            String startedAt,
            String finishedAt,
            String mode,
            String sourceFormat,
            List<RestoreCollectionReport> collections,
            List<String> diagnostics) {
        String toJson() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("startedAt", startedAt);
            root.put("finishedAt", finishedAt);
            root.put("mode", mode);
            root.put("sourceFormat", sourceFormat);
            final List<Map<String, Object>> collectionItems = new ArrayList<>(collections.size());
            for (final RestoreCollectionReport collection : collections) {
                collectionItems.add(collection.toMap());
            }
            root.put("collections", collectionItems);
            root.put("diagnostics", diagnostics);
            return DiffSummaryGenerator.JsonEncoder.encode(root);
        }
    }
}
