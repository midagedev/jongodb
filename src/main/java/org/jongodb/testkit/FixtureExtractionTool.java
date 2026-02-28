package org.jongodb.testkit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

/**
 * CLI for fixture extraction from MongoDB with guardrails and resumable checkpoints.
 */
public final class FixtureExtractionTool {
    private static final String REPORT_FILE = "fixture-extraction-report.json";
    private static final Pattern NON_ALNUM = Pattern.compile("[^A-Za-z0-9]");
    private static final Set<String> WRITE_ROLES = Set.of(
            "readWrite",
            "dbOwner",
            "dbAdmin",
            "dbAdminAnyDatabase",
            "readWriteAnyDatabase",
            "root",
            "clusterAdmin",
            "userAdmin",
            "userAdminAnyDatabase");

    private FixtureExtractionTool() {}

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
            final FixtureManifest manifest = FixtureManifestLoader.load(config.manifestPath());
            final FixtureExtractionPlan plan = FixtureExtractionPlanner.plan(manifest, config.profile());
            final String sourceAlias = plan.sourceUriAlias();
            enforceAllowlist(config, sourceAlias);
            final String mongoUri = resolveMongoUri(config, sourceAlias);

            Files.createDirectories(config.outputDir());
            final Path checkpointPath = config.outputDir().resolve("checkpoint-" + config.profile().value() + ".json");
            final Checkpoint checkpoint = config.resume() ? Checkpoint.load(checkpointPath) : Checkpoint.empty();

            final List<CollectionReport> reports = new ArrayList<>();
            final Instant startedAt = Instant.now();

            final MongoClientSettings settings = buildSettings(mongoUri, config);
            try (MongoClient client = MongoClients.create(settings)) {
                verifyReadonlyMode(client, config, out, err);
                for (final FixtureExtractionPlan.CollectionPlan collectionPlan : plan.collections()) {
                    final String key = collectionKey(collectionPlan.database(), collectionPlan.collection());
                    if (config.resume() && checkpoint.completedCollections().containsKey(key)) {
                        out.println("Skip completed collection (resume): " + key);
                        final CompletedCollection completed = checkpoint.completedCollections().get(key);
                        reports.add(new CollectionReport(
                                key,
                                completed.file(),
                                completed.count(),
                                completed.sha256(),
                                "SKIPPED_RESUME",
                                null));
                        continue;
                    }

                    final Path outputFile = config.outputDir().resolve(key + ".ndjson");
                    try {
                        final ExtractionResult result = extractCollection(client, collectionPlan, outputFile, config, out);
                        reports.add(new CollectionReport(
                                key,
                                outputFile.toString(),
                                result.count(),
                                result.sha256(),
                                "SUCCESS",
                                null));
                        checkpoint.completedCollections().put(
                                key,
                                new CompletedCollection(outputFile.toString(), result.count(), result.sha256(), Instant.now().toString()));
                        checkpoint.write(checkpointPath);
                    } catch (final RuntimeException | IOException extractionError) {
                        final String errorMessage = extractionError.getMessage() == null
                                ? extractionError.getClass().getSimpleName()
                                : extractionError.getClass().getSimpleName() + ": " + extractionError.getMessage();
                        err.println("Collection extraction failed: " + key + " -> " + errorMessage);
                        reports.add(new CollectionReport(
                                key,
                                outputFile.toString(),
                                0,
                                "",
                                "FAILED",
                                errorMessage));
                    }
                }
            }

            final Instant finishedAt = Instant.now();
            final ExtractionReport report = new ExtractionReport(
                    startedAt.toString(),
                    finishedAt.toString(),
                    plan.sourceUriAlias(),
                    plan.profile().value(),
                    reports);
            Files.writeString(
                    config.outputDir().resolve(REPORT_FILE),
                    report.toJson(),
                    StandardCharsets.UTF_8);

            final long successCount = reports.stream().filter(item -> "SUCCESS".equals(item.status())).count();
            final long failedCount = reports.stream().filter(item -> "FAILED".equals(item.status())).count();
            out.println("Fixture extraction finished");
            out.println("- sourceAlias: " + sourceAlias);
            out.println("- profile: " + plan.profile().value());
            out.println("- collections: " + reports.size());
            out.println("- success: " + successCount);
            out.println("- failed: " + failedCount);
            out.println("- report: " + config.outputDir().resolve(REPORT_FILE));
            return failedCount == 0 ? 0 : 1;
        } catch (final IOException | RuntimeException exception) {
            err.println("fixture extraction failed: " + exception.getMessage());
            return 1;
        }
    }

    private static ExtractionResult extractCollection(
            final MongoClient client,
            final FixtureExtractionPlan.CollectionPlan collectionPlan,
            final Path outputFile,
            final Config config,
            final PrintStream out) throws IOException {
        final MongoCollection<Document> collection = client
                .getDatabase(collectionPlan.database())
                .getCollection(collectionPlan.collection());

        final Document filter = toDocument(collectionPlan.filter());
        final Document projection = toDocument(collectionPlan.projection());
        final Document sort = toDocument(collectionPlan.sort());
        final int effectiveLimit = resolveEffectiveLimit(collectionPlan.limit(), config.maxDocs());

        FindIterable<Document> iterable = collection.find(filter).batchSize(config.batchSize());
        if (!projection.isEmpty()) {
            iterable = iterable.projection(projection);
        }
        if (!sort.isEmpty()) {
            iterable = iterable.sort(sort);
        }
        if (effectiveLimit > 0) {
            iterable = iterable.limit(effectiveLimit);
        }

        final List<String> lines = new ArrayList<>();
        final MessageDigest digest = sha256();
        int count = 0;
        for (final Document document : iterable) {
            final String line = document.toJson();
            lines.add(line);
            digest.update(line.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) '\n');
            count++;
            if (config.rateLimitMillis() > 0) {
                sleepQuietly(config.rateLimitMillis());
            }
            if (count % Math.max(1, config.batchSize()) == 0) {
                out.println("Progress " + collectionKey(collectionPlan.database(), collectionPlan.collection()) + ": " + count);
            }
        }

        final String content = lines.isEmpty() ? "" : String.join("\n", lines) + "\n";
        Files.writeString(outputFile, content, StandardCharsets.UTF_8);
        return new ExtractionResult(count, toHex(digest.digest()));
    }

    private static void verifyReadonlyMode(
            final MongoClient client,
            final Config config,
            final PrintStream out,
            final PrintStream err) {
        if (config.readonlyCheck() == ReadonlyCheck.OFF) {
            out.println("Readonly check: OFF");
            return;
        }

        try {
            final Document response = client.getDatabase("admin")
                    .runCommand(new Document("connectionStatus", 1).append("showPrivileges", true));
            final Object authInfo = response.get("authInfo");
            if (!(authInfo instanceof Document authInfoDocument)) {
                throw new IllegalStateException("connectionStatus authInfo is missing");
            }
            final Object rolesRaw = authInfoDocument.get("authenticatedUserRoles");
            if (!(rolesRaw instanceof List<?> roles)) {
                throw new IllegalStateException("connectionStatus authenticatedUserRoles is missing");
            }

            for (final Object roleRaw : roles) {
                if (!(roleRaw instanceof Document roleDocument)) {
                    continue;
                }
                final Object roleNameRaw = roleDocument.get("role");
                if (!(roleNameRaw instanceof String roleName)) {
                    continue;
                }
                if (WRITE_ROLES.contains(roleName)) {
                    throw new IllegalStateException("write-capable role detected during readonly check: " + roleName);
                }
            }
            out.println("Readonly check: PASS");
        } catch (final RuntimeException exception) {
            if (config.readonlyCheck() == ReadonlyCheck.BEST_EFFORT) {
                err.println("Readonly check warning: " + exception.getMessage());
                return;
            }
            throw exception;
        }
    }

    private static int resolveEffectiveLimit(final Integer planLimit, final Integer cliMaxDocs) {
        int effective = Integer.MAX_VALUE;
        if (planLimit != null && planLimit > 0) {
            effective = Math.min(effective, planLimit);
        }
        if (cliMaxDocs != null && cliMaxDocs > 0) {
            effective = Math.min(effective, cliMaxDocs);
        }
        return effective == Integer.MAX_VALUE ? 0 : effective;
    }

    private static String resolveMongoUri(final Config config, final String sourceAlias) {
        if (config.mongoUri() != null && !config.mongoUri().isBlank()) {
            return config.mongoUri();
        }
        final String envName = "JONGODB_FIXTURE_SOURCE_" + normalizeAliasForEnv(sourceAlias);
        final String envValue = System.getenv(envName);
        if (envValue == null || envValue.isBlank()) {
            throw new IllegalArgumentException("mongo uri is missing. Use --mongo-uri or env " + envName);
        }
        return envValue.trim();
    }

    private static void enforceAllowlist(final Config config, final String sourceAlias) {
        if (config.allowedUriAliases().isEmpty()) {
            return;
        }
        if (!config.allowedUriAliases().contains(sourceAlias)) {
            throw new IllegalArgumentException(
                    "source alias '" + sourceAlias + "' is not in allowlist " + config.allowedUriAliases());
        }
    }

    private static MongoClientSettings buildSettings(final String mongoUri, final Config config) {
        return MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoUri))
                .readPreference(parseReadPreference(config.readPreference()))
                .applyToSocketSettings(builder -> {
                    builder.connectTimeout(config.timeoutMillis(), TimeUnit.MILLISECONDS);
                    builder.readTimeout(config.timeoutMillis(), TimeUnit.MILLISECONDS);
                })
                .build();
    }

    private static ReadPreference parseReadPreference(final String value) {
        final String normalized = Objects.requireNonNull(value, "readPreference").trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "primary" -> ReadPreference.primary();
            case "primarypreferred" -> ReadPreference.primaryPreferred();
            case "secondary" -> ReadPreference.secondary();
            case "secondarypreferred" -> ReadPreference.secondaryPreferred();
            case "nearest" -> ReadPreference.nearest();
            default -> throw new IllegalArgumentException("unsupported readPreference: " + value);
        };
    }

    private static Document toDocument(final Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return new Document();
        }
        return Document.parse(DiffSummaryGenerator.JsonEncoder.encode(source));
    }

    private static String normalizeAliasForEnv(final String alias) {
        final String uppercase = alias.toUpperCase(Locale.ROOT);
        return NON_ALNUM.matcher(uppercase).replaceAll("_");
    }

    private static String collectionKey(final String database, final String collection) {
        return database + "." + collection;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private static String toHex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (final byte item : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", item));
        }
        return sb.toString();
    }

    private static void sleepQuietly(final int millis) {
        try {
            Thread.sleep(Math.max(0, millis));
        } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printUsage(final PrintStream stream) {
        stream.println("Usage: FixtureExtractionTool --manifest=<path> --output-dir=<dir> [options]");
        stream.println("  --manifest=<path>              Fixture manifest path (json/yaml)");
        stream.println("  --profile=dev|smoke|full       Profile to extract (default: dev)");
        stream.println("  --output-dir=<dir>             Output directory for ndjson/report/checkpoint");
        stream.println("  --mongo-uri=<uri>              MongoDB URI (optional; env fallback)");
        stream.println("  --allow-uri-alias=a,b,c        Allowed source aliases from manifest");
        stream.println("  --read-preference=<mode>       primary|primaryPreferred|secondary|secondaryPreferred|nearest");
        stream.println("  --timeout-ms=<int>             Socket connect/read timeout in ms (default: 30000)");
        stream.println("  --batch-size=<int>             Cursor batch size (default: 500)");
        stream.println("  --max-docs=<int>               Upper bound docs per collection");
        stream.println("  --rate-limit-ms=<int>          Delay after each extracted document");
        stream.println("  --resume                       Resume from checkpoint file");
        stream.println("  --readonly-check=strict|best-effort|off   Readonly role verification mode");
        stream.println("  --help                         Show usage");
    }

    private record Config(
            Path manifestPath,
            FixtureManifest.ScenarioProfile profile,
            Path outputDir,
            String mongoUri,
            Set<String> allowedUriAliases,
            String readPreference,
            int timeoutMillis,
            int batchSize,
            Integer maxDocs,
            int rateLimitMillis,
            boolean resume,
            ReadonlyCheck readonlyCheck,
            boolean help) {
        static Config fromArgs(final String[] args) {
            Path manifestPath = null;
            FixtureManifest.ScenarioProfile profile = FixtureManifest.ScenarioProfile.DEV;
            Path outputDir = null;
            String mongoUri = null;
            final Set<String> allowedUriAliases = new LinkedHashSet<>();
            String readPreference = "secondaryPreferred";
            int timeoutMillis = 30_000;
            int batchSize = 500;
            Integer maxDocs = null;
            int rateLimitMillis = 0;
            boolean resume = false;
            ReadonlyCheck readonlyCheck = ReadonlyCheck.BEST_EFFORT;
            boolean help = false;

            for (final String arg : args) {
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
                    continue;
                }
                if ("--resume".equals(arg)) {
                    resume = true;
                    continue;
                }
                if (arg.startsWith("--manifest=")) {
                    manifestPath = Path.of(valueAfterPrefix(arg, "--manifest="));
                    continue;
                }
                if (arg.startsWith("--profile=")) {
                    profile = FixtureManifest.ScenarioProfile.fromText(valueAfterPrefix(arg, "--profile="));
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(valueAfterPrefix(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--mongo-uri=")) {
                    mongoUri = valueAfterPrefix(arg, "--mongo-uri=");
                    continue;
                }
                if (arg.startsWith("--allow-uri-alias=")) {
                    final String raw = valueAfterPrefix(arg, "--allow-uri-alias=");
                    for (final String item : raw.split(",")) {
                        final String alias = item.trim();
                        if (!alias.isEmpty()) {
                            allowedUriAliases.add(alias);
                        }
                    }
                    continue;
                }
                if (arg.startsWith("--read-preference=")) {
                    readPreference = valueAfterPrefix(arg, "--read-preference=");
                    continue;
                }
                if (arg.startsWith("--timeout-ms=")) {
                    timeoutMillis = parsePositiveInt(valueAfterPrefix(arg, "--timeout-ms="), "timeout-ms");
                    continue;
                }
                if (arg.startsWith("--batch-size=")) {
                    batchSize = parsePositiveInt(valueAfterPrefix(arg, "--batch-size="), "batch-size");
                    continue;
                }
                if (arg.startsWith("--max-docs=")) {
                    maxDocs = parsePositiveInt(valueAfterPrefix(arg, "--max-docs="), "max-docs");
                    continue;
                }
                if (arg.startsWith("--rate-limit-ms=")) {
                    rateLimitMillis = parseNonNegativeInt(valueAfterPrefix(arg, "--rate-limit-ms="), "rate-limit-ms");
                    continue;
                }
                if (arg.startsWith("--readonly-check=")) {
                    readonlyCheck = ReadonlyCheck.fromText(valueAfterPrefix(arg, "--readonly-check="));
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }

            if (!help && manifestPath == null) {
                throw new IllegalArgumentException("--manifest=<path> is required");
            }
            if (!help && outputDir == null) {
                throw new IllegalArgumentException("--output-dir=<dir> is required");
            }
            return new Config(
                    manifestPath,
                    profile,
                    outputDir,
                    mongoUri,
                    Set.copyOf(allowedUriAliases),
                    readPreference,
                    timeoutMillis,
                    batchSize,
                    maxDocs,
                    rateLimitMillis,
                    resume,
                    readonlyCheck,
                    help);
        }

        private static String valueAfterPrefix(final String arg, final String prefix) {
            final String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " must have a value");
            }
            return value;
        }

        private static int parsePositiveInt(final String value, final String fieldName) {
            try {
                final int parsed = Integer.parseInt(value);
                if (parsed <= 0) {
                    throw new IllegalArgumentException(fieldName + " must be > 0");
                }
                return parsed;
            } catch (final NumberFormatException numberFormatException) {
                throw new IllegalArgumentException(fieldName + " must be an integer", numberFormatException);
            }
        }

        private static int parseNonNegativeInt(final String value, final String fieldName) {
            try {
                final int parsed = Integer.parseInt(value);
                if (parsed < 0) {
                    throw new IllegalArgumentException(fieldName + " must be >= 0");
                }
                return parsed;
            } catch (final NumberFormatException numberFormatException) {
                throw new IllegalArgumentException(fieldName + " must be an integer", numberFormatException);
            }
        }
    }

    enum ReadonlyCheck {
        STRICT("strict"),
        BEST_EFFORT("best-effort"),
        OFF("off");

        private final String value;

        ReadonlyCheck(final String value) {
            this.value = value;
        }

        static ReadonlyCheck fromText(final String rawValue) {
            final String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
            for (final ReadonlyCheck mode : values()) {
                if (mode.value.equals(normalized)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("readonly-check must be strict|best-effort|off");
        }
    }

    private record ExtractionResult(int count, String sha256) {}

    private record CollectionReport(
            String key,
            String file,
            int count,
            String sha256,
            String status,
            String error) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("collection", key);
            root.put("file", file);
            root.put("count", count);
            root.put("sha256", sha256);
            root.put("status", status);
            if (error != null && !error.isBlank()) {
                root.put("error", error);
            }
            return root;
        }
    }

    private record ExtractionReport(
            String startedAt,
            String finishedAt,
            String sourceAlias,
            String profile,
            List<CollectionReport> collections) {
        String toJson() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("startedAt", startedAt);
            root.put("finishedAt", finishedAt);
            root.put("sourceAlias", sourceAlias);
            root.put("profile", profile);
            final List<Map<String, Object>> collectionItems = new ArrayList<>(collections.size());
            for (final CollectionReport collection : collections) {
                collectionItems.add(collection.toMap());
            }
            root.put("collections", collectionItems);
            return DiffSummaryGenerator.JsonEncoder.encode(root);
        }
    }

    private record CompletedCollection(String file, int count, String sha256, String completedAt) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("file", file);
            root.put("count", count);
            root.put("sha256", sha256);
            root.put("completedAt", completedAt);
            return root;
        }

        static CompletedCollection fromDocument(final Document document) {
            final String file = document.getString("file");
            final Number count = (Number) document.getOrDefault("count", 0);
            final String sha256 = document.getString("sha256");
            final String completedAt = document.getString("completedAt");
            return new CompletedCollection(
                    file == null ? "" : file,
                    count == null ? 0 : count.intValue(),
                    sha256 == null ? "" : sha256,
                    completedAt == null ? "" : completedAt);
        }
    }

    private record Checkpoint(Map<String, CompletedCollection> completedCollections) {
        static Checkpoint empty() {
            return new Checkpoint(new LinkedHashMap<>());
        }

        static Checkpoint load(final Path path) throws IOException {
            if (!Files.exists(path)) {
                return empty();
            }
            final String content = Files.readString(path, StandardCharsets.UTF_8);
            if (content.isBlank()) {
                return empty();
            }
            final Document root = Document.parse(content);
            final Object completedRaw = root.get("completedCollections");
            if (!(completedRaw instanceof Document completedDocument)) {
                return empty();
            }
            final Map<String, CompletedCollection> completed = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : completedDocument.entrySet()) {
                if (!(entry.getValue() instanceof Document completedItem)) {
                    continue;
                }
                completed.put(entry.getKey(), CompletedCollection.fromDocument(completedItem));
            }
            return new Checkpoint(completed);
        }

        void write(final Path path) throws IOException {
            final Map<String, Object> root = new LinkedHashMap<>();
            final Map<String, Object> completedItems = new LinkedHashMap<>();
            for (final Map.Entry<String, CompletedCollection> entry : completedCollections.entrySet()) {
                completedItems.put(entry.getKey(), entry.getValue().toMap());
            }
            root.put("completedCollections", completedItems);
            Files.writeString(path, DiffSummaryGenerator.JsonEncoder.encode(root), StandardCharsets.UTF_8);
        }
    }
}
