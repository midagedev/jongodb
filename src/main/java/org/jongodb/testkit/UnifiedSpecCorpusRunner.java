package org.jongodb.testkit;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import org.bson.Document;

/**
 * Runs imported Unified Test Format scenarios through the differential harness and writes artifacts.
 */
public final class UnifiedSpecCorpusRunner {
    private static final String DEFAULT_MONGO_URI_ENV = "JONGODB_REAL_MONGOD_URI";
    private static final Path DEFAULT_SPEC_ROOT = Path.of("testkit/specs/unified");
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/unified-spec");
    private static final String DEFAULT_SEED = "utf-corpus-v1";
    private static final int DEFAULT_REPLAY_LIMIT = 20;
    private static final String JSON_ARTIFACT_FILE = "utf-differential-report.json";
    private static final String MARKDOWN_ARTIFACT_FILE = "utf-differential-report.md";
    private static final String REPLAY_BUNDLE_DIR = DeterministicReplayBundles.DEFAULT_BUNDLE_DIR_NAME;

    private final Clock clock;
    private final UnifiedSpecImporter importer;
    private final Supplier<DifferentialBackend> leftBackendFactory;
    private final Function<String, DifferentialBackend> rightBackendFactory;

    public UnifiedSpecCorpusRunner() {
        this(
                Clock.systemUTC(),
                new UnifiedSpecImporter(UnifiedSpecImporter.ImportProfile.STRICT),
                () -> new WireCommandIngressBackend("wire-backend"),
                mongoUri -> new RealMongodBackend("real-mongod", mongoUri));
    }

    UnifiedSpecCorpusRunner(
            final Clock clock,
            final UnifiedSpecImporter importer,
            final Supplier<DifferentialBackend> leftBackendFactory,
            final Function<String, DifferentialBackend> rightBackendFactory) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.importer = Objects.requireNonNull(importer, "importer");
        this.leftBackendFactory = Objects.requireNonNull(leftBackendFactory, "leftBackendFactory");
        this.rightBackendFactory = Objects.requireNonNull(rightBackendFactory, "rightBackendFactory");
    }

    public static void main(final String[] args) throws Exception {
        if (containsHelpFlag(args)) {
            printUsage();
            return;
        }

        final RunConfig config;
        try {
            config = RunConfig.fromArgs(args);
        } catch (final IllegalArgumentException exception) {
            System.err.println("Invalid argument: " + exception.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        final UnifiedSpecCorpusRunner runner = new UnifiedSpecCorpusRunner(
                Clock.systemUTC(),
                new UnifiedSpecImporter(config.importProfile()),
                () -> new WireCommandIngressBackend("wire-backend"),
                mongoUri -> new RealMongodBackend("real-mongod", mongoUri));
        final RunResult result = runner.runAndWrite(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        final DifferentialReport differential = result.differentialReport();
        System.out.println("UTF differential report generated.");
        System.out.println("- generatedAt: " + result.generatedAt());
        System.out.println("- seed: " + result.seed() + " (numericSeed=" + result.numericSeed() + ")");
        System.out.println("- importProfile: " + result.config().importProfile().cliValue());
        System.out.println("- imported: " + result.importResult().importedCount());
        System.out.println("- skipped: " + result.importResult().skippedCount());
        System.out.println("- unsupported: " + result.importResult().unsupportedCount());
        System.out.println("- match: " + differential.matchCount());
        System.out.println("- mismatch: " + differential.mismatchCount());
        System.out.println("- error: " + differential.errorCount());
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());
    }

    public RunResult runAndWrite(final RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        final RunResult result = run(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        Files.createDirectories(config.outputDir());
        Files.writeString(paths.jsonArtifact(), renderJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.markdownArtifact(), renderMarkdown(result), StandardCharsets.UTF_8);
        DeterministicReplayBundles.writeBundles(paths.replayBundleDir(), result.replayBundles());
        return result;
    }

    public RunResult run(final RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        final UnifiedSpecImporter.RunOnContext runOnContext = detectRunOnContext(config.mongoUri());
        final UnifiedSpecImporter.ImportResult importResult =
                importer.importCorpus(config.specRoot(), runOnContext);
        final long numericSeed = RealMongodCorpusRunner.deterministicSeed(config.seed());

        final List<UnifiedSpecImporter.ImportedScenario> orderedImported =
                orderedScenarios(importResult.importedScenarios(), numericSeed);
        final List<Scenario> scenarios = new ArrayList<>(orderedImported.size());
        final Map<String, Scenario> scenarioById = new LinkedHashMap<>();
        for (final UnifiedSpecImporter.ImportedScenario importedScenario : orderedImported) {
            scenarios.add(importedScenario.scenario());
            scenarioById.put(importedScenario.caseId(), importedScenario.scenario());
        }

        final DifferentialHarness harness = new DifferentialHarness(
                Objects.requireNonNull(leftBackendFactory.get(), "leftBackendFactory result"),
                Objects.requireNonNull(rightBackendFactory.apply(config.mongoUri()), "rightBackendFactory result"),
                clock);
        final DifferentialReport report = harness.run(List.copyOf(scenarios));
        final List<FailureReplay> failureReplays =
                buildFailureReplays(report, scenarioById, config.replayLimit());
        final List<DeterministicReplayBundles.Bundle> replayBundles =
                buildReplayBundles(report, scenarioById);

        return new RunResult(
                config,
                config.seed(),
                numericSeed,
                report.generatedAt(),
                importResult,
                report,
                failureReplays,
                replayBundles);
    }

    private static UnifiedSpecImporter.RunOnContext detectRunOnContext(final String mongoUri) {
        Objects.requireNonNull(mongoUri, "mongoUri");
        try (MongoClient client = MongoClients.create(mongoUri)) {
            final MongoDatabase admin = client.getDatabase("admin");
            final Document buildInfo = admin.runCommand(new Document("buildInfo", 1));
            final Document hello = admin.runCommand(new Document("hello", 1));
            final String serverVersion = readServerVersion(buildInfo);
            final String topology = detectTopology(hello);
            final boolean serverless = readBoolean(hello, "isServerless");
            return UnifiedSpecImporter.RunOnContext.evaluated(
                    serverVersion,
                    topology,
                    serverless,
                    false);
        } catch (final RuntimeException ignored) {
            return UnifiedSpecImporter.RunOnContext.unevaluated();
        }
    }

    private static String readServerVersion(final Document buildInfo) {
        final Object versionValue = buildInfo.get("version");
        if (versionValue == null) {
            return "";
        }
        return String.valueOf(versionValue).trim();
    }

    private static String detectTopology(final Document hello) {
        if (hello.containsKey("setName")) {
            return "replicaset";
        }
        final Object message = hello.get("msg");
        if (message != null && "isdbgrid".equalsIgnoreCase(String.valueOf(message))) {
            return "sharded";
        }
        if (readBoolean(hello, "loadBalanced")) {
            return "load-balanced";
        }
        return "single";
    }

    private static boolean readBoolean(final Document document, final String key) {
        final Object rawValue = document.get(key);
        if (rawValue instanceof Boolean booleanValue) {
            return booleanValue;
        }
        return false;
    }

    public static ArtifactPaths artifactPaths(final Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        final Path normalized = outputDir.normalize();
        return new ArtifactPaths(
                normalized.resolve(JSON_ARTIFACT_FILE),
                normalized.resolve(MARKDOWN_ARTIFACT_FILE),
                normalized.resolve(REPLAY_BUNDLE_DIR));
    }

    private static List<UnifiedSpecImporter.ImportedScenario> orderedScenarios(
            final List<UnifiedSpecImporter.ImportedScenario> importedScenarios,
            final long numericSeed) {
        final List<UnifiedSpecImporter.ImportedScenario> ordered = new ArrayList<>(importedScenarios);
        ordered.sort(Comparator.comparing(UnifiedSpecImporter.ImportedScenario::caseId));
        final Random random = new Random(numericSeed);
        for (int index = ordered.size() - 1; index > 0; index--) {
            final int swapIndex = random.nextInt(index + 1);
            final UnifiedSpecImporter.ImportedScenario current = ordered.get(index);
            ordered.set(index, ordered.get(swapIndex));
            ordered.set(swapIndex, current);
        }
        return List.copyOf(ordered);
    }

    private static List<FailureReplay> buildFailureReplays(
            final DifferentialReport report,
            final Map<String, Scenario> scenarioById,
            final int replayLimit) {
        if (replayLimit <= 0) {
            throw new IllegalArgumentException("replayLimit must be > 0");
        }

        final List<FailureReplay> replays = new ArrayList<>();
        for (final DiffResult result : report.results()) {
            if (result.status() == DiffStatus.MATCH) {
                continue;
            }
            if (replays.size() >= replayLimit) {
                break;
            }
            final Scenario scenario = scenarioById.get(result.scenarioId());
            final String message = replayMessage(result);
            replays.add(new FailureReplay(
                    result.scenarioId(),
                    result.status(),
                    message,
                    scenario == null ? List.of() : scenario.commands()));
        }
        return List.copyOf(replays);
    }

    private static List<DeterministicReplayBundles.Bundle> buildReplayBundles(
            final DifferentialReport report,
            final Map<String, Scenario> scenarioById) {
        final List<DeterministicReplayBundles.Bundle> bundles = new ArrayList<>();
        for (final DiffResult result : report.results()) {
            if (result.status() == DiffStatus.MATCH) {
                continue;
            }
            final Scenario scenario = scenarioById.get(result.scenarioId());
            final List<ScenarioCommand> commands = scenario == null ? List.of() : scenario.commands();
            bundles.add(DeterministicReplayBundles.fromFailure("utf", result, commands));
        }
        return List.copyOf(bundles);
    }

    private static String replayMessage(final DiffResult result) {
        if (result.status() == DiffStatus.ERROR) {
            return result.errorMessage().orElse("unknown error");
        }
        if (result.entries().isEmpty()) {
            return "mismatch without diff entries";
        }
        final DiffEntry first = result.entries().get(0);
        return first.path() + ": " + first.note();
    }

    String renderJson(final RunResult result) {
        Objects.requireNonNull(result, "result");
        final Document root = new Document();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("seed", result.seed());
        root.put("numericSeed", result.numericSeed());
        root.put("specRoot", result.config().specRoot().toString());
        root.put("importProfile", result.config().importProfile().cliValue());
        root.put("backends", new Document()
                .append("left", result.differentialReport().leftBackend())
                .append("right", result.differentialReport().rightBackend()));

        root.put("importSummary", new Document()
                .append("imported", result.importResult().importedCount())
                .append("skipped", result.importResult().skippedCount())
                .append("unsupported", result.importResult().unsupportedCount()));

        root.put("differentialSummary", new Document()
                .append("total", result.differentialReport().totalScenarios())
                .append("match", result.differentialReport().matchCount())
                .append("mismatch", result.differentialReport().mismatchCount())
                .append("error", result.differentialReport().errorCount()));

        final List<Document> skippedCases = new ArrayList<>(result.importResult().skippedCases().size());
        for (final UnifiedSpecImporter.SkippedCase skippedCase : result.importResult().skippedCases()) {
            skippedCases.add(new Document()
                    .append("caseId", skippedCase.caseId())
                    .append("sourcePath", skippedCase.sourcePath())
                    .append("kind", skippedCase.kind().name())
                    .append("reason", skippedCase.reason()));
        }
        root.put("skippedCases", skippedCases);

        final List<Document> failureReplays = new ArrayList<>(result.failureReplays().size());
        for (final FailureReplay failureReplay : result.failureReplays()) {
            final List<Document> commands = new ArrayList<>(failureReplay.commands().size());
            for (final ScenarioCommand command : failureReplay.commands()) {
                commands.add(new Document()
                        .append("commandName", command.commandName())
                        .append("payload", toDocument(command.payload())));
            }
            failureReplays.add(new Document()
                    .append("scenarioId", failureReplay.scenarioId())
                    .append("status", failureReplay.status().name())
                    .append("message", failureReplay.message())
                    .append("commands", commands));
        }
        root.put("failureReplays", failureReplays);
        root.put("replayBundles", new Document()
                .append("dir", REPLAY_BUNDLE_DIR)
                .append("count", result.replayBundles().size())
                .append("manifest", REPLAY_BUNDLE_DIR + "/" + DeterministicReplayBundles.MANIFEST_FILE_NAME));

        return root.toJson();
    }

    String renderMarkdown(final RunResult result) {
        Objects.requireNonNull(result, "result");
        final DifferentialReport report = result.differentialReport();
        final StringBuilder markdown = new StringBuilder();
        markdown.append("# UTF Differential Report\n\n");
        markdown.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        markdown.append("- seed: ").append(result.seed())
                .append(" (numericSeed=").append(result.numericSeed()).append(")\n");
        markdown.append("- specRoot: ").append(result.config().specRoot()).append('\n');
        markdown.append("- importProfile: ").append(result.config().importProfile().cliValue()).append('\n');
        markdown.append("- backends: ").append(report.leftBackend()).append(" vs ").append(report.rightBackend()).append('\n');
        markdown.append('\n');

        markdown.append("## Breakdown\n\n");
        markdown.append("- imported: ").append(result.importResult().importedCount()).append('\n');
        markdown.append("- pass: ").append(report.matchCount()).append('\n');
        markdown.append("- fail(mismatch+error): ")
                .append(report.mismatchCount() + report.errorCount())
                .append('\n');
        markdown.append("- skipped: ").append(result.importResult().skippedCount()).append('\n');
        markdown.append("- unsupported: ").append(result.importResult().unsupportedCount()).append('\n');
        markdown.append('\n');

        markdown.append("## Failure Replays\n\n");
        if (result.failureReplays().isEmpty()) {
            markdown.append("- none\n");
        } else {
            for (final FailureReplay replay : result.failureReplays()) {
                markdown.append("- ").append(replay.scenarioId())
                        .append(" [").append(replay.status().name()).append("] ")
                        .append(replay.message())
                        .append('\n');
            }
        }
        markdown.append('\n');

        markdown.append("## Replay Bundles\n\n");
        markdown.append("- dir: ").append(REPLAY_BUNDLE_DIR).append('\n');
        markdown.append("- bundleCount: ").append(result.replayBundles().size()).append('\n');
        markdown.append("- manifest: ")
                .append(REPLAY_BUNDLE_DIR)
                .append('/')
                .append(DeterministicReplayBundles.MANIFEST_FILE_NAME)
                .append('\n');
        markdown.append('\n');

        markdown.append("## Skipped / Unsupported\n\n");
        if (result.importResult().skippedCases().isEmpty()) {
            markdown.append("- none\n");
        } else {
            for (final UnifiedSpecImporter.SkippedCase skippedCase : result.importResult().skippedCases()) {
                markdown.append("- ").append(skippedCase.caseId())
                        .append(" [").append(skippedCase.kind().name()).append("] ")
                        .append(skippedCase.reason())
                        .append('\n');
            }
        }
        return markdown.toString();
    }

    private static Document toDocument(final Map<String, Object> mapValue) {
        final Document document = new Document();
        for (final Map.Entry<String, Object> entry : mapValue.entrySet()) {
            document.put(entry.getKey(), toBsonFriendly(entry.getValue()));
        }
        return document;
    }

    private static Object toBsonFriendly(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> mapValue) {
            final Document document = new Document();
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                document.put(String.valueOf(entry.getKey()), toBsonFriendly(entry.getValue()));
            }
            return document;
        }
        if (value instanceof List<?> listValue) {
            final List<Object> converted = new ArrayList<>(listValue.size());
            for (final Object item : listValue) {
                converted.add(toBsonFriendly(item));
            }
            return List.copyOf(converted);
        }
        if (value instanceof Instant instant) {
            return instant.toString();
        }
        return value;
    }

    private static boolean containsHelpFlag(final String[] args) {
        for (final String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static void printUsage() {
        System.out.println("Usage: UnifiedSpecCorpusRunner [options]");
        System.out.println("  --spec-root=<path>         UTF spec root directory (default: testkit/specs/unified)");
        System.out.println("  --output-dir=<path>        Output directory (default: build/reports/unified-spec)");
        System.out.println("  --seed=<value>             Deterministic seed (default: utf-corpus-v1)");
        System.out.println("  --mongo-uri=<uri>          Real mongod URI (or env JONGODB_REAL_MONGOD_URI)");
        System.out.println("  --replay-limit=<n>         Number of failure replays to include (default: 20)");
        System.out.println("  --import-profile=<name>    Import profile: strict | compat (default: strict)");
        System.out.println("  --help, -h                 Show this help");
    }

    public record ArtifactPaths(Path jsonArtifact, Path markdownArtifact, Path replayBundleDir) {}

    public record FailureReplay(String scenarioId, DiffStatus status, String message, List<ScenarioCommand> commands) {
        public FailureReplay {
            scenarioId = requireText(scenarioId, "scenarioId");
            status = Objects.requireNonNull(status, "status");
            message = requireText(message, "message");
            commands = List.copyOf(Objects.requireNonNull(commands, "commands"));
        }
    }

    public record RunResult(
            RunConfig config,
            String seed,
            long numericSeed,
            Instant generatedAt,
            UnifiedSpecImporter.ImportResult importResult,
            DifferentialReport differentialReport,
            List<FailureReplay> failureReplays,
            List<DeterministicReplayBundles.Bundle> replayBundles) {
        public RunResult {
            config = Objects.requireNonNull(config, "config");
            seed = requireText(seed, "seed");
            generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            importResult = Objects.requireNonNull(importResult, "importResult");
            differentialReport = Objects.requireNonNull(differentialReport, "differentialReport");
            failureReplays = List.copyOf(Objects.requireNonNull(failureReplays, "failureReplays"));
            replayBundles = List.copyOf(Objects.requireNonNull(replayBundles, "replayBundles"));
        }
    }

    public record RunConfig(
            Path specRoot,
            Path outputDir,
            String seed,
            String mongoUri,
            int replayLimit,
            UnifiedSpecImporter.ImportProfile importProfile) {
        public RunConfig {
            specRoot = normalizePath(specRoot, "specRoot");
            outputDir = normalizePath(outputDir, "outputDir");
            seed = requireText(seed, "seed");
            mongoUri = requireText(mongoUri, "mongoUri");
            if (replayLimit <= 0) {
                throw new IllegalArgumentException("replayLimit must be > 0");
            }
            importProfile = Objects.requireNonNull(importProfile, "importProfile");
        }

        static RunConfig fromArgs(final String[] args) {
            Path specRoot = DEFAULT_SPEC_ROOT;
            Path outputDir = DEFAULT_OUTPUT_DIR;
            String seed = DEFAULT_SEED;
            String mongoUri = trimToNull(System.getenv(DEFAULT_MONGO_URI_ENV));
            int replayLimit = DEFAULT_REPLAY_LIMIT;
            UnifiedSpecImporter.ImportProfile importProfile = UnifiedSpecImporter.ImportProfile.STRICT;

            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--spec-root=")) {
                    specRoot = Path.of(requireText(valueAfterPrefix(arg, "--spec-root="), "spec-root"));
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(requireText(valueAfterPrefix(arg, "--output-dir="), "output-dir"));
                    continue;
                }
                if (arg.startsWith("--seed=")) {
                    seed = requireText(valueAfterPrefix(arg, "--seed="), "seed");
                    continue;
                }
                if (arg.startsWith("--mongo-uri=")) {
                    mongoUri = requireText(valueAfterPrefix(arg, "--mongo-uri="), "mongo-uri");
                    continue;
                }
                if (arg.startsWith("--replay-limit=")) {
                    replayLimit = parsePositiveInt(valueAfterPrefix(arg, "--replay-limit="), "replay-limit");
                    continue;
                }
                if (arg.startsWith("--import-profile=")) {
                    importProfile = UnifiedSpecImporter.ImportProfile.parse(
                            valueAfterPrefix(arg, "--import-profile="));
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
            if (mongoUri == null || mongoUri.isBlank()) {
                throw new IllegalArgumentException("mongo-uri must be provided (arg or " + DEFAULT_MONGO_URI_ENV + ")");
            }
            return new RunConfig(specRoot, outputDir, seed, mongoUri, replayLimit, importProfile);
        }
    }

    private static Path normalizePath(final Path path, final String fieldName) {
        Objects.requireNonNull(path, fieldName);
        return path.normalize();
    }

    private static int parsePositiveInt(final String value, final String fieldName) {
        final String normalized = requireText(value, fieldName);
        try {
            final int parsed = Integer.parseInt(normalized);
            if (parsed <= 0) {
                throw new IllegalArgumentException(fieldName + " must be > 0");
            }
            return parsed;
        } catch (final NumberFormatException error) {
            throw new IllegalArgumentException(fieldName + " must be an integer", error);
        }
    }

    private static String valueAfterPrefix(final String arg, final String prefix) {
        return arg.substring(prefix.length());
    }

    private static String requireText(final String value, final String fieldName) {
        final String normalized = trimToNull(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String trimToNull(final String value) {
        if (value == null) {
            return null;
        }
        final String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
