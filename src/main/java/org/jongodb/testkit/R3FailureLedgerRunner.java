package org.jongodb.testkit;

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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.Document;

/**
 * Generates deterministic R3 failure-ledger artifacts from official suite runs.
 */
public final class R3FailureLedgerRunner {
    private static final String DEFAULT_MONGO_URI_ENV = "JONGODB_REAL_MONGOD_URI";
    private static final Path DEFAULT_SPEC_REPO_ROOT = Path.of("third_party/mongodb-specs/.checkout/specifications");
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/r3-failure-ledger");
    private static final String DEFAULT_SEED = "r3-failure-ledger-v1";
    private static final int DEFAULT_REPLAY_LIMIT = 20;
    private static final String JSON_ARTIFACT_FILE = "r3-failure-ledger.json";
    private static final String MARKDOWN_ARTIFACT_FILE = "r3-failure-ledger.md";
    private static final Pattern CODE_PATTERN = Pattern.compile("code=([-]?[0-9]+)");

    private static final List<SuiteConfig> DEFAULT_SUITES = List.of(
            new SuiteConfig("crud-unified", "source/crud/tests/unified"),
            new SuiteConfig("transactions-unified", "source/transactions/tests/unified"),
            new SuiteConfig("sessions", "source/sessions/tests"));

    private final Clock clock;
    private final UnifiedSpecImporter importer;
    private final Supplier<DifferentialBackend> leftBackendFactory;
    private final Function<String, DifferentialBackend> rightBackendFactory;

    public R3FailureLedgerRunner() {
        this(
                Clock.systemUTC(),
                new UnifiedSpecImporter(UnifiedSpecImporter.ImportProfile.STRICT),
                () -> new WireCommandIngressBackend("wire-backend"),
                mongoUri -> new RealMongodBackend("real-mongod", mongoUri));
    }

    R3FailureLedgerRunner(
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

        final R3FailureLedgerRunner runner = new R3FailureLedgerRunner(
                Clock.systemUTC(),
                new UnifiedSpecImporter(config.importProfile()),
                () -> new WireCommandIngressBackend("wire-backend"),
                mongoUri -> new RealMongodBackend("real-mongod", mongoUri));
        final RunResult result = runner.runAndWrite(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("R3 failure ledger generated.");
        System.out.println("- generatedAt: " + result.generatedAt());
        System.out.println("- specRepoRoot: " + result.config().specRepoRoot());
        System.out.println("- importProfile: " + result.config().importProfile().cliValue());
        System.out.println("- failures: " + result.entries().size());
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());

        if (config.failOnFailures() && hasGateFailure(result)) {
            System.err.println("R3 failure ledger gate failed: failures or missing suites detected.");
            System.exit(2);
        }
    }

    public RunResult runAndWrite(final RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        final RunResult result = run(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());
        Files.createDirectories(config.outputDir());
        Files.writeString(paths.jsonArtifact(), renderJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.markdownArtifact(), renderMarkdown(result), StandardCharsets.UTF_8);
        return result;
    }

    public RunResult run(final RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        final UnifiedSpecCorpusRunner unifiedRunner = new UnifiedSpecCorpusRunner(
                clock,
                importer,
                leftBackendFactory,
                rightBackendFactory);

        final Instant generatedAt = clock.instant();
        final List<FailureLedgerEntry> entries = new ArrayList<>();
        final List<SuiteRunSummary> suiteSummaries = new ArrayList<>();

        for (final SuiteConfig suiteConfig : config.suites()) {
            final Path suiteSpecRoot = config.specRepoRoot().resolve(suiteConfig.relativeSpecRoot()).normalize();
            if (!Files.exists(suiteSpecRoot)) {
                suiteSummaries.add(SuiteRunSummary.missing(suiteConfig.id(), suiteConfig.relativeSpecRoot()));
                continue;
            }

            final UnifiedSpecCorpusRunner.RunConfig suiteRunConfig = new UnifiedSpecCorpusRunner.RunConfig(
                    suiteSpecRoot,
                    config.outputDir().resolve("suite-artifacts").resolve(suiteConfig.id()),
                    config.seed() + "-" + suiteConfig.id(),
                    config.mongoUri(),
                    config.replayLimit(),
                    config.importProfile());
            final UnifiedSpecCorpusRunner.RunResult suiteResult = unifiedRunner.run(suiteRunConfig);
            suiteSummaries.add(SuiteRunSummary.fromSuite(suiteConfig.id(), suiteConfig.relativeSpecRoot(), suiteResult));
            collectFailures(entries, suiteConfig.id(), suiteConfig.relativeSpecRoot(), suiteResult);
        }

        entries.sort(Comparator.comparing(FailureLedgerEntry::failureId));
        final Map<String, Integer> byTrack = countBy(entries, FailureLedgerEntry::track);
        final Map<String, Integer> byStatus = countBy(entries, entry -> entry.status().name());

        return new RunResult(
                config,
                generatedAt,
                List.copyOf(entries),
                List.copyOf(suiteSummaries),
                Map.copyOf(byTrack),
                Map.copyOf(byStatus));
    }

    static boolean hasGateFailure(final RunResult result) {
        Objects.requireNonNull(result, "result");
        if (!result.entries().isEmpty()) {
            return true;
        }
        for (final SuiteRunSummary suiteSummary : result.suiteSummaries()) {
            if (!"OK".equals(suiteSummary.status())) {
                return true;
            }
        }
        return false;
    }

    public static ArtifactPaths artifactPaths(final Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        final Path normalized = outputDir.normalize();
        return new ArtifactPaths(
                normalized.resolve(JSON_ARTIFACT_FILE),
                normalized.resolve(MARKDOWN_ARTIFACT_FILE));
    }

    private static void collectFailures(
            final List<FailureLedgerEntry> sink,
            final String suiteId,
            final String suiteRoot,
            final UnifiedSpecCorpusRunner.RunResult suiteResult) {
        final Map<String, UnifiedSpecImporter.ImportedScenario> byCaseId = new LinkedHashMap<>();
        for (final UnifiedSpecImporter.ImportedScenario importedScenario : suiteResult.importResult().importedScenarios()) {
            byCaseId.put(importedScenario.caseId(), importedScenario);
        }

        for (final DiffResult diffResult : suiteResult.differentialReport().results()) {
            if (diffResult.status() == DiffStatus.MATCH) {
                continue;
            }
            final UnifiedSpecImporter.ImportedScenario importedScenario = byCaseId.get(diffResult.scenarioId());
            final Scenario scenario = importedScenario == null ? null : importedScenario.scenario();
            final List<ScenarioCommand> commands = scenario == null ? List.of() : scenario.commands();
            final String sourcePath = importedScenario == null ? "" : importedScenario.sourcePath();
            final String track = classifyTrack(suiteId, sourcePath, commands);
            final String primaryCommand = commands.isEmpty() ? "unknown" : commands.get(0).commandName();
            final String firstDiffPath = diffResult.entries().isEmpty() ? null : diffResult.entries().get(0).path();
            final String message = failureMessage(diffResult);
            final String errorKey = buildErrorKey(diffResult, message);

            sink.add(new FailureLedgerEntry(
                    buildFailureId(suiteId, diffResult),
                    suiteId,
                    suiteRoot,
                    diffResult.scenarioId(),
                    sourcePath,
                    track,
                    diffResult.status(),
                    primaryCommand,
                    commandNames(commands),
                    firstDiffPath,
                    errorKey,
                    message));
        }
    }

    private static String buildFailureId(final String suiteId, final DiffResult diffResult) {
        return suiteId + "::" + diffResult.status().name().toLowerCase(Locale.ROOT) + "::" + diffResult.scenarioId();
    }

    private static String failureMessage(final DiffResult diffResult) {
        if (diffResult.status() == DiffStatus.ERROR) {
            return diffResult.errorMessage().orElse("unknown error");
        }
        if (diffResult.entries().isEmpty()) {
            return "mismatch without diff entries";
        }
        final DiffEntry first = diffResult.entries().get(0);
        final String note = first.note() == null ? "" : first.note();
        return first.path() + ": " + (note.isBlank() ? "value mismatch" : note);
    }

    private static String buildErrorKey(final DiffResult diffResult, final String message) {
        if (diffResult.status() != DiffStatus.ERROR) {
            return "mismatch";
        }
        final Matcher matcher = CODE_PATTERN.matcher(message);
        if (matcher.find()) {
            return "code:" + matcher.group(1);
        }
        return "error:unknown-code";
    }

    private static String classifyTrack(
            final String suiteId,
            final String sourcePath,
            final List<ScenarioCommand> commands) {
        final String suiteLower = suiteId.toLowerCase(Locale.ROOT);
        final String sourceLower = sourcePath == null ? "" : sourcePath.toLowerCase(Locale.ROOT);
        if (suiteLower.contains("transaction") || sourceLower.contains("/transactions/")) {
            return "txn";
        }

        boolean hasTxn = false;
        boolean hasAggregation = false;
        boolean hasDistinct = false;
        boolean hasQueryUpdate = false;
        for (final ScenarioCommand command : commands) {
            final String name = command.commandName().toLowerCase(Locale.ROOT);
            if (name.contains("transaction")) {
                hasTxn = true;
            }
            if ("aggregate".equals(name)) {
                hasAggregation = true;
            }
            if ("distinct".equals(name)) {
                hasDistinct = true;
            }
            if ("find".equals(name)
                    || name.startsWith("update")
                    || name.startsWith("delete")
                    || name.startsWith("insert")
                    || "findandmodify".equals(name)
                    || "replaceone".equals(name)) {
                hasQueryUpdate = true;
            }

            final Map<String, Object> payload = command.payload();
            if (payload.containsKey("txnNumber")
                    || payload.containsKey("startTransaction")
                    || "committransaction".equals(name)
                    || "aborttransaction".equals(name)) {
                hasTxn = true;
            }
        }
        if (hasTxn) {
            return "txn";
        }
        if (hasDistinct) {
            return "distinct";
        }
        if (hasAggregation) {
            return "aggregation";
        }
        if (hasQueryUpdate) {
            return "query_update";
        }
        return "protocol";
    }

    private static List<String> commandNames(final List<ScenarioCommand> commands) {
        final List<String> names = new ArrayList<>(commands.size());
        for (final ScenarioCommand command : commands) {
            names.add(command.commandName());
        }
        return List.copyOf(names);
    }

    private static Map<String, Integer> countBy(
            final List<FailureLedgerEntry> entries,
            final Function<FailureLedgerEntry, String> keyFunction) {
        final Map<String, Integer> counts = new TreeMap<>();
        for (final FailureLedgerEntry entry : entries) {
            final String key = keyFunction.apply(entry);
            counts.merge(key, 1, Integer::sum);
        }
        return counts;
    }

    String renderJson(final RunResult result) {
        final Document root = new Document();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("seed", result.config().seed());
        root.put("specRepoRoot", result.config().specRepoRoot().toString());
        root.put("importProfile", result.config().importProfile().cliValue());
        root.put("suiteCount", result.config().suites().size());
        root.put("failureCount", result.entries().size());
        root.put("byTrack", new Document(result.byTrack()));
        root.put("byStatus", new Document(result.byStatus()));

        final List<Document> suiteSummaryDocs = new ArrayList<>();
        for (final SuiteRunSummary suiteSummary : result.suiteSummaries()) {
            suiteSummaryDocs.add(new Document()
                    .append("suiteId", suiteSummary.suiteId())
                    .append("suiteRoot", suiteSummary.suiteRoot())
                    .append("status", suiteSummary.status())
                    .append("imported", suiteSummary.imported())
                    .append("skipped", suiteSummary.skipped())
                    .append("unsupported", suiteSummary.unsupported())
                    .append("total", suiteSummary.total())
                    .append("match", suiteSummary.match())
                    .append("mismatch", suiteSummary.mismatch())
                    .append("error", suiteSummary.error()));
        }
        root.put("suiteSummaries", suiteSummaryDocs);

        final List<Document> failureDocs = new ArrayList<>();
        for (final FailureLedgerEntry entry : result.entries()) {
            failureDocs.add(new Document()
                    .append("failureId", entry.failureId())
                    .append("suiteId", entry.suiteId())
                    .append("suiteRoot", entry.suiteRoot())
                    .append("scenarioId", entry.scenarioId())
                    .append("sourcePath", entry.sourcePath())
                    .append("track", entry.track())
                    .append("status", entry.status().name())
                    .append("primaryCommand", entry.primaryCommand())
                    .append("commandNames", entry.commandNames())
                    .append("firstDiffPath", entry.firstDiffPath())
                    .append("errorKey", entry.errorKey())
                    .append("message", entry.message()));
        }
        root.put("entries", failureDocs);
        return root.toJson();
    }

    String renderMarkdown(final RunResult result) {
        final StringBuilder markdown = new StringBuilder();
        markdown.append("# R3 Failure Ledger\n\n");
        markdown.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        markdown.append("- specRepoRoot: ").append(result.config().specRepoRoot()).append('\n');
        markdown.append("- seed: ").append(result.config().seed()).append('\n');
        markdown.append("- importProfile: ").append(result.config().importProfile().cliValue()).append('\n');
        markdown.append("- failureCount: ").append(result.entries().size()).append('\n');
        markdown.append('\n');

        markdown.append("## Track Breakdown\n\n");
        if (result.byTrack().isEmpty()) {
            markdown.append("- none\n\n");
        } else {
            for (final Map.Entry<String, Integer> entry : result.byTrack().entrySet()) {
                markdown.append("- ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
            }
            markdown.append('\n');
        }

        markdown.append("## Suite Summaries\n\n");
        for (final SuiteRunSummary suiteSummary : result.suiteSummaries()) {
            markdown.append("- ").append(suiteSummary.suiteId())
                    .append(" [").append(suiteSummary.status()).append("] ")
                    .append("imported=").append(suiteSummary.imported())
                    .append(", mismatch=").append(suiteSummary.mismatch())
                    .append(", error=").append(suiteSummary.error())
                    .append('\n');
        }
        markdown.append('\n');

        markdown.append("## Failure Entries\n\n");
        if (result.entries().isEmpty()) {
            markdown.append("- none\n");
        } else {
            for (final FailureLedgerEntry entry : result.entries()) {
                markdown.append("- failureId=").append(entry.failureId())
                        .append(" firstDiffPath=").append(emptyToPlaceholder(entry.firstDiffPath()))
                        .append(" track=").append(entry.track())
                        .append(" status=").append(entry.status().name())
                        .append(" primaryCommand=").append(entry.primaryCommand())
                        .append(" errorKey=").append(entry.errorKey())
                        .append(" message=").append(entry.message())
                        .append('\n');
            }
        }
        return markdown.toString();
    }

    private static String emptyToPlaceholder(final String value) {
        final String normalized = value == null ? "" : value.trim();
        return normalized.isEmpty() ? "<none>" : normalized;
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
        System.out.println("Usage: R3FailureLedgerRunner [options]");
        System.out.println("  --spec-repo-root=<path>    Official specs checkout root");
        System.out.println("  --output-dir=<path>        Output dir (default: build/reports/r3-failure-ledger)");
        System.out.println("  --seed=<value>             Deterministic seed");
        System.out.println("  --mongo-uri=<uri>          Real mongod URI (or env JONGODB_REAL_MONGOD_URI)");
        System.out.println("  --replay-limit=<n>         Failure replay capture limit per suite");
        System.out.println("  --import-profile=<name>    Import profile: strict | compat (default: strict)");
        System.out.println("  --suite=<id>:<root>        Additional suite mapping (repeatable)");
        System.out.println("  --fail-on-failures         Exit non-zero when failures or missing suites exist");
        System.out.println("  --no-fail-on-failures      Do not fail the process on ledger gate failure");
        System.out.println("  --help, -h                 Show help");
    }

    public record SuiteConfig(String id, String relativeSpecRoot) {
        public SuiteConfig {
            id = requireText(id, "suite.id");
            relativeSpecRoot = requireText(relativeSpecRoot, "suite.relativeSpecRoot");
        }
    }

    public record FailureLedgerEntry(
            String failureId,
            String suiteId,
            String suiteRoot,
            String scenarioId,
            String sourcePath,
            String track,
            DiffStatus status,
            String primaryCommand,
            List<String> commandNames,
            String firstDiffPath,
            String errorKey,
            String message) {
        public FailureLedgerEntry {
            failureId = requireText(failureId, "failureId");
            suiteId = requireText(suiteId, "suiteId");
            suiteRoot = requireText(suiteRoot, "suiteRoot");
            scenarioId = requireText(scenarioId, "scenarioId");
            sourcePath = sourcePath == null ? "" : sourcePath;
            track = requireText(track, "track");
            status = Objects.requireNonNull(status, "status");
            primaryCommand = requireText(primaryCommand, "primaryCommand");
            commandNames = List.copyOf(Objects.requireNonNull(commandNames, "commandNames"));
            firstDiffPath = firstDiffPath == null ? "" : firstDiffPath;
            errorKey = requireText(errorKey, "errorKey");
            message = requireText(message, "message");
        }
    }

    public record SuiteRunSummary(
            String suiteId,
            String suiteRoot,
            String status,
            int imported,
            int skipped,
            int unsupported,
            int total,
            int match,
            int mismatch,
            int error) {
        static SuiteRunSummary missing(final String suiteId, final String suiteRoot) {
            return new SuiteRunSummary(suiteId, suiteRoot, "MISSING", 0, 0, 0, 0, 0, 0, 0);
        }

        static SuiteRunSummary fromSuite(
                final String suiteId,
                final String suiteRoot,
                final UnifiedSpecCorpusRunner.RunResult runResult) {
            return new SuiteRunSummary(
                    suiteId,
                    suiteRoot,
                    "OK",
                    runResult.importResult().importedCount(),
                    runResult.importResult().skippedCount(),
                    runResult.importResult().unsupportedCount(),
                    runResult.differentialReport().totalScenarios(),
                    runResult.differentialReport().matchCount(),
                    runResult.differentialReport().mismatchCount(),
                    runResult.differentialReport().errorCount());
        }

        public SuiteRunSummary {
            suiteId = requireText(suiteId, "suiteId");
            suiteRoot = requireText(suiteRoot, "suiteRoot");
            status = requireText(status, "status");
        }
    }

    public record ArtifactPaths(Path jsonArtifact, Path markdownArtifact) {}

    public record RunResult(
            RunConfig config,
            Instant generatedAt,
            List<FailureLedgerEntry> entries,
            List<SuiteRunSummary> suiteSummaries,
            Map<String, Integer> byTrack,
            Map<String, Integer> byStatus) {
        public RunResult {
            config = Objects.requireNonNull(config, "config");
            generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
            suiteSummaries = List.copyOf(Objects.requireNonNull(suiteSummaries, "suiteSummaries"));
            byTrack = Map.copyOf(Objects.requireNonNull(byTrack, "byTrack"));
            byStatus = Map.copyOf(Objects.requireNonNull(byStatus, "byStatus"));
        }
    }

    public record RunConfig(
            Path specRepoRoot,
            Path outputDir,
            String seed,
            String mongoUri,
            int replayLimit,
            UnifiedSpecImporter.ImportProfile importProfile,
            boolean failOnFailures,
            List<SuiteConfig> suites) {
        public RunConfig {
            specRepoRoot = normalizePath(specRepoRoot, "specRepoRoot");
            outputDir = normalizePath(outputDir, "outputDir");
            seed = requireText(seed, "seed");
            mongoUri = requireText(mongoUri, "mongoUri");
            if (replayLimit <= 0) {
                throw new IllegalArgumentException("replayLimit must be > 0");
            }
            importProfile = Objects.requireNonNull(importProfile, "importProfile");
            suites = List.copyOf(Objects.requireNonNull(suites, "suites"));
            if (suites.isEmpty()) {
                throw new IllegalArgumentException("at least one suite is required");
            }
        }

        static RunConfig fromArgs(final String[] args) {
            Path specRepoRoot = DEFAULT_SPEC_REPO_ROOT;
            Path outputDir = DEFAULT_OUTPUT_DIR;
            String seed = DEFAULT_SEED;
            String mongoUri = trimToNull(System.getenv(DEFAULT_MONGO_URI_ENV));
            int replayLimit = DEFAULT_REPLAY_LIMIT;
            UnifiedSpecImporter.ImportProfile importProfile = UnifiedSpecImporter.ImportProfile.STRICT;
            boolean failOnFailures = false;
            final List<SuiteConfig> suites = new ArrayList<>(DEFAULT_SUITES);

            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--spec-repo-root=")) {
                    specRepoRoot = Path.of(requireText(valueAfterPrefix(arg, "--spec-repo-root="), "spec-repo-root"));
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
                if (arg.startsWith("--suite=")) {
                    final String raw = requireText(valueAfterPrefix(arg, "--suite="), "suite");
                    final int separator = raw.indexOf(':');
                    if (separator <= 0 || separator >= raw.length() - 1) {
                        throw new IllegalArgumentException("suite must be <id>:<relative-path>");
                    }
                    suites.add(new SuiteConfig(raw.substring(0, separator), raw.substring(separator + 1)));
                    continue;
                }
                if ("--fail-on-failures".equals(arg)) {
                    failOnFailures = true;
                    continue;
                }
                if ("--no-fail-on-failures".equals(arg)) {
                    failOnFailures = false;
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }

            if (mongoUri == null || mongoUri.isBlank()) {
                throw new IllegalArgumentException("mongo-uri must be provided (arg or " + DEFAULT_MONGO_URI_ENV + ")");
            }
            return new RunConfig(
                    specRepoRoot,
                    outputDir,
                    seed,
                    mongoUri,
                    replayLimit,
                    importProfile,
                    failOnFailures,
                    suites);
        }
    }

    private static int parsePositiveInt(final String value, final String fieldName) {
        try {
            final int parsed = Integer.parseInt(requireText(value, fieldName));
            if (parsed <= 0) {
                throw new IllegalArgumentException(fieldName + " must be > 0");
            }
            return parsed;
        } catch (final NumberFormatException exception) {
            throw new IllegalArgumentException(fieldName + " must be an integer", exception);
        }
    }

    private static Path normalizePath(final Path path, final String fieldName) {
        Objects.requireNonNull(path, fieldName);
        return path.normalize();
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
