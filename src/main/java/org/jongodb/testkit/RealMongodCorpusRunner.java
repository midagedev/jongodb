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
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Runs the differential corpus against wire backend vs real mongod and writes artifacts.
 */
public final class RealMongodCorpusRunner {
    private static final String DEFAULT_MONGO_URI_ENV = "JONGODB_REAL_MONGOD_URI";
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/real-mongod-baseline");
    private static final String DEFAULT_SEED = "wire-vs-real-mongod-baseline-v1";
    private static final int DEFAULT_TOP_REGRESSION_LIMIT = 10;

    private static final String BASELINE_JSON = "real-mongod-differential-baseline.json";
    private static final String BASELINE_MARKDOWN = "real-mongod-differential-baseline.md";

    private final Clock clock;
    private final Supplier<DifferentialBackend> wireBackendFactory;
    private final Function<String, DifferentialBackend> realBackendFactory;

    public RealMongodCorpusRunner() {
        this(Clock.systemUTC());
    }

    RealMongodCorpusRunner(Clock clock) {
        this(
            clock,
            () -> new WireCommandIngressBackend("wire-backend"),
            mongoUri -> new RealMongodBackend("real-mongod", mongoUri)
        );
    }

    RealMongodCorpusRunner(
        Clock clock,
        Supplier<DifferentialBackend> wireBackendFactory,
        Function<String, DifferentialBackend> realBackendFactory
    ) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.wireBackendFactory = Objects.requireNonNull(wireBackendFactory, "wireBackendFactory");
        this.realBackendFactory = Objects.requireNonNull(realBackendFactory, "realBackendFactory");
    }

    public static void main(String[] args) throws Exception {
        if (containsHelpFlag(args)) {
            printUsage();
            return;
        }

        final RunConfig config;
        try {
            config = RunConfig.fromArgs(args);
        } catch (IllegalArgumentException exception) {
            System.err.println("Invalid argument: " + exception.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        RealMongodCorpusRunner runner = new RealMongodCorpusRunner();
        RunResult result = runner.runAndWrite(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("Real mongod differential baseline generated.");
        System.out.println("- generatedAt: " + result.generatedAt());
        System.out.println("- seed: " + result.seed() + " (numericSeed=" + result.numericSeed() + ")");
        System.out.println("- backends: " + result.report().leftBackend() + " vs " + result.report().rightBackend());
        System.out.println("- total: " + result.report().totalScenarios());
        System.out.println("- match: " + result.report().matchCount());
        System.out.println("- mismatch: " + result.report().mismatchCount());
        System.out.println("- error: " + result.report().errorCount());
        System.out.println("- passRate: " + PassRate.from(result.report()).formatted());
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());
    }

    public RunResult runAndWrite(RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        RunResult result = run(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());
        Files.createDirectories(config.outputDir());
        Files.writeString(paths.jsonArtifact(), renderJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.markdownArtifact(), renderMarkdown(result), StandardCharsets.UTF_8);
        return result;
    }

    public RunResult run(RunConfig config) {
        Objects.requireNonNull(config, "config");
        List<Scenario> scenarios = buildScenarioCorpus(config.seed());
        DifferentialHarness harness = new DifferentialHarness(
            Objects.requireNonNull(wireBackendFactory.get(), "wireBackendFactory result"),
            Objects.requireNonNull(realBackendFactory.apply(config.mongoUri()), "realBackendFactory result"),
            clock
        );
        DifferentialReport report = harness.run(scenarios);
        long numericSeed = deterministicSeed(config.seed());
        List<RegressionSample> topRegressions = topRegressions(report, config.topRegressionLimit());
        return new RunResult(config.seed(), numericSeed, report.generatedAt(), report, topRegressions);
    }

    public static ArtifactPaths artifactPaths(Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        Path normalized = outputDir.normalize();
        return new ArtifactPaths(
            normalized.resolve(BASELINE_JSON),
            normalized.resolve(BASELINE_MARKDOWN)
        );
    }

    static List<Scenario> buildScenarioCorpus(String seed) {
        requireText(seed, "seed");
        List<Scenario> scenarios = new ArrayList<>();
        scenarios.addAll(CrudScenarioCatalog.scenarios());
        scenarios.addAll(TransactionScenarioCatalog.scenarios());
        scenarios.sort(Comparator.comparing(Scenario::id));
        Random random = new Random(deterministicSeed(seed));
        for (int i = scenarios.size() - 1; i > 0; i--) {
            int swapIndex = random.nextInt(i + 1);
            Scenario current = scenarios.get(i);
            scenarios.set(i, scenarios.get(swapIndex));
            scenarios.set(swapIndex, current);
        }
        return List.copyOf(scenarios);
    }

    static List<RegressionSample> topRegressions(DifferentialReport report, int limit) {
        Objects.requireNonNull(report, "report");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }

        List<DiffResult> regressions = new ArrayList<>();
        for (DiffResult result : report.results()) {
            if (result.status() == DiffStatus.MATCH) {
                continue;
            }
            regressions.add(result);
        }
        regressions.sort(
            Comparator
                .comparingInt((DiffResult result) -> statusPriority(result.status()))
                .reversed()
                .thenComparing(Comparator.comparingInt((DiffResult result) -> result.entries().size()).reversed())
                .thenComparing(DiffResult::scenarioId)
        );

        List<RegressionSample> samples = new ArrayList<>();
        for (int i = 0; i < regressions.size() && i < limit; i++) {
            DiffResult result = regressions.get(i);
            if (result.status() == DiffStatus.ERROR) {
                samples.add(
                    RegressionSample.error(
                        result.scenarioId(),
                        result.status(),
                        result.errorMessage().orElse("unknown error")
                    )
                );
                continue;
            }
            DiffEntry firstEntry = result.entries().isEmpty()
                ? null
                : result.entries().get(0);
            samples.add(
                RegressionSample.mismatch(
                    result.scenarioId(),
                    result.status(),
                    result.entries().size(),
                    firstEntry
                )
            );
        }
        return List.copyOf(samples);
    }

    static long deterministicSeed(String seed) {
        String normalized = requireText(seed, "seed");
        long hash = 0xcbf29ce484222325L; // FNV-1a 64-bit offset basis
        for (int i = 0; i < normalized.length(); i++) {
            hash ^= normalized.charAt(i);
            hash *= 0x100000001b3L; // FNV-1a 64-bit prime
        }
        return hash;
    }

    String renderMarkdown(RunResult result) {
        Objects.requireNonNull(result, "result");
        DifferentialReport report = result.report();
        StringBuilder sb = new StringBuilder();
        sb.append("# Real Mongod Differential Baseline\n\n");
        sb.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        sb.append("- seed: ").append(result.seed()).append('\n');
        sb.append("- numericSeed: ").append(result.numericSeed()).append('\n');
        sb.append("- backends: ").append(report.leftBackend()).append(" vs ").append(report.rightBackend()).append('\n');
        sb.append("- total: ").append(report.totalScenarios()).append('\n');
        sb.append("- match: ").append(report.matchCount()).append('\n');
        sb.append("- mismatch: ").append(report.mismatchCount()).append('\n');
        sb.append("- error: ").append(report.errorCount()).append('\n');
        sb.append("- passRate: ").append(PassRate.from(report).formatted()).append("\n\n");

        sb.append("## Top Regressions\n");
        if (result.topRegressions().isEmpty()) {
            sb.append("- none\n\n");
        } else {
            for (RegressionSample sample : result.topRegressions()) {
                sb.append("- ")
                    .append(sample.scenarioId())
                    .append(" (")
                    .append(sample.status())
                    .append(")");
                if (sample.status() == DiffStatus.ERROR) {
                    sb.append(": ").append(sample.errorMessage());
                } else {
                    sb.append(": ")
                        .append(sample.entryPath())
                        .append(" (")
                        .append(sample.entryNote())
                        .append(")");
                }
                sb.append('\n');
            }
            sb.append('\n');
        }

        sb.append("## Differential Details\n\n");
        sb.append(new DiffSummaryGenerator().toMarkdown(report));
        return sb.toString();
    }

    String renderJson(RunResult result) {
        Objects.requireNonNull(result, "result");
        DifferentialReport report = result.report();

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("seed", result.seed());
        root.put("numericSeed", result.numericSeed());
        root.put("leftBackend", report.leftBackend());
        root.put("rightBackend", report.rightBackend());

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total", report.totalScenarios());
        summary.put("match", report.matchCount());
        summary.put("mismatch", report.mismatchCount());
        summary.put("error", report.errorCount());
        summary.put("passRate", PassRate.from(report).ratio());
        root.put("summary", summary);

        List<Map<String, Object>> topRegressionItems = new ArrayList<>();
        for (RegressionSample sample : result.topRegressions()) {
            topRegressionItems.add(sample.toJsonMap());
        }
        root.put("topRegressions", topRegressionItems);
        root.put("report", toReportJson(report));

        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
    }

    private static Map<String, Object> toReportJson(DifferentialReport report) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", report.generatedAt().toString());
        root.put("leftBackend", report.leftBackend());
        root.put("rightBackend", report.rightBackend());

        List<Map<String, Object>> resultItems = new ArrayList<>();
        for (DiffResult result : report.results()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("scenarioId", result.scenarioId());
            item.put("status", result.status().name());
            item.put("errorMessage", result.errorMessage().orElse(null));

            List<Map<String, Object>> entryItems = new ArrayList<>();
            for (DiffEntry entry : result.entries()) {
                Map<String, Object> entryItem = new LinkedHashMap<>();
                entryItem.put("path", entry.path());
                entryItem.put("leftValue", entry.leftValue());
                entryItem.put("rightValue", entry.rightValue());
                entryItem.put("note", entry.note());
                entryItems.add(entryItem);
            }
            item.put("entries", entryItems);
            resultItems.add(item);
        }
        root.put("results", resultItems);
        return root;
    }

    private static boolean containsHelpFlag(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static void printUsage() {
        System.out.println("Usage: RealMongodCorpusRunner [options]");
        System.out.println("  --mongo-uri=<uri>         MongoDB connection URI (or env JONGODB_REAL_MONGOD_URI)");
        System.out.println("  --output-dir=<path>       Output directory for JSON/MD artifacts");
        System.out.println("  --seed=<text>             Deterministic corpus seed");
        System.out.println("  --top-regressions=<int>   Number of top regressions to extract");
        System.out.println("  --help                    Show this help message");
    }

    private static int statusPriority(DiffStatus status) {
        return switch (status) {
            case ERROR -> 2;
            case MISMATCH -> 1;
            case MATCH -> 0;
        };
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    public static final class ArtifactPaths {
        private final Path jsonArtifact;
        private final Path markdownArtifact;

        ArtifactPaths(Path jsonArtifact, Path markdownArtifact) {
            this.jsonArtifact = Objects.requireNonNull(jsonArtifact, "jsonArtifact");
            this.markdownArtifact = Objects.requireNonNull(markdownArtifact, "markdownArtifact");
        }

        public Path jsonArtifact() {
            return jsonArtifact;
        }

        public Path markdownArtifact() {
            return markdownArtifact;
        }
    }

    public static final class RunConfig {
        private final Path outputDir;
        private final String mongoUri;
        private final String seed;
        private final int topRegressionLimit;

        public RunConfig(Path outputDir, String mongoUri, String seed, int topRegressionLimit) {
            this.outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
            this.mongoUri = requireText(mongoUri, "mongoUri");
            this.seed = requireText(seed, "seed");
            if (topRegressionLimit <= 0) {
                throw new IllegalArgumentException("topRegressionLimit must be > 0");
            }
            this.topRegressionLimit = topRegressionLimit;
        }

        static RunConfig fromArgs(String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            String mongoUri = System.getenv(DEFAULT_MONGO_URI_ENV);
            String seed = DEFAULT_SEED;
            int topRegressionLimit = DEFAULT_TOP_REGRESSION_LIMIT;

            for (String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--mongo-uri=")) {
                    mongoUri = readValue(arg, "--mongo-uri=");
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(readValue(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--seed=")) {
                    seed = readValue(arg, "--seed=");
                    continue;
                }
                if (arg.startsWith("--top-regressions=")) {
                    topRegressionLimit = parseInt(readValue(arg, "--top-regressions="), "top-regressions");
                    continue;
                }
                throw new IllegalArgumentException("unknown option: " + arg);
            }

            if (mongoUri == null || mongoUri.isBlank()) {
                throw new IllegalArgumentException(
                    "mongo-uri is required (set --mongo-uri or " + DEFAULT_MONGO_URI_ENV + ")"
                );
            }
            return new RunConfig(outputDir, mongoUri, seed, topRegressionLimit);
        }

        public Path outputDir() {
            return outputDir;
        }

        public String mongoUri() {
            return mongoUri;
        }

        public String seed() {
            return seed;
        }

        public int topRegressionLimit() {
            return topRegressionLimit;
        }

        private static String readValue(String arg, String prefix) {
            String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " requires a value");
            }
            return value;
        }

        private static int parseInt(String value, String optionName) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(optionName + " must be an integer: " + value);
            }
        }
    }

    public static final class RunResult {
        private final String seed;
        private final long numericSeed;
        private final Instant generatedAt;
        private final DifferentialReport report;
        private final List<RegressionSample> topRegressions;

        RunResult(
            String seed,
            long numericSeed,
            Instant generatedAt,
            DifferentialReport report,
            List<RegressionSample> topRegressions
        ) {
            this.seed = requireText(seed, "seed");
            this.numericSeed = numericSeed;
            this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            this.report = Objects.requireNonNull(report, "report");
            this.topRegressions = List.copyOf(new ArrayList<>(Objects.requireNonNull(topRegressions, "topRegressions")));
        }

        public String seed() {
            return seed;
        }

        public long numericSeed() {
            return numericSeed;
        }

        public Instant generatedAt() {
            return generatedAt;
        }

        public DifferentialReport report() {
            return report;
        }

        public List<RegressionSample> topRegressions() {
            return topRegressions;
        }
    }

    public static final class RegressionSample {
        private final String scenarioId;
        private final DiffStatus status;
        private final String errorMessage;
        private final String entryPath;
        private final String entryNote;
        private final Object leftValue;
        private final Object rightValue;
        private final int entryCount;

        private RegressionSample(
            String scenarioId,
            DiffStatus status,
            String errorMessage,
            String entryPath,
            String entryNote,
            Object leftValue,
            Object rightValue,
            int entryCount
        ) {
            this.scenarioId = requireText(scenarioId, "scenarioId");
            this.status = Objects.requireNonNull(status, "status");
            this.errorMessage = normalize(errorMessage);
            this.entryPath = normalize(entryPath);
            this.entryNote = normalize(entryNote);
            this.leftValue = leftValue;
            this.rightValue = rightValue;
            this.entryCount = entryCount;
        }

        static RegressionSample error(String scenarioId, DiffStatus status, String errorMessage) {
            return new RegressionSample(scenarioId, status, errorMessage, null, null, null, null, 0);
        }

        static RegressionSample mismatch(String scenarioId, DiffStatus status, int entryCount, DiffEntry firstEntry) {
            if (entryCount <= 0) {
                throw new IllegalArgumentException("entryCount must be > 0 for mismatches");
            }
            if (firstEntry == null) {
                throw new IllegalArgumentException("firstEntry must not be null for mismatches");
            }
            return new RegressionSample(
                scenarioId,
                status,
                null,
                firstEntry.path(),
                firstEntry.note(),
                firstEntry.leftValue(),
                firstEntry.rightValue(),
                entryCount
            );
        }

        public String scenarioId() {
            return scenarioId;
        }

        public DiffStatus status() {
            return status;
        }

        public String errorMessage() {
            return errorMessage;
        }

        public String entryPath() {
            return entryPath;
        }

        public String entryNote() {
            return entryNote;
        }

        public Object leftValue() {
            return leftValue;
        }

        public Object rightValue() {
            return rightValue;
        }

        public int entryCount() {
            return entryCount;
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("scenarioId", scenarioId);
            item.put("status", status.name());
            item.put("errorMessage", errorMessage);
            item.put("entryPath", entryPath);
            item.put("entryNote", entryNote);
            item.put("leftValue", leftValue);
            item.put("rightValue", rightValue);
            item.put("entryCount", entryCount);
            return item;
        }

        private static String normalize(String value) {
            if (value == null) {
                return null;
            }
            String trimmed = value.trim();
            return trimmed.isEmpty() ? null : trimmed;
        }
    }
}
