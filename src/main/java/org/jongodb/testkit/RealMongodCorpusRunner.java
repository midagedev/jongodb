package org.jongodb.testkit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
    private static final int DEFAULT_SCENARIO_COUNT = 2_000;
    private static final int DEFAULT_MAX_MISMATCH = 0;
    private static final int DEFAULT_MAX_ERROR = 0;
    private static final boolean DEFAULT_FAIL_ON_GATE = true;

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
        System.out.println("- scenarioCount: " + config.scenarioCount());
        System.out.println("- backends: " + result.report().leftBackend() + " vs " + result.report().rightBackend());
        System.out.println("- total: " + result.report().totalScenarios());
        System.out.println("- match: " + result.report().matchCount());
        System.out.println("- mismatch: " + result.report().mismatchCount());
        System.out.println("- error: " + result.report().errorCount());
        System.out.println("- passRate: " + PassRate.from(result.report()).formatted());
        System.out.println("- gate: " + result.gateResult().status());
        System.out.println("- maxMismatch: " + result.gateResult().thresholds().maxMismatch());
        System.out.println("- maxError: " + result.gateResult().thresholds().maxError());
        if (result.gateResult().thresholds().minPassRate() != null) {
            System.out.println("- minPassRate: " + formatRatio(result.gateResult().thresholds().minPassRate()));
        }
        if (!result.gateResult().failureReasons().isEmpty()) {
            for (String failureReason : result.gateResult().failureReasons()) {
                System.out.println("- gateFailure: " + failureReason);
            }
        }
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());
        if (config.failOnGate() && result.gateResult().status() == QualityGateStatus.FAIL) {
            System.err.println("Real mongod differential baseline gate failed.");
            System.exit(2);
        }
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
        List<Scenario> scenarios = buildScenarioCorpus(config.seed(), config.scenarioCount());
        DifferentialHarness harness = new DifferentialHarness(
            Objects.requireNonNull(wireBackendFactory.get(), "wireBackendFactory result"),
            Objects.requireNonNull(realBackendFactory.apply(config.mongoUri()), "realBackendFactory result"),
            clock
        );
        DifferentialReport report = harness.run(scenarios);
        long numericSeed = deterministicSeed(config.seed());
        List<RegressionSample> topRegressions = topRegressions(report, config.topRegressionLimit());
        GateResult gateResult = evaluateGate(report, config.gateThresholds());
        return new RunResult(config.seed(), numericSeed, report.generatedAt(), report, topRegressions, gateResult);
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
        return buildScenarioCorpus(seed, DEFAULT_SCENARIO_COUNT);
    }

    static List<Scenario> buildScenarioCorpus(String seed, int scenarioCount) {
        requireText(seed, "seed");
        if (scenarioCount <= 0) {
            throw new IllegalArgumentException("scenarioCount must be > 0");
        }
        List<Scenario> scenarioTemplates = baseScenarioTemplates();
        List<Scenario> scenarios = expandScenarioTemplates(seed, scenarioTemplates, scenarioCount);
        Random random = new Random(deterministicSeed(seed));
        for (int i = scenarios.size() - 1; i > 0; i--) {
            int swapIndex = random.nextInt(i + 1);
            Scenario current = scenarios.get(i);
            scenarios.set(i, scenarios.get(swapIndex));
            scenarios.set(swapIndex, current);
        }
        return List.copyOf(scenarios.subList(0, scenarioCount));
    }

    private static List<Scenario> baseScenarioTemplates() {
        List<Scenario> scenarios = new ArrayList<>();
        scenarios.addAll(CrudScenarioCatalog.scenarios());
        scenarios.addAll(TransactionScenarioCatalog.scenarios());
        scenarios.sort(Comparator.comparing(Scenario::id));
        return scenarios;
    }

    private static List<Scenario> expandScenarioTemplates(
        String seed,
        List<Scenario> scenarioTemplates,
        int targetCount
    ) {
        List<Scenario> expanded = new ArrayList<>(Math.max(targetCount, scenarioTemplates.size()));
        expanded.addAll(scenarioTemplates);
        if (expanded.size() >= targetCount) {
            return expanded;
        }

        long numericSeed = deterministicSeed(seed);
        int variantIndex = 1;
        while (expanded.size() < targetCount) {
            for (Scenario template : scenarioTemplates) {
                if (expanded.size() >= targetCount) {
                    break;
                }
                expanded.add(variantScenario(template, variantIndex, numericSeed));
            }
            variantIndex++;
        }
        return expanded;
    }

    private static Scenario variantScenario(Scenario template, int variantIndex, long numericSeed) {
        String variantTag = String.format(Locale.ROOT, "v%04d", variantIndex);
        String variantId = variantTag + "." + template.id();
        String variantDescription = template.description() + " [variant " + variantTag + "]";

        List<ScenarioCommand> commands = new ArrayList<>(template.commands().size());
        for (ScenarioCommand command : template.commands()) {
            commands.add(variantCommand(command, variantIndex, variantTag, numericSeed));
        }
        return new Scenario(variantId, variantDescription, List.copyOf(commands));
    }

    private static ScenarioCommand variantCommand(
        ScenarioCommand command,
        int variantIndex,
        String variantTag,
        long numericSeed
    ) {
        Map<String, Object> payload = transformPayloadMap(
            command.payload(),
            null,
            variantIndex,
            variantTag,
            numericSeed
        );
        return new ScenarioCommand(command.commandName(), payload);
    }

    private static Map<String, Object> transformPayloadMap(
        Map<?, ?> source,
        String parentKey,
        int variantIndex,
        String variantTag,
        long numericSeed
    ) {
        Map<String, Object> transformed = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object value = transformPayloadValue(key, parentKey, entry.getValue(), variantIndex, variantTag, numericSeed);
            transformed.put(key, value);
        }
        return Collections.unmodifiableMap(transformed);
    }

    private static List<Object> transformPayloadList(
        List<?> source,
        String parentKey,
        int variantIndex,
        String variantTag,
        long numericSeed
    ) {
        List<Object> transformed = new ArrayList<>(source.size());
        for (Object value : source) {
            transformed.add(transformPayloadValue(null, parentKey, value, variantIndex, variantTag, numericSeed));
        }
        return Collections.unmodifiableList(transformed);
    }

    private static Object transformPayloadValue(
        String key,
        String parentKey,
        Object value,
        int variantIndex,
        String variantTag,
        long numericSeed
    ) {
        if (value instanceof Map<?, ?> mapValue) {
            return transformPayloadMap(mapValue, key, variantIndex, variantTag, numericSeed);
        }
        if (value instanceof List<?> listValue) {
            return transformPayloadList(listValue, key, variantIndex, variantTag, numericSeed);
        }
        if (value instanceof String stringValue) {
            return transformStringValue(key, parentKey, stringValue, variantTag);
        }
        if (value instanceof Number numberValue) {
            return transformNumericValue(key, numberValue, variantIndex, numericSeed);
        }
        return value;
    }

    private static Object transformStringValue(
        String key,
        String parentKey,
        String value,
        String variantTag
    ) {
        if ("collection".equals(key)) {
            return value + "_" + variantTag;
        }
        if ("id".equals(key) && "lsid".equals(parentKey)) {
            return value + "-" + variantTag;
        }
        if ("email".equals(key)) {
            int atIndex = value.indexOf('@');
            if (atIndex > 0 && atIndex < value.length() - 1) {
                return value.substring(0, atIndex) + "+" + variantTag + value.substring(atIndex);
            }
        }
        return value;
    }

    private static Object transformNumericValue(
        String key,
        Number value,
        int variantIndex,
        long numericSeed
    ) {
        if ("txnNumber".equals(key)) {
            return value.longValue() + (long) variantIndex;
        }
        if ("_id".equals(key)) {
            long seedOffset = Math.abs(numericSeed % 10_000L);
            return value.longValue() + (long) variantIndex * 100_000L + seedOffset;
        }
        return value;
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

    static GateResult evaluateGate(DifferentialReport report, GateThresholds thresholds) {
        Objects.requireNonNull(report, "report");
        Objects.requireNonNull(thresholds, "thresholds");

        List<String> failureReasons = new ArrayList<>();
        int mismatchCount = report.mismatchCount();
        int errorCount = report.errorCount();
        double passRate = PassRate.from(report).ratio();

        if (mismatchCount > thresholds.maxMismatch()) {
            failureReasons.add("mismatch threshold exceeded: " + mismatchCount + " > " + thresholds.maxMismatch());
        }
        if (errorCount > thresholds.maxError()) {
            failureReasons.add("error threshold exceeded: " + errorCount + " > " + thresholds.maxError());
        }
        if (thresholds.minPassRate() != null && passRate < thresholds.minPassRate()) {
            failureReasons.add(
                "passRate threshold not met: " + formatRatio(passRate) + " < " + formatRatio(thresholds.minPassRate())
            );
        }

        QualityGateStatus status = failureReasons.isEmpty() ? QualityGateStatus.PASS : QualityGateStatus.FAIL;
        return new GateResult(status, mismatchCount, errorCount, passRate, thresholds, List.copyOf(failureReasons));
    }

    private static String formatRatio(double ratio) {
        return String.format(Locale.ROOT, "%.4f", ratio);
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

        sb.append("## Gate\n");
        sb.append("- status: ").append(result.gateResult().status()).append('\n');
        sb.append("- measuredMismatch: ").append(result.gateResult().mismatchCount()).append('\n');
        sb.append("- measuredError: ").append(result.gateResult().errorCount()).append('\n');
        sb.append("- measuredPassRate: ").append(formatRatio(result.gateResult().passRate())).append('\n');
        sb.append("- maxMismatch: ").append(result.gateResult().thresholds().maxMismatch()).append('\n');
        sb.append("- maxError: ").append(result.gateResult().thresholds().maxError()).append('\n');
        if (result.gateResult().thresholds().minPassRate() == null) {
            sb.append("- minPassRate: none\n");
        } else {
            sb.append("- minPassRate: ").append(formatRatio(result.gateResult().thresholds().minPassRate())).append('\n');
        }
        if (result.gateResult().failureReasons().isEmpty()) {
            sb.append("- gateFailures: none\n\n");
        } else {
            sb.append("- gateFailures:\n");
            for (String failureReason : result.gateResult().failureReasons()) {
                sb.append("  - ").append(failureReason).append('\n');
            }
            sb.append('\n');
        }

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
        root.put("gate", result.gateResult().toJsonMap());

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
        System.out.println("  --scenario-count=<int>    Corpus size target (default: 2000)");
        System.out.println("  --top-regressions=<int>   Number of top regressions to extract");
        System.out.println("  --max-mismatch=<int>      Maximum allowed mismatches before gate fails (default: 0)");
        System.out.println("  --max-error=<int>         Maximum allowed errors before gate fails (default: 0)");
        System.out.println("  --min-pass-rate=<ratio>   Optional minimum pass rate threshold in [0.0, 1.0]");
        System.out.println("  --fail-on-gate            Exit non-zero when baseline gate fails (default)");
        System.out.println("  --no-fail-on-gate         Always exit zero");
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

    private static double requireRatio(double value, String fieldName) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(fieldName + " must be finite");
        }
        if (value < 0.0d || value > 1.0d) {
            throw new IllegalArgumentException(fieldName + " must be in range [0.0, 1.0]");
        }
        return value;
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

    public static final class GateThresholds {
        private final int maxMismatch;
        private final int maxError;
        private final Double minPassRate;

        public GateThresholds(int maxMismatch, int maxError, Double minPassRate) {
            if (maxMismatch < 0) {
                throw new IllegalArgumentException("maxMismatch must be >= 0");
            }
            if (maxError < 0) {
                throw new IllegalArgumentException("maxError must be >= 0");
            }
            if (minPassRate != null) {
                requireRatio(minPassRate, "minPassRate");
            }
            this.maxMismatch = maxMismatch;
            this.maxError = maxError;
            this.minPassRate = minPassRate;
        }

        public int maxMismatch() {
            return maxMismatch;
        }

        public int maxError() {
            return maxError;
        }

        public Double minPassRate() {
            return minPassRate;
        }
    }

    public static final class GateResult {
        private final QualityGateStatus status;
        private final int mismatchCount;
        private final int errorCount;
        private final double passRate;
        private final GateThresholds thresholds;
        private final List<String> failureReasons;

        GateResult(
            QualityGateStatus status,
            int mismatchCount,
            int errorCount,
            double passRate,
            GateThresholds thresholds,
            List<String> failureReasons
        ) {
            this.status = Objects.requireNonNull(status, "status");
            if (mismatchCount < 0) {
                throw new IllegalArgumentException("mismatchCount must be >= 0");
            }
            if (errorCount < 0) {
                throw new IllegalArgumentException("errorCount must be >= 0");
            }
            requireRatio(passRate, "passRate");
            this.mismatchCount = mismatchCount;
            this.errorCount = errorCount;
            this.passRate = passRate;
            this.thresholds = Objects.requireNonNull(thresholds, "thresholds");
            this.failureReasons = List.copyOf(new ArrayList<>(Objects.requireNonNull(failureReasons, "failureReasons")));
        }

        public QualityGateStatus status() {
            return status;
        }

        public int mismatchCount() {
            return mismatchCount;
        }

        public int errorCount() {
            return errorCount;
        }

        public double passRate() {
            return passRate;
        }

        public GateThresholds thresholds() {
            return thresholds;
        }

        public List<String> failureReasons() {
            return failureReasons;
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> thresholdMap = new LinkedHashMap<>();
            thresholdMap.put("maxMismatch", thresholds.maxMismatch());
            thresholdMap.put("maxError", thresholds.maxError());
            thresholdMap.put("minPassRate", thresholds.minPassRate());

            Map<String, Object> measured = new LinkedHashMap<>();
            measured.put("mismatch", mismatchCount);
            measured.put("error", errorCount);
            measured.put("passRate", passRate);

            Map<String, Object> root = new LinkedHashMap<>();
            root.put("status", status.name());
            root.put("thresholds", thresholdMap);
            root.put("measured", measured);
            root.put("failureReasons", failureReasons);
            return root;
        }
    }

    public static final class RunConfig {
        private final Path outputDir;
        private final String mongoUri;
        private final String seed;
        private final int scenarioCount;
        private final int topRegressionLimit;
        private final GateThresholds gateThresholds;
        private final boolean failOnGate;

        public RunConfig(Path outputDir, String mongoUri, String seed, int topRegressionLimit) {
            this(outputDir, mongoUri, seed, DEFAULT_SCENARIO_COUNT, topRegressionLimit);
        }

        public RunConfig(Path outputDir, String mongoUri, String seed, int scenarioCount, int topRegressionLimit) {
            this(
                outputDir,
                mongoUri,
                seed,
                scenarioCount,
                topRegressionLimit,
                new GateThresholds(DEFAULT_MAX_MISMATCH, DEFAULT_MAX_ERROR, null),
                DEFAULT_FAIL_ON_GATE
            );
        }

        public RunConfig(
            Path outputDir,
            String mongoUri,
            String seed,
            int scenarioCount,
            int topRegressionLimit,
            GateThresholds gateThresholds,
            boolean failOnGate
        ) {
            this.outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
            this.mongoUri = requireText(mongoUri, "mongoUri");
            this.seed = requireText(seed, "seed");
            if (scenarioCount <= 0) {
                throw new IllegalArgumentException("scenarioCount must be > 0");
            }
            this.scenarioCount = scenarioCount;
            if (topRegressionLimit <= 0) {
                throw new IllegalArgumentException("topRegressionLimit must be > 0");
            }
            this.topRegressionLimit = topRegressionLimit;
            this.gateThresholds = Objects.requireNonNull(gateThresholds, "gateThresholds");
            this.failOnGate = failOnGate;
        }

        static RunConfig fromArgs(String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            String mongoUri = System.getenv(DEFAULT_MONGO_URI_ENV);
            String seed = DEFAULT_SEED;
            int scenarioCount = DEFAULT_SCENARIO_COUNT;
            int topRegressionLimit = DEFAULT_TOP_REGRESSION_LIMIT;
            int maxMismatch = DEFAULT_MAX_MISMATCH;
            int maxError = DEFAULT_MAX_ERROR;
            Double minPassRate = null;
            boolean failOnGate = DEFAULT_FAIL_ON_GATE;

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
                if (arg.startsWith("--scenario-count=")) {
                    scenarioCount = parseInt(readValue(arg, "--scenario-count="), "scenario-count");
                    continue;
                }
                if (arg.startsWith("--top-regressions=")) {
                    topRegressionLimit = parseInt(readValue(arg, "--top-regressions="), "top-regressions");
                    continue;
                }
                if (arg.startsWith("--max-mismatch=")) {
                    maxMismatch = parseInt(readValue(arg, "--max-mismatch="), "max-mismatch");
                    continue;
                }
                if (arg.startsWith("--max-error=")) {
                    maxError = parseInt(readValue(arg, "--max-error="), "max-error");
                    continue;
                }
                if (arg.startsWith("--min-pass-rate=")) {
                    minPassRate = parseDouble(readValue(arg, "--min-pass-rate="), "min-pass-rate");
                    continue;
                }
                if ("--fail-on-gate".equals(arg)) {
                    failOnGate = true;
                    continue;
                }
                if ("--no-fail-on-gate".equals(arg)) {
                    failOnGate = false;
                    continue;
                }
                throw new IllegalArgumentException("unknown option: " + arg);
            }

            if (mongoUri == null || mongoUri.isBlank()) {
                throw new IllegalArgumentException(
                    "mongo-uri is required (set --mongo-uri or " + DEFAULT_MONGO_URI_ENV + ")"
                );
            }
            GateThresholds thresholds = new GateThresholds(maxMismatch, maxError, minPassRate);
            return new RunConfig(outputDir, mongoUri, seed, scenarioCount, topRegressionLimit, thresholds, failOnGate);
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

        public int scenarioCount() {
            return scenarioCount;
        }

        public int topRegressionLimit() {
            return topRegressionLimit;
        }

        public GateThresholds gateThresholds() {
            return gateThresholds;
        }

        public boolean failOnGate() {
            return failOnGate;
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

        private static double parseDouble(String value, String optionName) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(optionName + " must be a number: " + value);
            }
        }
    }

    public static final class RunResult {
        private final String seed;
        private final long numericSeed;
        private final Instant generatedAt;
        private final DifferentialReport report;
        private final List<RegressionSample> topRegressions;
        private final GateResult gateResult;

        RunResult(
            String seed,
            long numericSeed,
            Instant generatedAt,
            DifferentialReport report,
            List<RegressionSample> topRegressions,
            GateResult gateResult
        ) {
            this.seed = requireText(seed, "seed");
            this.numericSeed = numericSeed;
            this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            this.report = Objects.requireNonNull(report, "report");
            this.topRegressions = List.copyOf(new ArrayList<>(Objects.requireNonNull(topRegressions, "topRegressions")));
            this.gateResult = Objects.requireNonNull(gateResult, "gateResult");
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

        public GateResult gateResult() {
            return gateResult;
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
