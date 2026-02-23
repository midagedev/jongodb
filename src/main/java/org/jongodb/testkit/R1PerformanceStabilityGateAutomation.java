package org.jongodb.testkit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * R1 performance/stability gate automation.
 */
public final class R1PerformanceStabilityGateAutomation {
    private static final String COLD_START_GATE_ID = "cold-start";
    private static final String RESET_GATE_ID = "reset";
    private static final String CRUD_P95_GATE_ID = "crud-p95-latency";
    private static final String FLAKE_GATE_ID = "flake-rate";

    private static final String COLD_START_KEY = "coldStartMillis";
    private static final String RESET_KEY = "resetMillis";
    private static final String CRUD_P95_KEY = "crudP95LatencyMillis";
    private static final String THROUGHPUT_KEY = "throughputOpsPerSecond";
    private static final String FLAKE_KEY = "flakeRate";

    private static final double MAX_COLD_START_MILLIS = 150.0d;
    private static final double MAX_RESET_MILLIS = 10.0d;
    private static final double MAX_CRUD_P95_LATENCY_MILLIS = 5.0d;
    private static final double MAX_FLAKE_RATE = 0.002d;

    private static final int DEFAULT_FLAKE_RUNS = 20;
    private static final int DEFAULT_COLD_START_SAMPLES = 21;
    private static final int DEFAULT_RESET_SAMPLES = 21;
    private static final int DEFAULT_WARMUP_OPS = 100;
    private static final int DEFAULT_MEASURED_OPS = 500;
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/r1-gates");

    private static final String GATE_JSON = "r1-performance-stability-gate.json";
    private static final String GATE_MARKDOWN = "r1-performance-stability-gate.md";
    private static final String FLAKE_BASELINE_JSON = "r1-flake-baseline.json";
    private static final String FLAKE_BASELINE_MARKDOWN = "r1-flake-baseline.md";

    private final Clock clock;
    private final R1BenchmarkRunner benchmarkRunner;
    private final R1FlakeRateEvaluator flakeRateEvaluator;

    public R1PerformanceStabilityGateAutomation() {
        this(Clock.systemUTC(), new R1BenchmarkRunner(), new R1FlakeRateEvaluator());
    }

    R1PerformanceStabilityGateAutomation(
        Clock clock,
        R1BenchmarkRunner benchmarkRunner,
        R1FlakeRateEvaluator flakeRateEvaluator
    ) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.benchmarkRunner = Objects.requireNonNull(benchmarkRunner, "benchmarkRunner");
        this.flakeRateEvaluator = Objects.requireNonNull(flakeRateEvaluator, "flakeRateEvaluator");
    }

    public static void main(String[] args) throws Exception {
        if (containsHelpFlag(args)) {
            printUsage();
            return;
        }

        final EvidenceConfig config;
        try {
            config = EvidenceConfig.fromArgs(args);
        } catch (IllegalArgumentException exception) {
            System.err.println("Invalid argument: " + exception.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        R1PerformanceStabilityGateAutomation automation = new R1PerformanceStabilityGateAutomation();
        EvidenceResult result = automation.runAndWrite(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("R1 performance/stability gate evidence generated.");
        System.out.println("- overall: " + (result.overallPassed() ? "PASS" : "FAIL"));
        System.out.println("- coldStartMillis: " + formatMillis(result.coldStartMillis()));
        System.out.println("- resetMillis: " + formatMillis(result.resetMillis()));
        System.out.println("- crudP95LatencyMillis: " + formatMillis(result.crudP95LatencyMillis()));
        System.out.println("- throughputOpsPerSecond: " + formatThroughput(result.throughputOpsPerSecond()));
        System.out.println("- flakeRate: " + formatPercent(result.flakeRate()));
        System.out.println("- gateJson: " + paths.gateJson());
        System.out.println("- gateMarkdown: " + paths.gateMarkdown());
        System.out.println("- flakeBaselineJson: " + paths.flakeBaselineJson());
        System.out.println("- flakeBaselineMarkdown: " + paths.flakeBaselineMarkdown());

        if (config.failOnGate() && !result.overallPassed()) {
            System.err.println("R1 performance/stability gate failed.");
            System.exit(2);
        }
    }

    public EvidenceResult runAndWrite(EvidenceConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        EvidenceResult result = run(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());
        Files.createDirectories(config.outputDir());

        DiffSummaryGenerator diffSummaryGenerator = new DiffSummaryGenerator();
        Files.writeString(paths.gateJson(), renderGateJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.gateMarkdown(), renderGateMarkdown(result), StandardCharsets.UTF_8);
        Files.writeString(
            paths.flakeBaselineJson(),
            diffSummaryGenerator.toJson(result.flakeEvaluation().baselineReport()),
            StandardCharsets.UTF_8
        );
        Files.writeString(
            paths.flakeBaselineMarkdown(),
            diffSummaryGenerator.toMarkdown(result.flakeEvaluation().baselineReport()),
            StandardCharsets.UTF_8
        );
        return result;
    }

    public EvidenceResult run(EvidenceConfig config) {
        Objects.requireNonNull(config, "config");
        long startedAtNanos = System.nanoTime();

        R1BenchmarkRunner.BenchmarkResult benchmarkResult = benchmarkRunner.run(config.benchmarkConfig());
        R1FlakeRateEvaluator.FlakeEvaluation flakeEvaluation = flakeRateEvaluator.run(
            newHarness(),
            buildScenarios(),
            config.flakeRuns()
        );

        List<GateCheck> gateChecks = List.of(
            evaluateGate(
                COLD_START_GATE_ID,
                COLD_START_KEY,
                QualityGateOperator.LESS_OR_EQUAL,
                benchmarkResult.coldStartMillis(),
                MAX_COLD_START_MILLIS
            ),
            evaluateGate(
                RESET_GATE_ID,
                RESET_KEY,
                QualityGateOperator.LESS_OR_EQUAL,
                benchmarkResult.resetMillis(),
                MAX_RESET_MILLIS
            ),
            evaluateGate(
                CRUD_P95_GATE_ID,
                CRUD_P95_KEY,
                QualityGateOperator.LESS_OR_EQUAL,
                benchmarkResult.crudP95LatencyMillis(),
                MAX_CRUD_P95_LATENCY_MILLIS
            ),
            evaluateGate(
                FLAKE_GATE_ID,
                FLAKE_KEY,
                QualityGateOperator.LESS_OR_EQUAL,
                flakeEvaluation.summary().rate(),
                MAX_FLAKE_RATE
            )
        );

        long durationMillis = Math.max(0L, (System.nanoTime() - startedAtNanos) / 1_000_000L);
        return new EvidenceResult(Instant.now(clock), durationMillis, benchmarkResult, flakeEvaluation, gateChecks);
    }

    public static ArtifactPaths artifactPaths(Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        Path normalized = outputDir.normalize();
        return new ArtifactPaths(
            normalized.resolve(GATE_JSON),
            normalized.resolve(GATE_MARKDOWN),
            normalized.resolve(FLAKE_BASELINE_JSON),
            normalized.resolve(FLAKE_BASELINE_MARKDOWN)
        );
    }

    String renderGateMarkdown(EvidenceResult result) {
        Objects.requireNonNull(result, "result");
        StringBuilder sb = new StringBuilder();
        sb.append("# R1 Performance and Stability Gate\n\n");
        sb.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        sb.append("- overall: ").append(result.overallPassed() ? "PASS" : "FAIL").append('\n');
        sb.append("- pass: ").append(result.passCount()).append('\n');
        sb.append("- fail: ").append(result.failCount()).append('\n');
        sb.append("- durationMillis: ").append(result.durationMillis()).append("\n\n");

        sb.append("## Metrics\n");
        sb.append("- ").append(COLD_START_KEY).append(": ").append(formatMillis(result.coldStartMillis())).append('\n');
        sb.append("- ").append(RESET_KEY).append(": ").append(formatMillis(result.resetMillis())).append('\n');
        sb.append("- ").append(CRUD_P95_KEY).append(": ").append(formatMillis(result.crudP95LatencyMillis())).append('\n');
        sb.append("- ").append(THROUGHPUT_KEY).append(": ").append(formatThroughput(result.throughputOpsPerSecond())).append('\n');
        sb.append("- ").append(FLAKE_KEY).append(": ").append(formatPercent(result.flakeRate())).append("\n\n");

        sb.append("## Benchmarks\n");
        sb.append("- warmupOperations: ").append(result.benchmarkResult().warmupOperations()).append('\n');
        sb.append("- measuredOperations: ").append(result.benchmarkResult().measuredOperations()).append('\n');
        sb.append("- coldStartSamplesMillis: ").append(formatSampleList(result.benchmarkResult().coldStartSamplesMillis())).append('\n');
        sb.append("- resetSamplesMillis: ").append(formatSampleList(result.benchmarkResult().resetSamplesMillis())).append('\n');
        sb.append("- crudLatencySamplesMillis: ").append(formatSampleList(result.benchmarkResult().crudLatencySamplesMillis())).append("\n\n");

        R1FlakeRateEvaluator.FlakeSummary flakeSummary = result.flakeEvaluation().summary();
        sb.append("## Flake Evidence\n");
        sb.append("- runs: ").append(flakeSummary.runs()).append('\n');
        sb.append("- observations: ").append(flakeSummary.observations()).append('\n');
        sb.append("- flakyObservations: ").append(flakeSummary.flakyObservations()).append('\n');
        sb.append("- flakeRate: ").append(formatPercent(flakeSummary.rate())).append("\n\n");

        DifferentialReport baseline = result.flakeEvaluation().baselineReport();
        sb.append("## Differential Baseline\n");
        sb.append("- total: ").append(baseline.totalScenarios()).append('\n');
        sb.append("- match: ").append(baseline.matchCount()).append('\n');
        sb.append("- mismatch: ").append(baseline.mismatchCount()).append('\n');
        sb.append("- error: ").append(baseline.errorCount()).append("\n\n");

        sb.append("## Gates\n");
        for (GateCheck gateCheck : result.gateChecks()) {
            sb.append("- ")
                .append(gateCheck.gateId())
                .append(": ")
                .append(gateCheck.status())
                .append(" (")
                .append(gateCheck.metricKey())
                .append(' ')
                .append(formatMetricValue(gateCheck.metricKey(), gateCheck.measuredValue()))
                .append(' ')
                .append(gateCheck.operator().symbol())
                .append(' ')
                .append(formatMetricValue(gateCheck.metricKey(), gateCheck.thresholdValue()))
                .append(")\n");
        }
        return sb.toString();
    }

    String renderGateJson(EvidenceResult result) {
        Objects.requireNonNull(result, "result");
        DifferentialReport baseline = result.flakeEvaluation().baselineReport();
        R1FlakeRateEvaluator.FlakeSummary flakeSummary = result.flakeEvaluation().summary();

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("overallStatus", result.overallPassed() ? "PASS" : "FAIL");

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("pass", result.passCount());
        summary.put("fail", result.failCount());
        root.put("summary", summary);

        Map<String, Object> buildInfo = new LinkedHashMap<>();
        buildInfo.put("durationMillis", result.durationMillis());
        root.put("buildInfo", buildInfo);

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put(COLD_START_KEY, result.coldStartMillis());
        metrics.put(RESET_KEY, result.resetMillis());
        metrics.put(CRUD_P95_KEY, result.crudP95LatencyMillis());
        metrics.put(THROUGHPUT_KEY, result.throughputOpsPerSecond());
        metrics.put(FLAKE_KEY, result.flakeRate());
        root.put("metrics", metrics);

        Map<String, Object> benchmark = new LinkedHashMap<>();
        benchmark.put("warmupOperations", result.benchmarkResult().warmupOperations());
        benchmark.put("measuredOperations", result.benchmarkResult().measuredOperations());
        benchmark.put("coldStartSamplesMillis", result.benchmarkResult().coldStartSamplesMillis());
        benchmark.put("resetSamplesMillis", result.benchmarkResult().resetSamplesMillis());
        benchmark.put("crudLatencySamplesMillis", result.benchmarkResult().crudLatencySamplesMillis());
        root.put("benchmark", benchmark);

        Map<String, Object> flake = new LinkedHashMap<>();
        flake.put("runs", flakeSummary.runs());
        flake.put("observations", flakeSummary.observations());
        flake.put("flakyObservations", flakeSummary.flakyObservations());
        flake.put("rate", flakeSummary.rate());
        root.put("flake", flake);

        Map<String, Object> differential = new LinkedHashMap<>();
        differential.put("leftBackend", baseline.leftBackend());
        differential.put("rightBackend", baseline.rightBackend());
        differential.put("total", baseline.totalScenarios());
        differential.put("match", baseline.matchCount());
        differential.put("mismatch", baseline.mismatchCount());
        differential.put("error", baseline.errorCount());
        root.put("differentialBaseline", differential);

        List<Map<String, Object>> gates = new ArrayList<>();
        for (GateCheck gateCheck : result.gateChecks()) {
            Map<String, Object> gate = new LinkedHashMap<>();
            gate.put("gateId", gateCheck.gateId());
            gate.put("metricKey", gateCheck.metricKey());
            gate.put("measuredValue", gateCheck.measuredValue());
            gate.put("operator", gateCheck.operator().symbol());
            gate.put("thresholdValue", gateCheck.thresholdValue());
            gate.put("status", gateCheck.status().name());
            gates.add(gate);
        }
        root.put("gates", gates);

        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
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
        System.out.println("Usage: R1PerformanceStabilityGateAutomation [options]");
        System.out.println("  --output-dir=<path>            Output directory for JSON/MD artifacts");
        System.out.println("  --flake-runs=<int>             Differential reruns used for flake-rate estimation");
        System.out.println("  --cold-start-samples=<int>     Sample count for cold start benchmark");
        System.out.println("  --reset-samples=<int>          Sample count for reset benchmark");
        System.out.println("  --warmup-ops=<int>             Warm-up operations for CRUD benchmark");
        System.out.println("  --measured-ops=<int>           Measured operations for CRUD benchmark");
        System.out.println("  --fail-on-gate                 Exit non-zero when gate fails (default)");
        System.out.println("  --no-fail-on-gate              Always exit zero");
        System.out.println("  --help                         Show this help message");
    }

    private DifferentialHarness newHarness() {
        return new DifferentialHarness(
            new WireCommandIngressBackend("wire-left"),
            new WireCommandIngressBackend("wire-right"),
            clock
        );
    }

    private static List<Scenario> buildScenarios() {
        List<Scenario> scenarios = new ArrayList<>();
        scenarios.addAll(CrudScenarioCatalog.scenarios());
        scenarios.addAll(TransactionScenarioCatalog.scenarios());
        return List.copyOf(scenarios);
    }

    private static GateCheck evaluateGate(
        String gateId,
        String metricKey,
        QualityGateOperator operator,
        double measuredValue,
        double thresholdValue
    ) {
        QualityGateStatus status = operator.test(measuredValue, thresholdValue)
            ? QualityGateStatus.PASS
            : QualityGateStatus.FAIL;
        return new GateCheck(gateId, metricKey, operator, measuredValue, thresholdValue, status);
    }

    private static String formatMetricValue(String metricKey, double value) {
        return switch (metricKey) {
            case COLD_START_KEY, RESET_KEY, CRUD_P95_KEY -> formatMillis(value);
            case THROUGHPUT_KEY -> formatThroughput(value);
            case FLAKE_KEY -> formatPercent(value);
            default -> String.format(Locale.ROOT, "%.4f", value);
        };
    }

    private static String formatMillis(double millis) {
        return String.format(Locale.ROOT, "%.2fms", millis);
    }

    private static String formatThroughput(double throughputOpsPerSecond) {
        return String.format(Locale.ROOT, "%.2fops/s", throughputOpsPerSecond);
    }

    private static String formatPercent(double ratio) {
        return String.format(Locale.ROOT, "%.2f%%", ratio * 100.0d);
    }

    private static String formatSampleList(List<Double> samples) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < samples.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format(Locale.ROOT, "%.4f", samples.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    public static final class EvidenceConfig {
        private final Path outputDir;
        private final int flakeRuns;
        private final R1BenchmarkRunner.BenchmarkConfig benchmarkConfig;
        private final boolean failOnGate;

        public EvidenceConfig(
            Path outputDir,
            int flakeRuns,
            R1BenchmarkRunner.BenchmarkConfig benchmarkConfig,
            boolean failOnGate
        ) {
            this.outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
            if (flakeRuns < 0) {
                throw new IllegalArgumentException("flakeRuns must be >= 0");
            }
            this.flakeRuns = flakeRuns;
            this.benchmarkConfig = Objects.requireNonNull(benchmarkConfig, "benchmarkConfig");
            this.failOnGate = failOnGate;
        }

        public static EvidenceConfig fromArgs(String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            int flakeRuns = DEFAULT_FLAKE_RUNS;
            int coldStartSamples = DEFAULT_COLD_START_SAMPLES;
            int resetSamples = DEFAULT_RESET_SAMPLES;
            int warmupOps = DEFAULT_WARMUP_OPS;
            int measuredOps = DEFAULT_MEASURED_OPS;
            boolean failOnGate = true;

            for (String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(readValue(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--flake-runs=")) {
                    flakeRuns = parseInt(readValue(arg, "--flake-runs="), "flake-runs");
                    continue;
                }
                if (arg.startsWith("--cold-start-samples=")) {
                    coldStartSamples = parseInt(readValue(arg, "--cold-start-samples="), "cold-start-samples");
                    continue;
                }
                if (arg.startsWith("--reset-samples=")) {
                    resetSamples = parseInt(readValue(arg, "--reset-samples="), "reset-samples");
                    continue;
                }
                if (arg.startsWith("--warmup-ops=")) {
                    warmupOps = parseInt(readValue(arg, "--warmup-ops="), "warmup-ops");
                    continue;
                }
                if (arg.startsWith("--measured-ops=")) {
                    measuredOps = parseInt(readValue(arg, "--measured-ops="), "measured-ops");
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

            return new EvidenceConfig(
                outputDir,
                flakeRuns,
                new R1BenchmarkRunner.BenchmarkConfig(coldStartSamples, resetSamples, warmupOps, measuredOps),
                failOnGate
            );
        }

        public Path outputDir() {
            return outputDir;
        }

        public int flakeRuns() {
            return flakeRuns;
        }

        public R1BenchmarkRunner.BenchmarkConfig benchmarkConfig() {
            return benchmarkConfig;
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
    }

    public static final class ArtifactPaths {
        private final Path gateJson;
        private final Path gateMarkdown;
        private final Path flakeBaselineJson;
        private final Path flakeBaselineMarkdown;

        ArtifactPaths(
            Path gateJson,
            Path gateMarkdown,
            Path flakeBaselineJson,
            Path flakeBaselineMarkdown
        ) {
            this.gateJson = Objects.requireNonNull(gateJson, "gateJson");
            this.gateMarkdown = Objects.requireNonNull(gateMarkdown, "gateMarkdown");
            this.flakeBaselineJson = Objects.requireNonNull(flakeBaselineJson, "flakeBaselineJson");
            this.flakeBaselineMarkdown = Objects.requireNonNull(flakeBaselineMarkdown, "flakeBaselineMarkdown");
        }

        public Path gateJson() {
            return gateJson;
        }

        public Path gateMarkdown() {
            return gateMarkdown;
        }

        public Path flakeBaselineJson() {
            return flakeBaselineJson;
        }

        public Path flakeBaselineMarkdown() {
            return flakeBaselineMarkdown;
        }
    }

    public static final class GateCheck {
        private final String gateId;
        private final String metricKey;
        private final QualityGateOperator operator;
        private final double measuredValue;
        private final double thresholdValue;
        private final QualityGateStatus status;

        GateCheck(
            String gateId,
            String metricKey,
            QualityGateOperator operator,
            double measuredValue,
            double thresholdValue,
            QualityGateStatus status
        ) {
            this.gateId = requireText(gateId, "gateId");
            this.metricKey = requireText(metricKey, "metricKey");
            this.operator = Objects.requireNonNull(operator, "operator");
            this.measuredValue = requireFiniteNonNegative(measuredValue, "measuredValue");
            this.thresholdValue = requireFiniteNonNegative(thresholdValue, "thresholdValue");
            this.status = Objects.requireNonNull(status, "status");
        }

        public String gateId() {
            return gateId;
        }

        public String metricKey() {
            return metricKey;
        }

        public QualityGateOperator operator() {
            return operator;
        }

        public double measuredValue() {
            return measuredValue;
        }

        public double thresholdValue() {
            return thresholdValue;
        }

        public QualityGateStatus status() {
            return status;
        }

        public boolean passed() {
            return status == QualityGateStatus.PASS;
        }
    }

    public static final class EvidenceResult {
        private final Instant generatedAt;
        private final long durationMillis;
        private final R1BenchmarkRunner.BenchmarkResult benchmarkResult;
        private final R1FlakeRateEvaluator.FlakeEvaluation flakeEvaluation;
        private final List<GateCheck> gateChecks;

        EvidenceResult(
            Instant generatedAt,
            long durationMillis,
            R1BenchmarkRunner.BenchmarkResult benchmarkResult,
            R1FlakeRateEvaluator.FlakeEvaluation flakeEvaluation,
            List<GateCheck> gateChecks
        ) {
            this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            if (durationMillis < 0L) {
                throw new IllegalArgumentException("durationMillis must be >= 0");
            }
            this.durationMillis = durationMillis;
            this.benchmarkResult = Objects.requireNonNull(benchmarkResult, "benchmarkResult");
            this.flakeEvaluation = Objects.requireNonNull(flakeEvaluation, "flakeEvaluation");
            this.gateChecks = copyGateChecks(gateChecks);
        }

        public Instant generatedAt() {
            return generatedAt;
        }

        public long durationMillis() {
            return durationMillis;
        }

        public R1BenchmarkRunner.BenchmarkResult benchmarkResult() {
            return benchmarkResult;
        }

        public R1FlakeRateEvaluator.FlakeEvaluation flakeEvaluation() {
            return flakeEvaluation;
        }

        public List<GateCheck> gateChecks() {
            return gateChecks;
        }

        public double coldStartMillis() {
            return benchmarkResult.coldStartMillis();
        }

        public double resetMillis() {
            return benchmarkResult.resetMillis();
        }

        public double crudP95LatencyMillis() {
            return benchmarkResult.crudP95LatencyMillis();
        }

        public double throughputOpsPerSecond() {
            return benchmarkResult.throughputOpsPerSecond();
        }

        public double flakeRate() {
            return flakeEvaluation.summary().rate();
        }

        public int passCount() {
            int pass = 0;
            for (GateCheck gateCheck : gateChecks) {
                if (gateCheck.passed()) {
                    pass++;
                }
            }
            return pass;
        }

        public int failCount() {
            return gateChecks.size() - passCount();
        }

        public boolean overallPassed() {
            return failCount() == 0;
        }

        private static List<GateCheck> copyGateChecks(List<GateCheck> source) {
            Objects.requireNonNull(source, "gateChecks");
            List<GateCheck> copy = new ArrayList<>(source.size());
            for (GateCheck gateCheck : source) {
                copy.add(Objects.requireNonNull(gateCheck, "gateCheck"));
            }
            return List.copyOf(copy);
        }
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static double requireFiniteNonNegative(double value, String fieldName) {
        if (!Double.isFinite(value) || value < 0.0d) {
            throw new IllegalArgumentException(fieldName + " must be finite and >= 0.0");
        }
        return value;
    }
}
