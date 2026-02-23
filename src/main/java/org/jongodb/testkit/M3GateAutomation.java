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
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jongodb.server.WireCommandIngress;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

/**
 * M3 release-readiness gate automation.
 *
 * <p>Computes compatibility pass rate, flake rate, and repro time P50, then writes
 * JSON/Markdown evidence artifacts under a deterministic output directory.
 */
public final class M3GateAutomation {
    private static final String COMPATIBILITY_GATE_ID = "compatibility-pass-rate";
    private static final String FLAKE_GATE_ID = "flake-rate";
    private static final String REPRO_GATE_ID = "repro-time-p50";

    private static final String COMPATIBILITY_KEY = "compatibilityPassRate";
    private static final String FLAKE_KEY = "flakeRate";
    private static final String REPRO_KEY = "reproTimeP50Minutes";

    private static final double MIN_COMPATIBILITY_PASS_RATE = 0.95d;
    private static final double MAX_FLAKE_RATE = 0.005d;
    private static final double MAX_REPRO_TIME_P50_MINUTES = 5.0d;

    private static final int DEFAULT_FLAKE_RUNS = 30;
    private static final int DEFAULT_REPRO_SAMPLES = 21;
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/m3-gate");
    private static final String EVIDENCE_SEED = "crud+transaction-catalog-v1";

    private static final String RELEASE_READINESS_JSON = "m3-release-readiness.json";
    private static final String RELEASE_READINESS_MARKDOWN = "m3-release-readiness.md";
    private static final String COMPATIBILITY_JSON = "m3-compatibility-report.json";
    private static final String COMPATIBILITY_MARKDOWN = "m3-compatibility-report.md";

    private static final BsonDocument REPRO_SUCCESS_COMMAND = BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}");
    private static final BsonDocument REPRO_FAILURE_COMMAND = BsonDocument.parse(
        "{\"doesNotExist\": 1, \"$db\": \"admin\", \"lsid\": {\"id\": \"m3-repro\"}, \"txnNumber\": 1}"
    );

    private final Clock clock;

    public M3GateAutomation() {
        this(Clock.systemUTC());
    }

    M3GateAutomation(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
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

        M3GateAutomation automation = new M3GateAutomation();
        EvidenceResult result = automation.runAndWrite(config);
        ArtifactPaths artifactPaths = artifactPaths(config.outputDir());

        System.out.println("M3 release readiness evidence generated.");
        System.out.println("- overall: " + (result.overallPassed() ? "PASS" : "FAIL"));
        System.out.println("- compatibilityPassRate: " + formatPercent(result.compatibilityPassRate()));
        System.out.println("- flakeRate: " + formatPercent(result.flakeRate()));
        System.out.println("- reproTimeP50Minutes: " + formatMinutes(result.reproTimeP50Minutes()));
        System.out.println("- releaseReadinessJson: " + artifactPaths.releaseReadinessJson());
        System.out.println("- releaseReadinessMarkdown: " + artifactPaths.releaseReadinessMarkdown());
        System.out.println("- compatibilityJson: " + artifactPaths.compatibilityJson());
        System.out.println("- compatibilityMarkdown: " + artifactPaths.compatibilityMarkdown());

        if (config.failOnGate() && !result.overallPassed()) {
            System.err.println("M3 release readiness gate failed.");
            System.exit(2);
        }
    }

    public EvidenceResult runAndWrite(EvidenceConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        EvidenceResult result = run(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());
        Files.createDirectories(config.outputDir());

        DiffSummaryGenerator diffSummaryGenerator = new DiffSummaryGenerator();
        Files.writeString(paths.releaseReadinessJson(), renderReleaseReadinessJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.releaseReadinessMarkdown(), renderReleaseReadinessMarkdown(result), StandardCharsets.UTF_8);
        Files.writeString(paths.compatibilityJson(), diffSummaryGenerator.toJson(result.compatibilityReport()), StandardCharsets.UTF_8);
        Files.writeString(
            paths.compatibilityMarkdown(),
            diffSummaryGenerator.toMarkdown(result.compatibilityReport()),
            StandardCharsets.UTF_8
        );
        return result;
    }

    public EvidenceResult run(EvidenceConfig config) {
        Objects.requireNonNull(config, "config");
        long startedAtNanos = System.nanoTime();

        List<Scenario> scenarios = buildScenarios();
        DifferentialHarness harness = newHarness();
        DifferentialReport compatibilityReport = harness.run(scenarios);
        double compatibilityPassRate = PassRate.from(compatibilityReport).ratio();

        List<DifferentialReport> rerunReports = new ArrayList<>(config.flakeRuns());
        for (int i = 0; i < config.flakeRuns(); i++) {
            rerunReports.add(harness.run(scenarios));
        }
        FlakeSummary flakeSummary = computeFlakeSummary(compatibilityReport, rerunReports);
        ReproSummary reproSummary = measureReproSummary(config.reproSamples());

        List<GateCheck> gateChecks = List.of(
            evaluateGate(
                COMPATIBILITY_GATE_ID,
                COMPATIBILITY_KEY,
                QualityGateOperator.GREATER_OR_EQUAL,
                compatibilityPassRate,
                MIN_COMPATIBILITY_PASS_RATE
            ),
            evaluateGate(
                FLAKE_GATE_ID,
                FLAKE_KEY,
                QualityGateOperator.LESS_OR_EQUAL,
                flakeSummary.rate(),
                MAX_FLAKE_RATE
            ),
            evaluateGate(
                REPRO_GATE_ID,
                REPRO_KEY,
                QualityGateOperator.LESS_OR_EQUAL,
                reproSummary.p50Minutes(),
                MAX_REPRO_TIME_P50_MINUTES
            )
        );

        long durationMillis = Math.max(0L, (System.nanoTime() - startedAtNanos) / 1_000_000L);
        return new EvidenceResult(
            Instant.now(clock),
            durationMillis,
            compatibilityReport,
            compatibilityPassRate,
            flakeSummary,
            reproSummary,
            gateChecks
        );
    }

    public static ArtifactPaths artifactPaths(Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        Path normalizedDir = outputDir.normalize();
        return new ArtifactPaths(
            normalizedDir.resolve(RELEASE_READINESS_JSON),
            normalizedDir.resolve(RELEASE_READINESS_MARKDOWN),
            normalizedDir.resolve(COMPATIBILITY_JSON),
            normalizedDir.resolve(COMPATIBILITY_MARKDOWN)
        );
    }

    static FlakeSummary computeFlakeSummary(DifferentialReport baselineReport, List<DifferentialReport> rerunReports) {
        Objects.requireNonNull(baselineReport, "baselineReport");
        Objects.requireNonNull(rerunReports, "rerunReports");

        Map<String, String> baselineFingerprints = new LinkedHashMap<>();
        for (DiffResult result : baselineReport.results()) {
            baselineFingerprints.put(result.scenarioId(), fingerprint(result));
        }

        int observations = 0;
        int flakyObservations = 0;
        for (DifferentialReport rerunReport : rerunReports) {
            Objects.requireNonNull(rerunReport, "rerunReport");
            for (DiffResult result : rerunReport.results()) {
                observations++;
                String baselineFingerprint = baselineFingerprints.get(result.scenarioId());
                if (baselineFingerprint == null) {
                    flakyObservations++;
                    continue;
                }
                if (!baselineFingerprint.equals(fingerprint(result))) {
                    flakyObservations++;
                }
            }
        }

        double flakeRate = observations == 0 ? 0.0d : (double) flakyObservations / (double) observations;
        return new FlakeSummary(rerunReports.size(), observations, flakyObservations, flakeRate);
    }

    static double percentile(List<Double> samples, double percentile) {
        Objects.requireNonNull(samples, "samples");
        if (percentile <= 0.0d || percentile > 1.0d || !Double.isFinite(percentile)) {
            throw new IllegalArgumentException("percentile must be in range (0.0, 1.0]");
        }
        if (samples.isEmpty()) {
            return 0.0d;
        }
        List<Double> sortedSamples = new ArrayList<>(samples.size());
        for (Double value : samples) {
            if (value == null || !Double.isFinite(value)) {
                throw new IllegalArgumentException("samples must contain finite numbers only");
            }
            sortedSamples.add(value);
        }
        sortedSamples.sort(Double::compareTo);
        int index = (int) Math.ceil(sortedSamples.size() * percentile) - 1;
        int boundedIndex = Math.max(0, Math.min(sortedSamples.size() - 1, index));
        return sortedSamples.get(boundedIndex);
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
        System.out.println("Usage: M3GateAutomation [options]");
        System.out.println("  --output-dir=<path>       Output directory for JSON/MD artifacts");
        System.out.println("  --flake-runs=<int>        Additional reruns used for flake-rate estimation");
        System.out.println("  --repro-samples=<int>     Sample count used for repro-time P50 estimation");
        System.out.println("  --fail-on-gate            Exit non-zero when the gate fails (default)");
        System.out.println("  --no-fail-on-gate         Always exit zero even when the gate fails");
        System.out.println("  --help                    Show this help message");
    }

    private DifferentialHarness newHarness() {
        return new DifferentialHarness(
            new WireCommandIngressBackend("wire-left"),
            new WireCommandIngressBackend("wire-right"),
            clock
        );
    }

    private ReproSummary measureReproSummary(int sampleCount) {
        List<Double> samples = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            samples.add(measureSingleReproTimeMinutes(i));
        }
        double p50 = percentile(samples, 0.50d);
        return new ReproSummary(sampleCount, samples, p50);
    }

    private double measureSingleReproTimeMinutes(int sampleIndex) {
        WireCommandIngress ingress = WireCommandIngress.inMemory();
        OpMsgCodec codec = new OpMsgCodec();
        int requestBase = 10_000 + (sampleIndex * 10);

        roundTrip(ingress, codec, requestBase, REPRO_SUCCESS_COMMAND);
        OpMsg failureResponse = roundTrip(ingress, codec, requestBase + 1, REPRO_FAILURE_COMMAND);
        if (!isFailure(failureResponse.body())) {
            throw new IllegalStateException("failed to produce repro seed command");
        }

        String reproJsonLines = ingress.exportReproJsonLines();
        if (reproJsonLines == null || reproJsonLines.isBlank()) {
            throw new IllegalStateException("repro exporter produced no commands");
        }
        String[] lines = reproJsonLines.split("\\R");

        WireCommandIngress replayIngress = WireCommandIngress.inMemory();
        long startedAtNanos = System.nanoTime();
        OpMsg replayLastResponse = null;
        int replayRequestId = requestBase + 100;
        for (String line : lines) {
            if (line == null || line.isBlank()) {
                continue;
            }
            replayLastResponse = roundTrip(replayIngress, codec, replayRequestId++, BsonDocument.parse(line));
        }
        long elapsedNanos = System.nanoTime() - startedAtNanos;

        if (replayLastResponse == null || !isFailure(replayLastResponse.body())) {
            throw new IllegalStateException("replay did not reproduce a failing command");
        }
        return elapsedNanos / 60_000_000_000.0d;
    }

    private static OpMsg roundTrip(WireCommandIngress ingress, OpMsgCodec codec, int requestId, BsonDocument body) {
        OpMsg request = new OpMsg(requestId, 0, 0, body.clone());
        byte[] responseBytes = ingress.handle(codec.encode(request));
        return codec.decode(responseBytes);
    }

    private static boolean isFailure(BsonDocument responseBody) {
        BsonValue okValue = responseBody.get("ok");
        if (okValue == null || !okValue.isNumber()) {
            return true;
        }
        return okValue.asNumber().doubleValue() != 1.0d;
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

    private static String fingerprint(DiffResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append(result.status().name());
        result.errorMessage().ifPresent(message -> sb.append("|error=").append(message));
        for (DiffEntry entry : result.entries()) {
            sb.append("|path=").append(entry.path());
            sb.append("|left=").append(QualityGateArtifactRenderer.JsonEncoder.encode(entry.leftValue()));
            sb.append("|right=").append(QualityGateArtifactRenderer.JsonEncoder.encode(entry.rightValue()));
            sb.append("|note=").append(entry.note());
        }
        return sb.toString();
    }

    private String renderReleaseReadinessMarkdown(EvidenceResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("# M3 Release Readiness Gate\n\n");
        sb.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        sb.append("- overall: ").append(result.overallPassed() ? "PASS" : "FAIL").append('\n');
        sb.append("- pass: ").append(result.passCount()).append('\n');
        sb.append("- fail: ").append(result.failCount()).append('\n');
        sb.append("- durationMillis: ").append(result.durationMillis()).append('\n');
        sb.append("- seed: ").append(EVIDENCE_SEED).append("\n\n");

        sb.append("## Metrics\n");
        for (GateCheck gateCheck : result.gateChecks()) {
            sb.append("- ")
                .append(gateCheck.metricKey())
                .append(": ")
                .append(formatMetricValue(gateCheck.metricKey(), gateCheck.measuredValue()))
                .append(" (")
                .append(gateCheck.operator().symbol())
                .append(' ')
                .append(formatMetricValue(gateCheck.metricKey(), gateCheck.thresholdValue()))
                .append(") ")
                .append(gateCheck.status())
                .append('\n');
        }
        sb.append('\n');

        DifferentialReport compatibilityReport = result.compatibilityReport();
        sb.append("## Compatibility Summary\n");
        sb.append("- total: ").append(compatibilityReport.totalScenarios()).append('\n');
        sb.append("- match: ").append(compatibilityReport.matchCount()).append('\n');
        sb.append("- mismatch: ").append(compatibilityReport.mismatchCount()).append('\n');
        sb.append("- error: ").append(compatibilityReport.errorCount()).append('\n');
        sb.append("- compatibilityPassRate: ").append(formatPercent(result.compatibilityPassRate())).append("\n\n");

        FlakeSummary flakeSummary = result.flakeSummary();
        sb.append("## Flake Evidence\n");
        sb.append("- runs: ").append(flakeSummary.runs()).append('\n');
        sb.append("- observations: ").append(flakeSummary.observations()).append('\n');
        sb.append("- flakyObservations: ").append(flakeSummary.flakyObservations()).append('\n');
        sb.append("- flakeRate: ").append(formatPercent(flakeSummary.rate())).append("\n\n");

        ReproSummary reproSummary = result.reproSummary();
        sb.append("## Repro Evidence\n");
        sb.append("- sampleCount: ").append(reproSummary.sampleCount()).append('\n');
        sb.append("- reproTimeP50Minutes: ").append(formatMinutes(reproSummary.p50Minutes())).append('\n');
        sb.append("- samplesMinutes: ").append(reproSummary.sampleMinutes()).append("\n\n");

        sb.append("## Top Regressions\n");
        List<DiffResult> regressions = topRegressions(compatibilityReport, 10);
        if (regressions.isEmpty()) {
            sb.append("- none\n");
        } else {
            for (DiffResult regression : regressions) {
                sb.append("- ").append(regression.scenarioId()).append(" (").append(regression.status()).append(")");
                if (regression.status() == DiffStatus.ERROR) {
                    sb.append(": ").append(regression.errorMessage().orElse("unknown error"));
                } else if (!regression.entries().isEmpty()) {
                    DiffEntry firstEntry = regression.entries().get(0);
                    sb.append(": ").append(firstEntry.path()).append(" (").append(firstEntry.note()).append(")");
                }
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    private String renderReleaseReadinessJson(EvidenceResult result) {
        DifferentialReport compatibilityReport = result.compatibilityReport();
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("overallStatus", result.overallPassed() ? "PASS" : "FAIL");

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("pass", result.passCount());
        summary.put("fail", result.failCount());
        root.put("summary", summary);

        Map<String, Object> buildInfo = new LinkedHashMap<>();
        buildInfo.put("durationMillis", result.durationMillis());
        buildInfo.put("seed", EVIDENCE_SEED);
        root.put("buildInfo", buildInfo);

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put(COMPATIBILITY_KEY, result.compatibilityPassRate());
        metrics.put(FLAKE_KEY, result.flakeRate());
        metrics.put(REPRO_KEY, result.reproTimeP50Minutes());
        root.put("metrics", metrics);

        Map<String, Object> compatibility = new LinkedHashMap<>();
        compatibility.put("total", compatibilityReport.totalScenarios());
        compatibility.put("match", compatibilityReport.matchCount());
        compatibility.put("mismatch", compatibilityReport.mismatchCount());
        compatibility.put("error", compatibilityReport.errorCount());
        root.put("compatibility", compatibility);

        Map<String, Object> flake = new LinkedHashMap<>();
        flake.put("runs", result.flakeSummary().runs());
        flake.put("observations", result.flakeSummary().observations());
        flake.put("flakyObservations", result.flakeSummary().flakyObservations());
        flake.put("rate", result.flakeRate());
        root.put("flake", flake);

        Map<String, Object> repro = new LinkedHashMap<>();
        repro.put("sampleCount", result.reproSummary().sampleCount());
        repro.put("p50Minutes", result.reproTimeP50Minutes());
        repro.put("samplesMinutes", result.reproSummary().sampleMinutes());
        root.put("repro", repro);

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
        root.put("diffSamples", diffSamples(compatibilityReport, 3));

        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
    }

    private static List<Map<String, Object>> diffSamples(DifferentialReport report, int limit) {
        List<Map<String, Object>> samples = new ArrayList<>();
        for (DiffResult result : topRegressions(report, limit)) {
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("scenarioId", result.scenarioId());
            sample.put("status", result.status().name());
            sample.put("errorMessage", result.errorMessage().orElse(null));

            List<Map<String, Object>> entries = new ArrayList<>();
            for (int i = 0; i < result.entries().size() && i < 3; i++) {
                DiffEntry entry = result.entries().get(i);
                Map<String, Object> entryItem = new LinkedHashMap<>();
                entryItem.put("path", entry.path());
                entryItem.put("leftValue", entry.leftValue());
                entryItem.put("rightValue", entry.rightValue());
                entryItem.put("note", entry.note());
                entries.add(entryItem);
            }
            sample.put("entries", entries);
            samples.add(sample);
        }
        return samples;
    }

    private static List<DiffResult> topRegressions(DifferentialReport report, int limit) {
        List<DiffResult> regressions = new ArrayList<>();
        for (DiffResult result : report.results()) {
            if (result.status() == DiffStatus.MATCH) {
                continue;
            }
            regressions.add(result);
            if (regressions.size() == limit) {
                break;
            }
        }
        return regressions;
    }

    private static String formatMetricValue(String metricKey, double value) {
        return switch (metricKey) {
            case COMPATIBILITY_KEY, FLAKE_KEY -> formatPercent(value);
            case REPRO_KEY -> formatMinutes(value);
            default -> String.format(Locale.ROOT, "%.4f", value);
        };
    }

    private static String formatPercent(double ratio) {
        return String.format(Locale.ROOT, "%.2f%%", ratio * 100.0d);
    }

    private static String formatMinutes(double minutes) {
        return String.format(Locale.ROOT, "%.4fmin", minutes);
    }

    public static final class EvidenceConfig {
        private final Path outputDir;
        private final int flakeRuns;
        private final int reproSamples;
        private final boolean failOnGate;

        public EvidenceConfig(Path outputDir, int flakeRuns, int reproSamples, boolean failOnGate) {
            this.outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
            if (flakeRuns < 0) {
                throw new IllegalArgumentException("flakeRuns must be >= 0");
            }
            if (reproSamples <= 0) {
                throw new IllegalArgumentException("reproSamples must be > 0");
            }
            this.flakeRuns = flakeRuns;
            this.reproSamples = reproSamples;
            this.failOnGate = failOnGate;
        }

        public static EvidenceConfig fromArgs(String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            int flakeRuns = DEFAULT_FLAKE_RUNS;
            int reproSamples = DEFAULT_REPRO_SAMPLES;
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
                if (arg.startsWith("--repro-samples=")) {
                    reproSamples = parseInt(readValue(arg, "--repro-samples="), "repro-samples");
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

            return new EvidenceConfig(outputDir, flakeRuns, reproSamples, failOnGate);
        }

        public Path outputDir() {
            return outputDir;
        }

        public int flakeRuns() {
            return flakeRuns;
        }

        public int reproSamples() {
            return reproSamples;
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
        private final Path releaseReadinessJson;
        private final Path releaseReadinessMarkdown;
        private final Path compatibilityJson;
        private final Path compatibilityMarkdown;

        ArtifactPaths(
            Path releaseReadinessJson,
            Path releaseReadinessMarkdown,
            Path compatibilityJson,
            Path compatibilityMarkdown
        ) {
            this.releaseReadinessJson = Objects.requireNonNull(releaseReadinessJson, "releaseReadinessJson");
            this.releaseReadinessMarkdown = Objects.requireNonNull(releaseReadinessMarkdown, "releaseReadinessMarkdown");
            this.compatibilityJson = Objects.requireNonNull(compatibilityJson, "compatibilityJson");
            this.compatibilityMarkdown = Objects.requireNonNull(compatibilityMarkdown, "compatibilityMarkdown");
        }

        public Path releaseReadinessJson() {
            return releaseReadinessJson;
        }

        public Path releaseReadinessMarkdown() {
            return releaseReadinessMarkdown;
        }

        public Path compatibilityJson() {
            return compatibilityJson;
        }

        public Path compatibilityMarkdown() {
            return compatibilityMarkdown;
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
            this.measuredValue = requireFinite(measuredValue, "measuredValue");
            this.thresholdValue = requireFinite(thresholdValue, "thresholdValue");
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

    public static final class FlakeSummary {
        private final int runs;
        private final int observations;
        private final int flakyObservations;
        private final double rate;

        FlakeSummary(int runs, int observations, int flakyObservations, double rate) {
            if (runs < 0) {
                throw new IllegalArgumentException("runs must be >= 0");
            }
            if (observations < 0) {
                throw new IllegalArgumentException("observations must be >= 0");
            }
            if (flakyObservations < 0 || flakyObservations > observations) {
                throw new IllegalArgumentException("flakyObservations must be in range [0, observations]");
            }
            this.runs = runs;
            this.observations = observations;
            this.flakyObservations = flakyObservations;
            this.rate = requireFinite(rate, "rate");
        }

        public int runs() {
            return runs;
        }

        public int observations() {
            return observations;
        }

        public int flakyObservations() {
            return flakyObservations;
        }

        public double rate() {
            return rate;
        }
    }

    public static final class ReproSummary {
        private final int sampleCount;
        private final List<Double> sampleMinutes;
        private final double p50Minutes;

        ReproSummary(int sampleCount, List<Double> sampleMinutes, double p50Minutes) {
            if (sampleCount <= 0) {
                throw new IllegalArgumentException("sampleCount must be > 0");
            }
            this.sampleCount = sampleCount;
            this.sampleMinutes = copySamples(sampleMinutes);
            if (this.sampleMinutes.size() != sampleCount) {
                throw new IllegalArgumentException("sampleMinutes size must equal sampleCount");
            }
            this.p50Minutes = requireFinite(p50Minutes, "p50Minutes");
        }

        public int sampleCount() {
            return sampleCount;
        }

        public List<Double> sampleMinutes() {
            return sampleMinutes;
        }

        public double p50Minutes() {
            return p50Minutes;
        }

        private static List<Double> copySamples(List<Double> source) {
            Objects.requireNonNull(source, "sampleMinutes");
            List<Double> copy = new ArrayList<>(source.size());
            for (Double value : source) {
                copy.add(requireFinite(Objects.requireNonNull(value, "sampleMinute"), "sampleMinute"));
            }
            return List.copyOf(copy);
        }
    }

    public static final class EvidenceResult {
        private final Instant generatedAt;
        private final long durationMillis;
        private final DifferentialReport compatibilityReport;
        private final double compatibilityPassRate;
        private final FlakeSummary flakeSummary;
        private final ReproSummary reproSummary;
        private final List<GateCheck> gateChecks;

        EvidenceResult(
            Instant generatedAt,
            long durationMillis,
            DifferentialReport compatibilityReport,
            double compatibilityPassRate,
            FlakeSummary flakeSummary,
            ReproSummary reproSummary,
            List<GateCheck> gateChecks
        ) {
            this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            if (durationMillis < 0L) {
                throw new IllegalArgumentException("durationMillis must be >= 0");
            }
            this.durationMillis = durationMillis;
            this.compatibilityReport = Objects.requireNonNull(compatibilityReport, "compatibilityReport");
            this.compatibilityPassRate = requireFinite(compatibilityPassRate, "compatibilityPassRate");
            this.flakeSummary = Objects.requireNonNull(flakeSummary, "flakeSummary");
            this.reproSummary = Objects.requireNonNull(reproSummary, "reproSummary");
            this.gateChecks = copyGateChecks(gateChecks);
        }

        public Instant generatedAt() {
            return generatedAt;
        }

        public long durationMillis() {
            return durationMillis;
        }

        public DifferentialReport compatibilityReport() {
            return compatibilityReport;
        }

        public double compatibilityPassRate() {
            return compatibilityPassRate;
        }

        public FlakeSummary flakeSummary() {
            return flakeSummary;
        }

        public double flakeRate() {
            return flakeSummary.rate();
        }

        public ReproSummary reproSummary() {
            return reproSummary;
        }

        public double reproTimeP50Minutes() {
            return reproSummary.p50Minutes();
        }

        public List<GateCheck> gateChecks() {
            return gateChecks;
        }

        public int passCount() {
            int count = 0;
            for (GateCheck gateCheck : gateChecks) {
                if (gateCheck.passed()) {
                    count++;
                }
            }
            return count;
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

    private static double requireFinite(double value, String fieldName) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(fieldName + " must be finite");
        }
        return value;
    }
}
