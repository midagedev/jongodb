package org.jongodb.testkit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

/**
 * Aggregates release-readiness evidence into a single PASS/FAIL report.
 *
 * <p>Evidence sources:
 *
 * <ul>
 *   <li>M3 gate (`m3-release-readiness.json`)
 *   <li>Real mongod differential baseline (`real-mongod-differential-baseline.json`)
 *   <li>R1 performance/stability gate (`r1-performance-stability-gate.json`)
 *   <li>Spring compatibility matrix (`spring-compatibility-matrix.json`, optional)
 * </ul>
 */
public final class FinalReleaseReadinessAggregator {
    private static final String M3_GATE_ID = "m3-release-readiness";
    private static final String REAL_MONGOD_GATE_ID = "real-mongod-differential-baseline";
    private static final String R1_PERF_GATE_ID = "r1-performance-stability";
    private static final String SPRING_GATE_ID = "spring-compatibility-matrix";

    private static final String DEFAULT_REAL_MONGOD_URI_ENV = "JONGODB_REAL_MONGOD_URI";

    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/release-readiness");
    private static final Path DEFAULT_M3_OUTPUT_DIR = Path.of("build/reports/m3-gate");
    private static final Path DEFAULT_REAL_MONGOD_OUTPUT_DIR = Path.of("build/reports/real-mongod-baseline");
    private static final Path DEFAULT_R1_OUTPUT_DIR = Path.of("build/reports/r1-gates");
    private static final Path DEFAULT_SPRING_MATRIX_JSON = Path.of("build/reports/spring-matrix/spring-compatibility-matrix.json");

    private static final String FINAL_JSON = "r1-final-readiness-report.json";
    private static final String FINAL_MARKDOWN = "r1-final-readiness-report.md";

    private static final double MIN_SPRING_PASS_RATE = 0.98d;

    private final Clock clock;
    private final EvidenceGenerator evidenceGenerator;

    public FinalReleaseReadinessAggregator() {
        this(Clock.systemUTC(), new DefaultEvidenceGenerator());
    }

    FinalReleaseReadinessAggregator(Clock clock, EvidenceGenerator evidenceGenerator) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.evidenceGenerator = Objects.requireNonNull(evidenceGenerator, "evidenceGenerator");
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

        FinalReleaseReadinessAggregator aggregator = new FinalReleaseReadinessAggregator();
        RunResult result = aggregator.runAndWrite(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("Final release readiness report generated.");
        System.out.println("- overall: " + (result.overallPassed() ? "PASS" : "FAIL"));
        System.out.println("- pass: " + result.passCount());
        System.out.println("- fail: " + result.failCount());
        System.out.println("- missing: " + result.missingCount());
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());

        if (config.failOnGate() && !result.overallPassed()) {
            System.err.println("Final release readiness gate failed.");
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

    public RunResult run(RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        GenerationOutcome generationOutcome = maybeGenerateMissingEvidence(config);

        List<GateResult> gateResults = List.of(
            evaluateM3Gate(config, generationOutcome),
            evaluateRealMongodGate(config, generationOutcome),
            evaluateR1PerformanceGate(config, generationOutcome),
            evaluateSpringMatrixGate(config, generationOutcome)
        );
        return new RunResult(Instant.now(clock), gateResults);
    }

    static ArtifactPaths artifactPaths(Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        Path normalized = outputDir.normalize();
        return new ArtifactPaths(
            normalized.resolve(FINAL_JSON),
            normalized.resolve(FINAL_MARKDOWN)
        );
    }

    String renderJson(RunResult result) {
        Objects.requireNonNull(result, "result");
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("overallStatus", result.overallPassed() ? "PASS" : "FAIL");

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("pass", result.passCount());
        summary.put("fail", result.failCount());
        summary.put("missing", result.missingCount());
        root.put("summary", summary);

        List<Map<String, Object>> gateItems = new ArrayList<>();
        for (GateResult gateResult : result.gateResults()) {
            Map<String, Object> gate = new LinkedHashMap<>();
            gate.put("gateId", gateResult.gateId());
            gate.put("status", gateResult.status().name());
            gate.put("artifactPath", gateResult.artifactPath().toString());
            gate.put("artifactGeneratedAt", gateResult.artifactGeneratedAt());
            gate.put("evidenceGenerated", gateResult.evidenceGenerated());
            gate.put("metrics", gateResult.metrics());
            gate.put("diagnostics", gateResult.diagnostics());
            gateItems.add(gate);
        }
        root.put("gates", gateItems);

        List<Map<String, Object>> missingEvidence = new ArrayList<>();
        for (GateResult gateResult : result.gateResults()) {
            if (gateResult.status() != GateStatus.MISSING) {
                continue;
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("gateId", gateResult.gateId());
            item.put("artifactPath", gateResult.artifactPath().toString());
            item.put("diagnostics", gateResult.diagnostics());
            missingEvidence.add(item);
        }
        root.put("missingEvidence", missingEvidence);

        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
    }

    String renderMarkdown(RunResult result) {
        Objects.requireNonNull(result, "result");
        StringBuilder sb = new StringBuilder();
        sb.append("# R1 Final Release Readiness\n\n");
        sb.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        sb.append("- overall: ").append(result.overallPassed() ? "PASS" : "FAIL").append('\n');
        sb.append("- pass: ").append(result.passCount()).append('\n');
        sb.append("- fail: ").append(result.failCount()).append('\n');
        sb.append("- missing: ").append(result.missingCount()).append("\n\n");

        sb.append("## Gates\n");
        for (GateResult gateResult : result.gateResults()) {
            sb.append("- ")
                .append(gateResult.gateId())
                .append(": ")
                .append(gateResult.status())
                .append(" (artifact: ")
                .append(gateResult.artifactPath())
                .append(")\n");
            if (gateResult.artifactGeneratedAt() != null) {
                sb.append("  - artifactGeneratedAt: ").append(gateResult.artifactGeneratedAt()).append('\n');
            }
            sb.append("  - evidenceGenerated: ").append(gateResult.evidenceGenerated()).append('\n');
            if (!gateResult.metrics().isEmpty()) {
                sb.append("  - metrics: ").append(renderMetricMap(gateResult.metrics())).append('\n');
            }
            if (!gateResult.diagnostics().isEmpty()) {
                sb.append("  - diagnostics: ").append(gateResult.diagnostics()).append('\n');
            }
        }
        sb.append('\n');

        sb.append("## Missing Evidence Diagnostics\n");
        int missingCount = 0;
        for (GateResult gateResult : result.gateResults()) {
            if (gateResult.status() != GateStatus.MISSING) {
                continue;
            }
            missingCount++;
            sb.append("- ")
                .append(gateResult.gateId())
                .append(": ")
                .append(gateResult.artifactPath())
                .append(" -> ")
                .append(gateResult.diagnostics())
                .append('\n');
        }
        if (missingCount == 0) {
            sb.append("- none\n");
        }
        return sb.toString();
    }

    private static String renderMetricMap(Map<String, Object> metrics) {
        if (metrics.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    private GenerationOutcome maybeGenerateMissingEvidence(RunConfig config) {
        GenerationOutcome outcome = new GenerationOutcome();
        if (!config.generateMissingEvidence()) {
            return outcome;
        }

        Path m3Artifact = M3GateAutomation.artifactPaths(config.m3OutputDir()).releaseReadinessJson();
        if (!Files.exists(m3Artifact)) {
            tryGenerate(M3_GATE_ID, outcome, () -> evidenceGenerator.generateM3Evidence(config.m3OutputDir()));
        }

        Path realMongodArtifact = RealMongodCorpusRunner.artifactPaths(config.realMongodOutputDir()).jsonArtifact();
        if (!Files.exists(realMongodArtifact)) {
            if (config.realMongodUri() == null || config.realMongodUri().isBlank()) {
                outcome.addDiagnostic(
                    REAL_MONGOD_GATE_ID,
                    "generation skipped: missing --real-mongod-uri (or " + DEFAULT_REAL_MONGOD_URI_ENV + ")"
                );
            } else {
                tryGenerate(
                    REAL_MONGOD_GATE_ID,
                    outcome,
                    () -> evidenceGenerator.generateRealMongodEvidence(config.realMongodOutputDir(), config.realMongodUri())
                );
            }
        }

        Path r1Artifact = R1PerformanceStabilityGateAutomation.artifactPaths(config.r1OutputDir()).gateJson();
        if (!Files.exists(r1Artifact)) {
            tryGenerate(R1_PERF_GATE_ID, outcome, () -> evidenceGenerator.generateR1PerformanceEvidence(config.r1OutputDir()));
        }

        if (!Files.exists(config.springMatrixJson())) {
            outcome.addDiagnostic(
                SPRING_GATE_ID,
                "generation skipped: spring matrix generator is not wired in this slice"
            );
        }
        return outcome;
    }

    private void tryGenerate(String gateId, GenerationOutcome outcome, ThrowingRunnable action) {
        try {
            action.run();
            outcome.markGenerated(gateId);
            outcome.addDiagnostic(gateId, "evidence generated during this aggregation run");
        } catch (Exception exception) {
            outcome.addDiagnostic(gateId, "failed to generate evidence: " + normalizeMessage(exception));
        }
    }

    private GateResult evaluateM3Gate(RunConfig config, GenerationOutcome outcome) throws IOException {
        Path artifact = M3GateAutomation.artifactPaths(config.m3OutputDir()).releaseReadinessJson();
        List<String> diagnostics = new ArrayList<>(outcome.diagnostics(M3_GATE_ID));
        if (!Files.exists(artifact)) {
            diagnostics.add("missing artifact: " + artifact);
            return GateResult.missing(M3_GATE_ID, artifact, outcome.wasGenerated(M3_GATE_ID), diagnostics);
        }

        Document root;
        try {
            root = parseJsonArtifact(artifact);
        } catch (RuntimeException exception) {
            diagnostics.add("invalid JSON artifact: " + normalizeMessage(exception));
            return GateResult.fail(M3_GATE_ID, artifact, null, outcome.wasGenerated(M3_GATE_ID), diagnostics);
        }

        String overallStatus = readString(root, "overallStatus");
        String generatedAt = readString(root, "generatedAt");
        Map<String, Object> metrics = extractMetricValues(
            root,
            "compatibilityPassRate",
            "flakeRate",
            "reproTimeP50Minutes"
        );

        if (!"PASS".equals(overallStatus)) {
            if ("FAIL".equals(overallStatus)) {
                diagnostics.add("artifact overallStatus=FAIL");
            } else {
                diagnostics.add("artifact overallStatus missing or invalid: " + overallStatus);
            }
            return GateResult.fail(M3_GATE_ID, artifact, generatedAt, outcome.wasGenerated(M3_GATE_ID), metrics, diagnostics);
        }
        return GateResult.pass(M3_GATE_ID, artifact, generatedAt, outcome.wasGenerated(M3_GATE_ID), metrics, diagnostics);
    }

    private GateResult evaluateRealMongodGate(RunConfig config, GenerationOutcome outcome) throws IOException {
        Path artifact = RealMongodCorpusRunner.artifactPaths(config.realMongodOutputDir()).jsonArtifact();
        List<String> diagnostics = new ArrayList<>(outcome.diagnostics(REAL_MONGOD_GATE_ID));
        if (!Files.exists(artifact)) {
            diagnostics.add("missing artifact: " + artifact);
            return GateResult.missing(REAL_MONGOD_GATE_ID, artifact, outcome.wasGenerated(REAL_MONGOD_GATE_ID), diagnostics);
        }

        Document root;
        try {
            root = parseJsonArtifact(artifact);
        } catch (RuntimeException exception) {
            diagnostics.add("invalid JSON artifact: " + normalizeMessage(exception));
            return GateResult.fail(
                REAL_MONGOD_GATE_ID,
                artifact,
                null,
                outcome.wasGenerated(REAL_MONGOD_GATE_ID),
                diagnostics
            );
        }

        String generatedAt = readString(root, "generatedAt");
        Document summary = readDocument(root, "summary");
        if (summary == null) {
            diagnostics.add("missing summary object");
            return GateResult.fail(
                REAL_MONGOD_GATE_ID,
                artifact,
                generatedAt,
                outcome.wasGenerated(REAL_MONGOD_GATE_ID),
                diagnostics
            );
        }

        Integer total = readInteger(summary, "total");
        Integer mismatch = readInteger(summary, "mismatch");
        Integer error = readInteger(summary, "error");
        Double passRate = readDouble(summary, "passRate");

        Map<String, Object> metrics = new LinkedHashMap<>();
        if (total != null) {
            metrics.put("total", total);
        }
        if (mismatch != null) {
            metrics.put("mismatch", mismatch);
        }
        if (error != null) {
            metrics.put("error", error);
        }
        if (passRate != null) {
            metrics.put("passRate", passRate);
        }

        boolean schemaValid = true;
        if (total == null || total <= 0) {
            diagnostics.add("expected summary.total > 0");
            schemaValid = false;
        }
        if (mismatch == null) {
            diagnostics.add("missing summary.mismatch");
            schemaValid = false;
        }
        if (error == null) {
            diagnostics.add("missing summary.error");
            schemaValid = false;
        }
        if (!schemaValid) {
            return GateResult.fail(
                REAL_MONGOD_GATE_ID,
                artifact,
                generatedAt,
                outcome.wasGenerated(REAL_MONGOD_GATE_ID),
                metrics,
                diagnostics
            );
        }

        boolean gateFailed = false;
        if (mismatch != 0) {
            diagnostics.add("expected mismatch=0 but was " + mismatch);
            gateFailed = true;
        }
        if (error != 0) {
            diagnostics.add("expected error=0 but was " + error);
            gateFailed = true;
        }

        if (gateFailed) {
            return GateResult.fail(
                REAL_MONGOD_GATE_ID,
                artifact,
                generatedAt,
                outcome.wasGenerated(REAL_MONGOD_GATE_ID),
                metrics,
                diagnostics
            );
        }
        return GateResult.pass(
            REAL_MONGOD_GATE_ID,
            artifact,
            generatedAt,
            outcome.wasGenerated(REAL_MONGOD_GATE_ID),
            metrics,
            diagnostics
        );
    }

    private GateResult evaluateR1PerformanceGate(RunConfig config, GenerationOutcome outcome) throws IOException {
        Path artifact = R1PerformanceStabilityGateAutomation.artifactPaths(config.r1OutputDir()).gateJson();
        List<String> diagnostics = new ArrayList<>(outcome.diagnostics(R1_PERF_GATE_ID));
        if (!Files.exists(artifact)) {
            diagnostics.add("missing artifact: " + artifact);
            return GateResult.missing(R1_PERF_GATE_ID, artifact, outcome.wasGenerated(R1_PERF_GATE_ID), diagnostics);
        }

        Document root;
        try {
            root = parseJsonArtifact(artifact);
        } catch (RuntimeException exception) {
            diagnostics.add("invalid JSON artifact: " + normalizeMessage(exception));
            return GateResult.fail(R1_PERF_GATE_ID, artifact, null, outcome.wasGenerated(R1_PERF_GATE_ID), diagnostics);
        }

        String overallStatus = readString(root, "overallStatus");
        String generatedAt = readString(root, "generatedAt");
        Map<String, Object> metrics = extractMetricValues(
            root,
            "coldStartMillis",
            "resetMillis",
            "crudP95LatencyMillis",
            "throughputOpsPerSecond",
            "flakeRate"
        );

        if (!"PASS".equals(overallStatus)) {
            if ("FAIL".equals(overallStatus)) {
                diagnostics.add("artifact overallStatus=FAIL");
            } else {
                diagnostics.add("artifact overallStatus missing or invalid: " + overallStatus);
            }
            return GateResult.fail(R1_PERF_GATE_ID, artifact, generatedAt, outcome.wasGenerated(R1_PERF_GATE_ID), metrics, diagnostics);
        }
        return GateResult.pass(R1_PERF_GATE_ID, artifact, generatedAt, outcome.wasGenerated(R1_PERF_GATE_ID), metrics, diagnostics);
    }

    private GateResult evaluateSpringMatrixGate(RunConfig config, GenerationOutcome outcome) throws IOException {
        Path artifact = config.springMatrixJson();
        List<String> diagnostics = new ArrayList<>(outcome.diagnostics(SPRING_GATE_ID));
        if (!Files.exists(artifact)) {
            diagnostics.add("missing artifact: " + artifact);
            return GateResult.missing(SPRING_GATE_ID, artifact, outcome.wasGenerated(SPRING_GATE_ID), diagnostics);
        }

        Document root;
        try {
            root = parseJsonArtifact(artifact);
        } catch (RuntimeException exception) {
            diagnostics.add("invalid JSON artifact: " + normalizeMessage(exception));
            return GateResult.fail(SPRING_GATE_ID, artifact, null, outcome.wasGenerated(SPRING_GATE_ID), diagnostics);
        }

        String generatedAt = readString(root, "generatedAt");
        String overallStatus = readString(root, "overallStatus");
        Document summary = readDocument(root, "summary");
        Double passRate = resolvePassRate(summary);

        Map<String, Object> metrics = new LinkedHashMap<>();
        if (passRate != null) {
            metrics.put("passRate", passRate);
        }
        if (summary != null) {
            Integer pass = readInteger(summary, "pass");
            Integer fail = readInteger(summary, "fail");
            if (pass != null) {
                metrics.put("pass", pass);
            }
            if (fail != null) {
                metrics.put("fail", fail);
            }
        }

        if ("PASS".equals(overallStatus)) {
            return GateResult.pass(SPRING_GATE_ID, artifact, generatedAt, outcome.wasGenerated(SPRING_GATE_ID), metrics, diagnostics);
        }
        if ("FAIL".equals(overallStatus)) {
            diagnostics.add("artifact overallStatus=FAIL");
            return GateResult.fail(SPRING_GATE_ID, artifact, generatedAt, outcome.wasGenerated(SPRING_GATE_ID), metrics, diagnostics);
        }
        if (passRate == null) {
            diagnostics.add("artifact missing overallStatus and summary.passRate");
            return GateResult.fail(SPRING_GATE_ID, artifact, generatedAt, outcome.wasGenerated(SPRING_GATE_ID), metrics, diagnostics);
        }

        if (passRate < MIN_SPRING_PASS_RATE) {
            diagnostics.add(
                String.format(Locale.ROOT, "spring passRate %.4f < %.2f", passRate, MIN_SPRING_PASS_RATE)
            );
            return GateResult.fail(SPRING_GATE_ID, artifact, generatedAt, outcome.wasGenerated(SPRING_GATE_ID), metrics, diagnostics);
        }
        return GateResult.pass(SPRING_GATE_ID, artifact, generatedAt, outcome.wasGenerated(SPRING_GATE_ID), metrics, diagnostics);
    }

    private static Map<String, Object> extractMetricValues(Document root, String... keys) {
        Document metricsDoc = readDocument(root, "metrics");
        if (metricsDoc == null) {
            return Map.of();
        }
        Map<String, Object> metrics = new LinkedHashMap<>();
        for (String key : keys) {
            if (metricsDoc.containsKey(key)) {
                metrics.put(key, metricsDoc.get(key));
            }
        }
        return metrics;
    }

    private static Double resolvePassRate(Document summary) {
        if (summary == null) {
            return null;
        }
        Double passRate = readDouble(summary, "passRate");
        if (passRate != null) {
            return passRate;
        }
        Integer pass = readInteger(summary, "pass");
        Integer fail = readInteger(summary, "fail");
        if (pass == null || fail == null) {
            return null;
        }
        int total = pass + fail;
        if (total <= 0) {
            return null;
        }
        return (double) pass / (double) total;
    }

    private static Document parseJsonArtifact(Path artifactPath) throws IOException {
        String text = Files.readString(artifactPath, StandardCharsets.UTF_8);
        return Document.parse(text);
    }

    private static String readString(Document document, String key) {
        Object value = document.get(key);
        return value instanceof String string ? string : null;
    }

    private static Document readDocument(Document document, String key) {
        Object value = document.get(key);
        return value instanceof Document child ? child : null;
    }

    private static Integer readInteger(Document document, String key) {
        Object value = document.get(key);
        if (!(value instanceof Number number)) {
            return null;
        }
        return number.intValue();
    }

    private static Double readDouble(Document document, String key) {
        Object value = document.get(key);
        if (!(value instanceof Number number)) {
            return null;
        }
        return number.doubleValue();
    }

    private static String normalizeMessage(Throwable throwable) {
        String message = throwable.getMessage();
        if (message != null && !message.isBlank()) {
            return message.trim();
        }
        return throwable.getClass().getSimpleName();
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
        System.out.println("Usage: FinalReleaseReadinessAggregator [options]");
        System.out.println("  --output-dir=<path>                 Unified report output directory");
        System.out.println("  --m3-output-dir=<path>              M3 evidence directory");
        System.out.println("  --real-mongod-output-dir=<path>     Real mongod baseline evidence directory");
        System.out.println("  --r1-output-dir=<path>              R1 performance/stability evidence directory");
        System.out.println("  --spring-matrix-json=<path>         Spring matrix JSON artifact path");
        System.out.println("  --real-mongod-uri=<uri>             Real mongod URI for on-demand generation");
        System.out.println("  --generate-missing-evidence         Generate missing M3/R1/real evidence when possible");
        System.out.println("  --no-generate-missing-evidence      Disable evidence generation (default)");
        System.out.println("  --fail-on-gate                      Exit non-zero when any gate is FAIL/MISSING (default)");
        System.out.println("  --no-fail-on-gate                   Always exit zero");
        System.out.println("  --help                              Show this help message");
    }

    private static final class GenerationOutcome {
        private final Map<String, Boolean> generatedByGate = new LinkedHashMap<>();
        private final Map<String, List<String>> diagnosticsByGate = new LinkedHashMap<>();

        void markGenerated(String gateId) {
            generatedByGate.put(gateId, true);
        }

        boolean wasGenerated(String gateId) {
            return generatedByGate.getOrDefault(gateId, false);
        }

        void addDiagnostic(String gateId, String diagnostic) {
            diagnosticsByGate.computeIfAbsent(gateId, ignored -> new ArrayList<>()).add(requireText(diagnostic, "diagnostic"));
        }

        List<String> diagnostics(String gateId) {
            return List.copyOf(diagnosticsByGate.getOrDefault(gateId, List.of()));
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    public interface EvidenceGenerator {
        void generateM3Evidence(Path outputDir) throws Exception;

        void generateRealMongodEvidence(Path outputDir, String mongoUri) throws Exception;

        void generateR1PerformanceEvidence(Path outputDir) throws Exception;
    }

    static final class DefaultEvidenceGenerator implements EvidenceGenerator {
        @Override
        public void generateM3Evidence(Path outputDir) throws IOException {
            new M3GateAutomation().runAndWrite(
                new M3GateAutomation.EvidenceConfig(outputDir, 30, 21, false)
            );
        }

        @Override
        public void generateRealMongodEvidence(Path outputDir, String mongoUri) throws IOException {
            new RealMongodCorpusRunner().runAndWrite(
                new RealMongodCorpusRunner.RunConfig(
                    outputDir,
                    requireText(mongoUri, "mongoUri"),
                    "wire-vs-real-mongod-baseline-v1",
                    10
                )
            );
        }

        @Override
        public void generateR1PerformanceEvidence(Path outputDir) throws IOException {
            new R1PerformanceStabilityGateAutomation().runAndWrite(
                new R1PerformanceStabilityGateAutomation.EvidenceConfig(
                    outputDir,
                    20,
                    new R1BenchmarkRunner.BenchmarkConfig(21, 21, 100, 500),
                    false
                )
            );
        }
    }

    public enum GateStatus {
        PASS,
        FAIL,
        MISSING
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
        private final Path m3OutputDir;
        private final Path realMongodOutputDir;
        private final Path r1OutputDir;
        private final Path springMatrixJson;
        private final boolean generateMissingEvidence;
        private final String realMongodUri;
        private final boolean failOnGate;

        public RunConfig(
            Path outputDir,
            Path m3OutputDir,
            Path realMongodOutputDir,
            Path r1OutputDir,
            Path springMatrixJson,
            boolean generateMissingEvidence,
            String realMongodUri,
            boolean failOnGate
        ) {
            this.outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
            this.m3OutputDir = Objects.requireNonNull(m3OutputDir, "m3OutputDir").normalize();
            this.realMongodOutputDir = Objects.requireNonNull(realMongodOutputDir, "realMongodOutputDir").normalize();
            this.r1OutputDir = Objects.requireNonNull(r1OutputDir, "r1OutputDir").normalize();
            this.springMatrixJson = Objects.requireNonNull(springMatrixJson, "springMatrixJson").normalize();
            this.generateMissingEvidence = generateMissingEvidence;
            this.realMongodUri = normalize(realMongodUri);
            this.failOnGate = failOnGate;
        }

        public static RunConfig fromArgs(String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            Path m3OutputDir = DEFAULT_M3_OUTPUT_DIR;
            Path realMongodOutputDir = DEFAULT_REAL_MONGOD_OUTPUT_DIR;
            Path r1OutputDir = DEFAULT_R1_OUTPUT_DIR;
            Path springMatrixJson = DEFAULT_SPRING_MATRIX_JSON;
            boolean generateMissingEvidence = false;
            String realMongodUri = System.getenv(DEFAULT_REAL_MONGOD_URI_ENV);
            boolean failOnGate = true;

            for (String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(readValue(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--m3-output-dir=")) {
                    m3OutputDir = Path.of(readValue(arg, "--m3-output-dir="));
                    continue;
                }
                if (arg.startsWith("--real-mongod-output-dir=")) {
                    realMongodOutputDir = Path.of(readValue(arg, "--real-mongod-output-dir="));
                    continue;
                }
                if (arg.startsWith("--r1-output-dir=")) {
                    r1OutputDir = Path.of(readValue(arg, "--r1-output-dir="));
                    continue;
                }
                if (arg.startsWith("--spring-matrix-json=")) {
                    springMatrixJson = Path.of(readValue(arg, "--spring-matrix-json="));
                    continue;
                }
                if (arg.startsWith("--real-mongod-uri=")) {
                    realMongodUri = readValue(arg, "--real-mongod-uri=");
                    continue;
                }
                if ("--generate-missing-evidence".equals(arg)) {
                    generateMissingEvidence = true;
                    continue;
                }
                if ("--no-generate-missing-evidence".equals(arg)) {
                    generateMissingEvidence = false;
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

            return new RunConfig(
                outputDir,
                m3OutputDir,
                realMongodOutputDir,
                r1OutputDir,
                springMatrixJson,
                generateMissingEvidence,
                realMongodUri,
                failOnGate
            );
        }

        public Path outputDir() {
            return outputDir;
        }

        public Path m3OutputDir() {
            return m3OutputDir;
        }

        public Path realMongodOutputDir() {
            return realMongodOutputDir;
        }

        public Path r1OutputDir() {
            return r1OutputDir;
        }

        public Path springMatrixJson() {
            return springMatrixJson;
        }

        public boolean generateMissingEvidence() {
            return generateMissingEvidence;
        }

        public String realMongodUri() {
            return realMongodUri;
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
    }

    public static final class GateResult {
        private final String gateId;
        private final GateStatus status;
        private final Path artifactPath;
        private final String artifactGeneratedAt;
        private final boolean evidenceGenerated;
        private final Map<String, Object> metrics;
        private final List<String> diagnostics;

        private GateResult(
            String gateId,
            GateStatus status,
            Path artifactPath,
            String artifactGeneratedAt,
            boolean evidenceGenerated,
            Map<String, Object> metrics,
            List<String> diagnostics
        ) {
            this.gateId = requireText(gateId, "gateId");
            this.status = Objects.requireNonNull(status, "status");
            this.artifactPath = Objects.requireNonNull(artifactPath, "artifactPath").normalize();
            this.artifactGeneratedAt = normalize(artifactGeneratedAt);
            this.evidenceGenerated = evidenceGenerated;
            this.metrics = copyMetrics(metrics);
            this.diagnostics = copyDiagnostics(diagnostics);
        }

        static GateResult pass(
            String gateId,
            Path artifactPath,
            String artifactGeneratedAt,
            boolean evidenceGenerated,
            Map<String, Object> metrics,
            List<String> diagnostics
        ) {
            return new GateResult(gateId, GateStatus.PASS, artifactPath, artifactGeneratedAt, evidenceGenerated, metrics, diagnostics);
        }

        static GateResult pass(
            String gateId,
            Path artifactPath,
            String artifactGeneratedAt,
            boolean evidenceGenerated,
            List<String> diagnostics
        ) {
            return pass(gateId, artifactPath, artifactGeneratedAt, evidenceGenerated, Map.of(), diagnostics);
        }

        static GateResult fail(
            String gateId,
            Path artifactPath,
            String artifactGeneratedAt,
            boolean evidenceGenerated,
            Map<String, Object> metrics,
            List<String> diagnostics
        ) {
            return new GateResult(gateId, GateStatus.FAIL, artifactPath, artifactGeneratedAt, evidenceGenerated, metrics, diagnostics);
        }

        static GateResult fail(
            String gateId,
            Path artifactPath,
            String artifactGeneratedAt,
            boolean evidenceGenerated,
            List<String> diagnostics
        ) {
            return fail(gateId, artifactPath, artifactGeneratedAt, evidenceGenerated, Map.of(), diagnostics);
        }

        static GateResult missing(
            String gateId,
            Path artifactPath,
            boolean evidenceGenerated,
            List<String> diagnostics
        ) {
            return new GateResult(gateId, GateStatus.MISSING, artifactPath, null, evidenceGenerated, Map.of(), diagnostics);
        }

        public String gateId() {
            return gateId;
        }

        public GateStatus status() {
            return status;
        }

        public Path artifactPath() {
            return artifactPath;
        }

        public String artifactGeneratedAt() {
            return artifactGeneratedAt;
        }

        public boolean evidenceGenerated() {
            return evidenceGenerated;
        }

        public Map<String, Object> metrics() {
            return metrics;
        }

        public List<String> diagnostics() {
            return diagnostics;
        }
    }

    public static final class RunResult {
        private final Instant generatedAt;
        private final List<GateResult> gateResults;

        RunResult(Instant generatedAt, List<GateResult> gateResults) {
            this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            this.gateResults = List.copyOf(new ArrayList<>(Objects.requireNonNull(gateResults, "gateResults")));
            if (this.gateResults.isEmpty()) {
                throw new IllegalArgumentException("gateResults must not be empty");
            }
        }

        public Instant generatedAt() {
            return generatedAt;
        }

        public List<GateResult> gateResults() {
            return gateResults;
        }

        public boolean overallPassed() {
            for (GateResult gateResult : gateResults) {
                if (gateResult.status() != GateStatus.PASS) {
                    return false;
                }
            }
            return true;
        }

        public int passCount() {
            return countByStatus(GateStatus.PASS);
        }

        public int failCount() {
            return countByStatus(GateStatus.FAIL);
        }

        public int missingCount() {
            return countByStatus(GateStatus.MISSING);
        }

        private int countByStatus(GateStatus status) {
            int count = 0;
            for (GateResult gateResult : gateResults) {
                if (gateResult.status() == status) {
                    count++;
                }
            }
            return count;
        }
    }

    private static String requireText(String value, String fieldName) {
        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    private static List<String> copyDiagnostics(List<String> diagnostics) {
        Objects.requireNonNull(diagnostics, "diagnostics");
        List<String> copy = new ArrayList<>(diagnostics.size());
        for (String diagnostic : diagnostics) {
            copy.add(requireText(diagnostic, "diagnostic"));
        }
        return List.copyOf(copy);
    }

    private static Map<String, Object> copyMetrics(Map<String, Object> metrics) {
        Objects.requireNonNull(metrics, "metrics");
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            copy.put(requireText(String.valueOf(entry.getKey()), "metricKey"), entry.getValue());
        }
        return Collections.unmodifiableMap(copy);
    }
}
