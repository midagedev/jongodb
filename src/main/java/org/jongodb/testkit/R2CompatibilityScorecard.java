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
import org.bson.Document;

/**
 * R2 compatibility scorecard generator.
 *
 * <p>Inputs:
 *
 * <ul>
 *   <li>UTF differential report (`utf-differential-report.json`)
 *   <li>Spring compatibility matrix (`spring-compatibility-matrix.json`)
 * </ul>
 *
 * <p>Outputs:
 *
 * <ul>
 *   <li>`r2-compatibility-scorecard.json`
 *   <li>`r2-compatibility-scorecard.md`
 *   <li>`r2-support-manifest.json`
 * </ul>
 */
public final class R2CompatibilityScorecard {
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/r2-compatibility");
    private static final Path DEFAULT_UTF_REPORT = Path.of("build/reports/unified-spec/utf-differential-report.json");
    private static final Path DEFAULT_SPRING_REPORT = Path.of("build/reports/spring-matrix/spring-compatibility-matrix.json");
    private static final String SCORECARD_JSON = "r2-compatibility-scorecard.json";
    private static final String SCORECARD_MARKDOWN = "r2-compatibility-scorecard.md";
    private static final String SUPPORT_MANIFEST_JSON = "r2-support-manifest.json";

    private static final double MIN_SPRING_PASS_RATE = 0.98d;

    private static final String UTF_GATE_ID = "utf-differential";
    private static final String SPRING_GATE_ID = "spring-compatibility-matrix";

    private final Clock clock;

    public R2CompatibilityScorecard() {
        this(Clock.systemUTC());
    }

    R2CompatibilityScorecard(final Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
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

        final R2CompatibilityScorecard scorecard = new R2CompatibilityScorecard();
        final Result result = scorecard.runAndWrite(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("R2 compatibility scorecard generated.");
        System.out.println("- overall: " + (result.overallPassed() ? "PASS" : "FAIL"));
        System.out.println("- pass: " + result.passCount());
        System.out.println("- fail: " + result.failCount());
        System.out.println("- missing: " + result.missingCount());
        System.out.println("- scorecardJson: " + paths.scorecardJson());
        System.out.println("- scorecardMarkdown: " + paths.scorecardMarkdown());
        System.out.println("- supportManifestJson: " + paths.supportManifestJson());

        if (config.failOnGate() && !result.overallPassed()) {
            System.err.println("R2 compatibility gate failed.");
            System.exit(2);
        }
    }

    public Result runAndWrite(final RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        final Result result = run(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        Files.createDirectories(config.outputDir());
        Files.writeString(paths.scorecardJson(), renderScorecardJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.scorecardMarkdown(), renderScorecardMarkdown(result), StandardCharsets.UTF_8);
        Files.writeString(paths.supportManifestJson(), renderSupportManifestJson(result.manifest()), StandardCharsets.UTF_8);
        return result;
    }

    public Result run(final RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");

        final GateResult utfGate = evaluateUtfGate(config.utfReportPath());
        final GateResult springGate = evaluateSpringGate(config.springMatrixJsonPath());
        final SupportManifest manifest = SupportManifest.defaultManifest();

        return new Result(
                Instant.now(clock),
                List.of(utfGate, springGate),
                manifest);
    }

    static ArtifactPaths artifactPaths(final Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        final Path normalized = outputDir.normalize();
        return new ArtifactPaths(
                normalized.resolve(SCORECARD_JSON),
                normalized.resolve(SCORECARD_MARKDOWN),
                normalized.resolve(SUPPORT_MANIFEST_JSON));
    }

    private GateResult evaluateUtfGate(final Path artifactPath) throws IOException {
        final List<String> diagnostics = new ArrayList<>();
        if (!Files.exists(artifactPath)) {
            diagnostics.add("missing artifact: " + artifactPath);
            return GateResult.missing(UTF_GATE_ID, artifactPath, diagnostics);
        }

        final Document root;
        try {
            root = parseJsonArtifact(artifactPath);
        } catch (final RuntimeException exception) {
            diagnostics.add("invalid JSON artifact: " + normalizeMessage(exception));
            return GateResult.fail(UTF_GATE_ID, artifactPath, Map.of(), diagnostics);
        }

        final String generatedAt = readString(root, "generatedAt");
        final Document importSummary = readDocument(root, "importSummary");
        final Document differentialSummary = readDocument(root, "differentialSummary");
        if (importSummary == null || differentialSummary == null) {
            diagnostics.add("missing importSummary/differentialSummary");
            return GateResult.fail(UTF_GATE_ID, artifactPath, Map.of("generatedAt", generatedAt), diagnostics);
        }

        final Integer imported = readInteger(importSummary, "imported");
        final Integer skipped = readInteger(importSummary, "skipped");
        final Integer unsupported = readInteger(importSummary, "unsupported");
        final Integer match = readInteger(differentialSummary, "match");
        final Integer mismatch = readInteger(differentialSummary, "mismatch");
        final Integer error = readInteger(differentialSummary, "error");

        final Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("generatedAt", generatedAt);
        metrics.put("imported", imported);
        metrics.put("skipped", skipped);
        metrics.put("unsupported", unsupported);
        metrics.put("match", match);
        metrics.put("mismatch", mismatch);
        metrics.put("error", error);

        boolean schemaValid = true;
        if (imported == null || imported <= 0) {
            diagnostics.add("expected importSummary.imported > 0");
            schemaValid = false;
        }
        if (mismatch == null) {
            diagnostics.add("missing differentialSummary.mismatch");
            schemaValid = false;
        }
        if (error == null) {
            diagnostics.add("missing differentialSummary.error");
            schemaValid = false;
        }
        if (!schemaValid) {
            return GateResult.fail(UTF_GATE_ID, artifactPath, metrics, diagnostics);
        }

        if (mismatch != 0) {
            diagnostics.add("expected mismatch=0 but was " + mismatch);
        }
        if (error != 0) {
            diagnostics.add("expected error=0 but was " + error);
        }
        if (!diagnostics.isEmpty()) {
            return GateResult.fail(UTF_GATE_ID, artifactPath, metrics, diagnostics);
        }
        return GateResult.pass(UTF_GATE_ID, artifactPath, metrics, diagnostics);
    }

    private GateResult evaluateSpringGate(final Path artifactPath) throws IOException {
        final List<String> diagnostics = new ArrayList<>();
        if (!Files.exists(artifactPath)) {
            diagnostics.add("missing artifact: " + artifactPath);
            return GateResult.missing(SPRING_GATE_ID, artifactPath, diagnostics);
        }

        final Document root;
        try {
            root = parseJsonArtifact(artifactPath);
        } catch (final RuntimeException exception) {
            diagnostics.add("invalid JSON artifact: " + normalizeMessage(exception));
            return GateResult.fail(SPRING_GATE_ID, artifactPath, Map.of(), diagnostics);
        }

        final String generatedAt = readString(root, "generatedAt");
        final String overallStatus = readString(root, "overallStatus");
        final Document summary = readDocument(root, "summary");
        final Double passRate = resolvePassRate(summary);
        final Integer pass = summary == null ? null : readInteger(summary, "pass");
        final Integer fail = summary == null ? null : readInteger(summary, "fail");

        final Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("generatedAt", generatedAt);
        metrics.put("overallStatus", overallStatus);
        metrics.put("pass", pass);
        metrics.put("fail", fail);
        metrics.put("passRate", passRate);

        if (passRate == null) {
            diagnostics.add("missing summary.passRate (or pass/fail)");
            return GateResult.fail(SPRING_GATE_ID, artifactPath, metrics, diagnostics);
        }
        if (passRate < MIN_SPRING_PASS_RATE) {
            diagnostics.add(String.format(
                    Locale.ROOT,
                    "spring passRate %.4f < %.2f",
                    passRate,
                    MIN_SPRING_PASS_RATE));
            return GateResult.fail(SPRING_GATE_ID, artifactPath, metrics, diagnostics);
        }
        return GateResult.pass(SPRING_GATE_ID, artifactPath, metrics, diagnostics);
    }

    String renderScorecardJson(final Result result) {
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("overallStatus", result.overallPassed() ? "PASS" : "FAIL");

        final Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("pass", result.passCount());
        summary.put("fail", result.failCount());
        summary.put("missing", result.missingCount());
        root.put("summary", summary);

        final List<Map<String, Object>> gates = new ArrayList<>();
        for (final GateResult gate : result.gates()) {
            final Map<String, Object> gateItem = new LinkedHashMap<>();
            gateItem.put("gateId", gate.gateId());
            gateItem.put("status", gate.status().name());
            gateItem.put("artifactPath", gate.artifactPath().toString());
            gateItem.put("metrics", gate.metrics());
            gateItem.put("diagnostics", gate.diagnostics());
            gates.add(gateItem);
        }
        root.put("gates", gates);

        final Map<String, Object> manifestSummary = new LinkedHashMap<>();
        manifestSummary.put("supported", result.manifest().supportedCount());
        manifestSummary.put("partial", result.manifest().partialCount());
        manifestSummary.put("unsupported", result.manifest().unsupportedCount());
        root.put("supportManifestSummary", manifestSummary);

        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
    }

    String renderScorecardMarkdown(final Result result) {
        final StringBuilder sb = new StringBuilder();
        sb.append("# R2 Compatibility Scorecard\n\n");
        sb.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        sb.append("- overall: ").append(result.overallPassed() ? "PASS" : "FAIL").append('\n');
        sb.append("- pass: ").append(result.passCount()).append('\n');
        sb.append("- fail: ").append(result.failCount()).append('\n');
        sb.append("- missing: ").append(result.missingCount()).append("\n\n");

        sb.append("## Gates\n");
        for (final GateResult gate : result.gates()) {
            sb.append("- ")
                    .append(gate.gateId())
                    .append(": ")
                    .append(gate.status())
                    .append(" (artifact: ")
                    .append(gate.artifactPath())
                    .append(")\n");
            if (!gate.metrics().isEmpty()) {
                sb.append("  - metrics: ").append(gate.metrics()).append('\n');
            }
            if (!gate.diagnostics().isEmpty()) {
                sb.append("  - diagnostics: ").append(gate.diagnostics()).append('\n');
            }
        }

        sb.append("\n## Support Manifest Summary\n");
        sb.append("- supported: ").append(result.manifest().supportedCount()).append('\n');
        sb.append("- partial: ").append(result.manifest().partialCount()).append('\n');
        sb.append("- unsupported: ").append(result.manifest().unsupportedCount()).append('\n');
        return sb.toString();
    }

    String renderSupportManifestJson(final SupportManifest manifest) {
        final Map<String, Object> root = new LinkedHashMap<>();
        final Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("supported", manifest.supportedCount());
        summary.put("partial", manifest.partialCount());
        summary.put("unsupported", manifest.unsupportedCount());
        root.put("summary", summary);

        final List<Map<String, Object>> items = new ArrayList<>();
        for (final SupportEntry entry : manifest.entries()) {
            final Map<String, Object> encoded = new LinkedHashMap<>();
            encoded.put("featureId", entry.featureId());
            encoded.put("category", entry.category());
            encoded.put("status", entry.status().name());
            encoded.put("note", entry.note());
            items.add(encoded);
        }
        root.put("features", items);
        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
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
        System.out.println("Usage: R2CompatibilityScorecard [options]");
        System.out.println("  --output-dir=<path>          Output directory for scorecard/manifest artifacts");
        System.out.println("  --utf-report=<path>          UTF differential report JSON");
        System.out.println("  --spring-matrix-json=<path>  Spring compatibility matrix JSON");
        System.out.println("  --fail-on-gate               Exit non-zero if any gate FAIL/MISSING (default)");
        System.out.println("  --no-fail-on-gate            Always exit zero");
        System.out.println("  --help, -h                   Show help");
    }

    private static Document parseJsonArtifact(final Path artifactPath) throws IOException {
        final String text = Files.readString(artifactPath, StandardCharsets.UTF_8);
        return Document.parse(text);
    }

    private static String readString(final Document document, final String key) {
        final Object value = document.get(key);
        return value instanceof String string ? string : null;
    }

    private static Document readDocument(final Document document, final String key) {
        final Object value = document.get(key);
        return value instanceof Document child ? child : null;
    }

    private static Integer readInteger(final Document document, final String key) {
        final Object value = document.get(key);
        return value instanceof Number number ? number.intValue() : null;
    }

    private static Double readDouble(final Document document, final String key) {
        final Object value = document.get(key);
        return value instanceof Number number ? number.doubleValue() : null;
    }

    private static Double resolvePassRate(final Document summary) {
        if (summary == null) {
            return null;
        }
        final Double passRate = readDouble(summary, "passRate");
        if (passRate != null) {
            return passRate;
        }
        final Integer pass = readInteger(summary, "pass");
        final Integer fail = readInteger(summary, "fail");
        if (pass == null || fail == null) {
            return null;
        }
        final int total = pass + fail;
        if (total <= 0) {
            return null;
        }
        return (double) pass / (double) total;
    }

    private static String normalizeMessage(final Throwable throwable) {
        final String message = throwable.getMessage();
        if (message != null && !message.isBlank()) {
            return message.trim();
        }
        return throwable.getClass().getSimpleName();
    }

    private static String requireText(final String value, final String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    public record ArtifactPaths(Path scorecardJson, Path scorecardMarkdown, Path supportManifestJson) {}

    public enum GateStatus {
        PASS,
        FAIL,
        MISSING
    }

    public record GateResult(
            String gateId,
            GateStatus status,
            Path artifactPath,
            Map<String, Object> metrics,
            List<String> diagnostics) {
        public GateResult {
            gateId = requireText(gateId, "gateId");
            status = Objects.requireNonNull(status, "status");
            artifactPath = Objects.requireNonNull(artifactPath, "artifactPath");
            metrics = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(metrics, "metrics")));
            diagnostics = List.copyOf(Objects.requireNonNull(diagnostics, "diagnostics"));
        }

        static GateResult pass(
                final String gateId,
                final Path artifactPath,
                final Map<String, Object> metrics,
                final List<String> diagnostics) {
            return new GateResult(gateId, GateStatus.PASS, artifactPath, metrics, diagnostics);
        }

        static GateResult fail(
                final String gateId,
                final Path artifactPath,
                final Map<String, Object> metrics,
                final List<String> diagnostics) {
            return new GateResult(gateId, GateStatus.FAIL, artifactPath, metrics, diagnostics);
        }

        static GateResult missing(
                final String gateId,
                final Path artifactPath,
                final List<String> diagnostics) {
            return new GateResult(gateId, GateStatus.MISSING, artifactPath, Map.of(), diagnostics);
        }
    }

    public enum SupportStatus {
        SUPPORTED,
        PARTIAL,
        UNSUPPORTED
    }

    public record SupportEntry(String featureId, String category, SupportStatus status, String note) {
        public SupportEntry {
            featureId = requireText(featureId, "featureId");
            category = requireText(category, "category");
            status = Objects.requireNonNull(status, "status");
            note = requireText(note, "note");
        }
    }

    public record SupportManifest(List<SupportEntry> entries) {
        public SupportManifest {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        }

        static SupportManifest defaultManifest() {
            return new SupportManifest(List.of(
                    new SupportEntry("query.eq-ne-compare", "query", SupportStatus.SUPPORTED, "Core comparison operators"),
                    new SupportEntry("query.elemMatch-all-regex", "query", SupportStatus.SUPPORTED, "Array and regex operators"),
                    new SupportEntry("query.expr-subset", "query", SupportStatus.PARTIAL, "Subset: eq/ne/gt/gte/lt/lte/and/or/not"),
                    new SupportEntry("crud.count-alias", "crud", SupportStatus.SUPPORTED, "count/countDocuments alias parity with skip/limit"),
                    new SupportEntry("crud.distinct-core", "crud", SupportStatus.SUPPORTED, "Distinct key/query path with deterministic diff normalization"),
                    new SupportEntry("crud.findoneanddelete", "crud", SupportStatus.SUPPORTED, "findOneAndDelete via findAndModify remove=true contract"),
                    new SupportEntry("aggregation.match-project-group", "aggregation", SupportStatus.SUPPORTED, "Tier-1 pipeline stages"),
                    new SupportEntry("aggregation.lookup-union-facet", "aggregation", SupportStatus.PARTIAL, "Tier-2 subset without full expression parity"),
                    new SupportEntry("aggregation.expression-operators", "aggregation", SupportStatus.PARTIAL, "Limited expression coverage"),
                    new SupportEntry("index.unique-sparse-partial", "index", SupportStatus.SUPPORTED, "Unique/sparse/partial"),
                    new SupportEntry("index.collation-metadata", "index", SupportStatus.SUPPORTED, "Collation metadata round-trip"),
                    new SupportEntry(
                            "index.collation-semantic",
                            "index",
                            SupportStatus.PARTIAL,
                            "Subset: locale/strength/caseLevel on query-sort-distinct and unique index checks"),
                    new SupportEntry("transactions-single-session", "transaction", SupportStatus.SUPPORTED, "Session + txn command flow"),
                    new SupportEntry("transactions-retryable-advanced", "transaction", SupportStatus.PARTIAL, "Partial compatibility"),
                    new SupportEntry("protocol-unsupported-contract", "protocol", SupportStatus.SUPPORTED, "NotImplemented + UnsupportedFeature labels")));
        }

        int supportedCount() {
            return countByStatus(SupportStatus.SUPPORTED);
        }

        int partialCount() {
            return countByStatus(SupportStatus.PARTIAL);
        }

        int unsupportedCount() {
            return countByStatus(SupportStatus.UNSUPPORTED);
        }

        private int countByStatus(final SupportStatus status) {
            int count = 0;
            for (final SupportEntry entry : entries) {
                if (entry.status() == status) {
                    count++;
                }
            }
            return count;
        }
    }

    public record Result(Instant generatedAt, List<GateResult> gates, SupportManifest manifest) {
        public Result {
            generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            gates = List.copyOf(Objects.requireNonNull(gates, "gates"));
            manifest = Objects.requireNonNull(manifest, "manifest");
        }

        public boolean overallPassed() {
            for (final GateResult gate : gates) {
                if (gate.status() != GateStatus.PASS) {
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

        private int countByStatus(final GateStatus status) {
            int count = 0;
            for (final GateResult gate : gates) {
                if (gate.status() == status) {
                    count++;
                }
            }
            return count;
        }
    }

    public record RunConfig(
            Path outputDir,
            Path utfReportPath,
            Path springMatrixJsonPath,
            boolean failOnGate) {
        public RunConfig {
            outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
            utfReportPath = Objects.requireNonNull(utfReportPath, "utfReportPath").normalize();
            springMatrixJsonPath = Objects.requireNonNull(springMatrixJsonPath, "springMatrixJsonPath").normalize();
        }

        static RunConfig fromArgs(final String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            Path utfReportPath = DEFAULT_UTF_REPORT;
            Path springMatrixJsonPath = DEFAULT_SPRING_REPORT;
            boolean failOnGate = true;

            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(requireText(valueAfterPrefix(arg, "--output-dir="), "output-dir"));
                    continue;
                }
                if (arg.startsWith("--utf-report=")) {
                    utfReportPath = Path.of(requireText(valueAfterPrefix(arg, "--utf-report="), "utf-report"));
                    continue;
                }
                if (arg.startsWith("--spring-matrix-json=")) {
                    springMatrixJsonPath = Path.of(requireText(valueAfterPrefix(arg, "--spring-matrix-json="), "spring-matrix-json"));
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
            return new RunConfig(outputDir, utfReportPath, springMatrixJsonPath, failOnGate);
        }

        private static String valueAfterPrefix(final String arg, final String prefix) {
            return arg.substring(prefix.length()).trim();
        }
    }
}
