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
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Runs canonical complex-query certification pack and writes deterministic artifacts.
 */
public final class ComplexQueryCertificationRunner {
    private static final String DEFAULT_MONGO_URI_ENV = "JONGODB_REAL_MONGOD_URI";
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/complex-query-certification");
    private static final String DEFAULT_SEED = "complex-query-cert-v1";

    private static final String JSON_ARTIFACT_FILE = "complex-query-certification.json";
    private static final String MARKDOWN_ARTIFACT_FILE = "complex-query-certification.md";
    private static final String REPLAY_BUNDLE_DIR = DeterministicReplayBundles.DEFAULT_BUNDLE_DIR_NAME;

    private static final int NOT_IMPLEMENTED_CODE = 238;

    private final Clock clock;
    private final Supplier<DifferentialBackend> leftBackendFactory;
    private final Function<String, DifferentialBackend> rightBackendFactory;
    private final List<ComplexQueryPatternPack.PatternCase> patternPack;

    public ComplexQueryCertificationRunner() {
        this(
                Clock.systemUTC(),
                () -> new WireCommandIngressBackend("wire-backend"),
                mongoUri -> new RealMongodBackend("real-mongod", mongoUri),
                ComplexQueryPatternPack.patterns());
    }

    ComplexQueryCertificationRunner(
            final Clock clock,
            final Supplier<DifferentialBackend> leftBackendFactory,
            final Function<String, DifferentialBackend> rightBackendFactory,
            final List<ComplexQueryPatternPack.PatternCase> patternPack) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.leftBackendFactory = Objects.requireNonNull(leftBackendFactory, "leftBackendFactory");
        this.rightBackendFactory = Objects.requireNonNull(rightBackendFactory, "rightBackendFactory");
        this.patternPack = List.copyOf(Objects.requireNonNull(patternPack, "patternPack"));
        if (this.patternPack.isEmpty()) {
            throw new IllegalArgumentException("patternPack must not be empty");
        }
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

        final ComplexQueryCertificationRunner runner = new ComplexQueryCertificationRunner();
        final RunResult result = runner.runAndWrite(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("Complex-query certification generated.");
        System.out.println("- generatedAt: " + result.generatedAt());
        System.out.println("- packVersion: " + result.packVersion());
        System.out.println("- seed: " + result.seed() + " (numericSeed=" + result.numericSeed() + ")");
        System.out.println("- patterns: " + result.patternResults().size());
        System.out.println("- supportedPassRate: " + formatPercent(result.gateSummary().supportedPassRate()));
        System.out.println("- mismatch: " + result.summary().mismatchCount());
        System.out.println("- error: " + result.summary().errorCount());
        System.out.println("- unsupportedByPolicy: " + result.summary().unsupportedByPolicyCount());
        System.out.println("- unsupportedDelta: " + result.summary().unsupportedDeltaCount());
        System.out.println("- gate: " + result.gateSummary().status());
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());

        if (config.failOnGate() && result.gateSummary().status() == GateStatus.FAIL) {
            System.err.println("Complex-query certification gate failed.");
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
        DeterministicReplayBundles.writeBundles(paths.replayBundleDir(), result.replayBundles());
        return result;
    }

    public RunResult run(final RunConfig config) {
        Objects.requireNonNull(config, "config");

        final long numericSeed = RealMongodCorpusRunner.deterministicSeed(config.seed());
        final List<ComplexQueryPatternPack.PatternCase> selectedPatterns =
                selectPatterns(patternPack, numericSeed, config.patternLimit());

        final List<Scenario> scenarios = new ArrayList<>(selectedPatterns.size());
        final Map<String, ComplexQueryPatternPack.PatternCase> patternById = new LinkedHashMap<>();
        for (final ComplexQueryPatternPack.PatternCase pattern : selectedPatterns) {
            scenarios.add(pattern.scenario());
            patternById.put(pattern.id(), pattern);
        }

        final RecordingBackend leftBackend = new RecordingBackend(
                Objects.requireNonNull(leftBackendFactory.get(), "leftBackendFactory result"));
        final RecordingBackend rightBackend = new RecordingBackend(
                Objects.requireNonNull(rightBackendFactory.apply(config.mongoUri()), "rightBackendFactory result"));

        final DifferentialHarness harness = new DifferentialHarness(leftBackend, rightBackend, clock);
        final DifferentialReport report = harness.run(List.copyOf(scenarios));

        final Map<String, DiffResult> diffById = new LinkedHashMap<>();
        for (final DiffResult diffResult : report.results()) {
            diffById.put(diffResult.scenarioId(), diffResult);
        }

        final List<PatternResult> patternResults = new ArrayList<>(selectedPatterns.size());
        final List<DeterministicReplayBundles.Bundle> replayBundles = new ArrayList<>();
        for (final ComplexQueryPatternPack.PatternCase pattern : selectedPatterns) {
            final DiffResult diffResult = diffById.get(pattern.id());
            if (diffResult == null) {
                throw new IllegalStateException("missing diff result for pattern: " + pattern.id());
            }

            final ScenarioOutcome leftOutcome = leftBackend.outcome(pattern.id());
            final PatternStatus status = classifyStatus(diffResult, leftOutcome);
            final boolean expectationSatisfied = expectationSatisfied(pattern.expectedOutcome(), status);
            final String message = summarizePatternMessage(status, diffResult, leftOutcome);

            patternResults.add(new PatternResult(
                    pattern.id(),
                    pattern.title(),
                    pattern.supportClass(),
                    pattern.expectedOutcome(),
                    pattern.rationale(),
                    pattern.sampleUseCase(),
                    status,
                    expectationSatisfied,
                    message,
                    diffResult.status()));

            if (diffResult.status() != DiffStatus.MATCH) {
                replayBundles.add(DeterministicReplayBundles.fromFailure(
                        "complex-query",
                        diffResult,
                        pattern.scenario().commands()));
            }
        }

        final Summary summary = buildSummary(patternResults);
        final GateSummary gateSummary = evaluateGate(patternResults, summary);
        final List<TopReason> topReasons = topReasons(patternResults);

        return new RunResult(
                config,
                config.seed(),
                numericSeed,
                report.generatedAt(),
                ComplexQueryPatternPack.PACK_VERSION,
                report.leftBackend(),
                report.rightBackend(),
                List.copyOf(patternResults),
                summary,
                gateSummary,
                List.copyOf(topReasons),
                List.copyOf(replayBundles));
    }

    public static ArtifactPaths artifactPaths(final Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        final Path normalized = outputDir.normalize();
        return new ArtifactPaths(
                normalized.resolve(JSON_ARTIFACT_FILE),
                normalized.resolve(MARKDOWN_ARTIFACT_FILE),
                normalized.resolve(REPLAY_BUNDLE_DIR));
    }

    private static List<ComplexQueryPatternPack.PatternCase> selectPatterns(
            final List<ComplexQueryPatternPack.PatternCase> patterns,
            final long numericSeed,
            final Integer limit) {
        final List<ComplexQueryPatternPack.PatternCase> ordered = new ArrayList<>(patterns);
        ordered.sort(Comparator.comparing(ComplexQueryPatternPack.PatternCase::id));
        final Random random = new Random(numericSeed);
        for (int index = ordered.size() - 1; index > 0; index--) {
            final int swapIndex = random.nextInt(index + 1);
            final ComplexQueryPatternPack.PatternCase current = ordered.get(index);
            ordered.set(index, ordered.get(swapIndex));
            ordered.set(swapIndex, current);
        }

        if (limit == null || limit >= ordered.size()) {
            return List.copyOf(ordered);
        }
        return List.copyOf(ordered.subList(0, limit));
    }

    private static PatternStatus classifyStatus(final DiffResult diffResult, final ScenarioOutcome leftOutcome) {
        if (isUnsupportedPolicy(leftOutcome, diffResult)) {
            return PatternStatus.UNSUPPORTED_POLICY;
        }
        return switch (diffResult.status()) {
            case MATCH -> PatternStatus.MATCH;
            case MISMATCH -> PatternStatus.MISMATCH;
            case ERROR -> PatternStatus.ERROR;
        };
    }

    private static boolean isUnsupportedPolicy(final ScenarioOutcome leftOutcome, final DiffResult diffResult) {
        if (leftOutcome != null && !leftOutcome.success()) {
            final String message = leftOutcome.errorMessage().orElse(null);
            if (isUnsupportedMessage(message)) {
                return true;
            }
        }

        if (diffResult.status() == DiffStatus.ERROR && isUnsupportedMessage(diffResult.errorMessage().orElse(null))) {
            return true;
        }

        for (final DiffEntry entry : diffResult.entries()) {
            if (!"$.errorMessage".equals(entry.path())) {
                continue;
            }
            if (isUnsupportedMessage(toStringOrNull(entry.leftValue()))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isUnsupportedMessage(final String message) {
        if (message == null || message.isBlank()) {
            return false;
        }
        final String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("code=" + NOT_IMPLEMENTED_CODE)
                || normalized.contains("codename=notimplemented")
                || normalized.contains("[feature=");
    }

    private static String toStringOrNull(final Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    private static boolean expectationSatisfied(
            final ComplexQueryPatternPack.ExpectedOutcome expectedOutcome,
            final PatternStatus status) {
        return switch (expectedOutcome) {
            case MATCH -> status == PatternStatus.MATCH;
            case UNSUPPORTED_POLICY -> status == PatternStatus.UNSUPPORTED_POLICY;
        };
    }

    private static String summarizePatternMessage(
            final PatternStatus status,
            final DiffResult diffResult,
            final ScenarioOutcome leftOutcome) {
        if (status == PatternStatus.MATCH) {
            return "match";
        }
        if (status == PatternStatus.UNSUPPORTED_POLICY) {
            if (leftOutcome != null && !leftOutcome.success()) {
                return leftOutcome.errorMessage().orElse("unsupported by policy");
            }
            return "unsupported by policy";
        }
        if (status == PatternStatus.ERROR) {
            return diffResult.errorMessage().orElse("unknown differential error");
        }
        if (diffResult.entries().isEmpty()) {
            return "mismatch without diff entries";
        }
        final DiffEntry first = diffResult.entries().get(0);
        final String note = first.note() == null || first.note().isBlank() ? "value mismatch" : first.note();
        return first.path() + ": " + note;
    }

    private static Summary buildSummary(final List<PatternResult> patternResults) {
        int supportedTotal = 0;
        int supportedPass = 0;
        int supportedMismatch = 0;
        int supportedError = 0;
        int supportedUnsupported = 0;

        int mismatchCount = 0;
        int errorCount = 0;
        int unsupportedByPolicyCount = 0;
        int unsupportedDeltaCount = 0;

        for (final PatternResult patternResult : patternResults) {
            if (patternResult.supportClass() == ComplexQueryPatternPack.SupportClass.SUPPORTED) {
                supportedTotal++;
                if (patternResult.status() == PatternStatus.MATCH) {
                    supportedPass++;
                } else if (patternResult.status() == PatternStatus.MISMATCH) {
                    supportedMismatch++;
                } else if (patternResult.status() == PatternStatus.ERROR) {
                    supportedError++;
                } else if (patternResult.status() == PatternStatus.UNSUPPORTED_POLICY) {
                    supportedUnsupported++;
                }
            }

            if (patternResult.status() == PatternStatus.MISMATCH) {
                mismatchCount++;
            }
            if (patternResult.status() == PatternStatus.ERROR) {
                errorCount++;
            }
            if (patternResult.status() == PatternStatus.UNSUPPORTED_POLICY) {
                unsupportedByPolicyCount++;
                if (patternResult.expectedOutcome() != ComplexQueryPatternPack.ExpectedOutcome.UNSUPPORTED_POLICY) {
                    unsupportedDeltaCount++;
                }
            }
        }

        final double supportedPassRate = supportedTotal == 0
                ? 0.0d
                : (double) supportedPass / (double) supportedTotal;

        return new Summary(
                patternResults.size(),
                supportedTotal,
                supportedPass,
                supportedPassRate,
                supportedMismatch,
                supportedError,
                supportedUnsupported,
                mismatchCount,
                errorCount,
                unsupportedByPolicyCount,
                unsupportedDeltaCount);
    }

    private static GateSummary evaluateGate(final List<PatternResult> patternResults, final Summary summary) {
        final List<String> failures = new ArrayList<>();

        if (summary.supportedMismatchCount() > 0
                || summary.supportedErrorCount() > 0
                || summary.supportedUnsupportedCount() > 0) {
            failures.add(
                    "supported subset has non-match outcomes"
                            + " (mismatch="
                            + summary.supportedMismatchCount()
                            + ", error="
                            + summary.supportedErrorCount()
                            + ", unsupported="
                            + summary.supportedUnsupportedCount()
                            + ")");
        }

        if (summary.unsupportedDeltaCount() > 0) {
            failures.add("unapproved unsupported-policy increase detected (delta=" + summary.unsupportedDeltaCount() + ")");
        }

        int expectationFailureCount = 0;
        for (final PatternResult patternResult : patternResults) {
            if (!patternResult.expectationSatisfied()) {
                expectationFailureCount++;
            }
        }

        return new GateSummary(
                failures.isEmpty() ? GateStatus.PASS : GateStatus.FAIL,
                List.copyOf(failures),
                expectationFailureCount,
                summary.supportedPassRate());
    }

    private static List<TopReason> topReasons(final List<PatternResult> patternResults) {
        final Map<String, ReasonBucket> buckets = new LinkedHashMap<>();

        for (final PatternResult patternResult : patternResults) {
            if (patternResult.status() != PatternStatus.MISMATCH && patternResult.status() != PatternStatus.ERROR) {
                continue;
            }

            final String reason = patternResult.message();
            ReasonBucket bucket = buckets.get(reason);
            if (bucket == null) {
                bucket = new ReasonBucket();
                buckets.put(reason, bucket);
            }
            bucket.count++;
            if (bucket.samples.size() < 5) {
                bucket.samples.add(patternResult.patternId());
            }
        }

        final List<TopReason> reasons = new ArrayList<>(buckets.size());
        for (final Map.Entry<String, ReasonBucket> entry : buckets.entrySet()) {
            reasons.add(new TopReason(entry.getKey(), entry.getValue().count, List.copyOf(entry.getValue().samples)));
        }

        reasons.sort(
                Comparator.comparingInt(TopReason::count)
                        .reversed()
                        .thenComparing(TopReason::reason));
        return List.copyOf(reasons);
    }

    String renderJson(final RunResult result) {
        Objects.requireNonNull(result, "result");

        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("seed", result.seed());
        root.put("numericSeed", result.numericSeed());
        root.put("packVersion", result.packVersion());

        final Map<String, Object> backends = new LinkedHashMap<>();
        backends.put("left", result.leftBackend());
        backends.put("right", result.rightBackend());
        root.put("backends", backends);

        final Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalPatterns", result.summary().totalPatterns());
        summary.put("supportedPatterns", result.summary().supportedPatterns());
        summary.put("supportedPass", result.summary().supportedPass());
        summary.put("supportedPassRate", result.summary().supportedPassRate());
        summary.put("supportedMismatchCount", result.summary().supportedMismatchCount());
        summary.put("supportedErrorCount", result.summary().supportedErrorCount());
        summary.put("supportedUnsupportedCount", result.summary().supportedUnsupportedCount());
        summary.put("mismatchCount", result.summary().mismatchCount());
        summary.put("errorCount", result.summary().errorCount());
        summary.put("unsupportedByPolicyCount", result.summary().unsupportedByPolicyCount());
        summary.put("unsupportedDeltaCount", result.summary().unsupportedDeltaCount());
        root.put("summary", summary);

        final Map<String, Object> gate = new LinkedHashMap<>();
        gate.put("status", result.gateSummary().status().name());
        gate.put("failureReasons", result.gateSummary().failureReasons());
        gate.put("expectationFailureCount", result.gateSummary().expectationFailureCount());
        root.put("gate", gate);

        final List<Map<String, Object>> reasons = new ArrayList<>(result.topReasons().size());
        for (final TopReason topReason : result.topReasons()) {
            final Map<String, Object> reasonItem = new LinkedHashMap<>();
            reasonItem.put("reason", topReason.reason());
            reasonItem.put("count", topReason.count());
            reasonItem.put("samplePatternIds", topReason.samplePatternIds());
            reasons.add(reasonItem);
        }
        root.put("topMismatchReasons", reasons);

        final List<Map<String, Object>> patterns = new ArrayList<>(result.patternResults().size());
        for (final PatternResult patternResult : result.patternResults()) {
            final Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", patternResult.patternId());
            item.put("title", patternResult.title());
            item.put("supportClass", patternResult.supportClass().name().toLowerCase(Locale.ROOT));
            item.put("expectedOutcome", patternResult.expectedOutcome().name().toLowerCase(Locale.ROOT));
            item.put("rationale", patternResult.rationale());
            item.put("sampleUseCase", patternResult.sampleUseCase());
            item.put("status", patternResult.status().jsonValue());
            item.put("expectationSatisfied", patternResult.expectationSatisfied());
            item.put("diffStatus", patternResult.diffStatus().name());
            item.put("message", patternResult.message());
            patterns.add(item);
        }
        root.put("patterns", patterns);

        final Map<String, Object> replayBundles = new LinkedHashMap<>();
        replayBundles.put("dir", REPLAY_BUNDLE_DIR);
        replayBundles.put("count", result.replayBundles().size());
        replayBundles.put("manifest", REPLAY_BUNDLE_DIR + "/" + DeterministicReplayBundles.MANIFEST_FILE_NAME);
        root.put("replayBundles", replayBundles);

        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
    }

    String renderMarkdown(final RunResult result) {
        Objects.requireNonNull(result, "result");

        final StringBuilder markdown = new StringBuilder();
        markdown.append("# Complex Query Certification\n\n");
        markdown.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        markdown.append("- packVersion: ").append(result.packVersion()).append('\n');
        markdown.append("- seed: ")
                .append(result.seed())
                .append(" (numericSeed=")
                .append(result.numericSeed())
                .append(")\n");
        markdown.append("- backends: ")
                .append(result.leftBackend())
                .append(" vs ")
                .append(result.rightBackend())
                .append('\n');
        markdown.append("- gate: ").append(result.gateSummary().status()).append("\n\n");

        markdown.append("## Metrics\n\n");
        markdown.append("- totalPatterns: ").append(result.summary().totalPatterns()).append('\n');
        markdown.append("- supportedPatterns: ").append(result.summary().supportedPatterns()).append('\n');
        markdown.append("- supportedPass: ").append(result.summary().supportedPass()).append('\n');
        markdown.append("- supportedPassRate: ").append(formatPercent(result.summary().supportedPassRate())).append('\n');
        markdown.append("- mismatchCount: ").append(result.summary().mismatchCount()).append('\n');
        markdown.append("- errorCount: ").append(result.summary().errorCount()).append('\n');
        markdown.append("- unsupportedByPolicyCount: ").append(result.summary().unsupportedByPolicyCount()).append('\n');
        markdown.append("- unsupportedDeltaCount: ").append(result.summary().unsupportedDeltaCount()).append("\n\n");

        markdown.append("## Gate\n\n");
        if (result.gateSummary().failureReasons().isEmpty()) {
            markdown.append("- status: PASS\n");
        } else {
            markdown.append("- status: FAIL\n");
            for (final String reason : result.gateSummary().failureReasons()) {
                markdown.append("- failReason: ").append(reason).append('\n');
            }
        }
        markdown.append("- expectationFailureCount: ").append(result.gateSummary().expectationFailureCount()).append("\n\n");

        markdown.append("## Top Mismatch Reasons\n\n");
        if (result.topReasons().isEmpty()) {
            markdown.append("- none\n\n");
        } else {
            for (final TopReason reason : result.topReasons()) {
                markdown.append("- ")
                        .append(reason.reason())
                        .append(" (count=")
                        .append(reason.count())
                        .append(", patterns=")
                        .append(reason.samplePatternIds())
                        .append(")\n");
            }
            markdown.append('\n');
        }

        markdown.append("## Pattern Results\n\n");
        for (final PatternResult patternResult : result.patternResults()) {
            markdown.append("- ")
                    .append(patternResult.patternId())
                    .append(" [")
                    .append(patternResult.status().jsonValue())
                    .append("] support=")
                    .append(patternResult.supportClass().name().toLowerCase(Locale.ROOT))
                    .append(", expected=")
                    .append(patternResult.expectedOutcome().name().toLowerCase(Locale.ROOT))
                    .append(", expectationSatisfied=")
                    .append(patternResult.expectationSatisfied())
                    .append(" -> ")
                    .append(patternResult.message())
                    .append('\n');
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
        return markdown.toString();
    }

    private static String formatPercent(final double value) {
        return String.format(Locale.ROOT, "%.2f%%", value * 100.0d);
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
        System.out.println("Usage: ComplexQueryCertificationRunner [options]");
        System.out.println("  --output-dir=<path>       Output directory (default: build/reports/complex-query-certification)");
        System.out.println("  --seed=<value>            Deterministic seed (default: complex-query-cert-v1)");
        System.out.println("  --pattern-limit=<n>       Optional max pattern count (1..pack-size)");
        System.out.println("  --mongo-uri=<uri>         Real mongod URI (or env JONGODB_REAL_MONGOD_URI)");
        System.out.println("  --fail-on-gate            Exit non-zero when gate fails (default)");
        System.out.println("  --no-fail-on-gate         Always exit zero");
        System.out.println("  --help, -h                Show this help");
    }

    public enum PatternStatus {
        MATCH("match"),
        MISMATCH("mismatch"),
        ERROR("error"),
        UNSUPPORTED_POLICY("unsupported-policy");

        private final String jsonValue;

        PatternStatus(final String jsonValue) {
            this.jsonValue = jsonValue;
        }

        public String jsonValue() {
            return jsonValue;
        }
    }

    public enum GateStatus {
        PASS,
        FAIL
    }

    public record ArtifactPaths(Path jsonArtifact, Path markdownArtifact, Path replayBundleDir) {}

    public record PatternResult(
            String patternId,
            String title,
            ComplexQueryPatternPack.SupportClass supportClass,
            ComplexQueryPatternPack.ExpectedOutcome expectedOutcome,
            String rationale,
            String sampleUseCase,
            PatternStatus status,
            boolean expectationSatisfied,
            String message,
            DiffStatus diffStatus) {
        public PatternResult {
            patternId = requireText(patternId, "patternId");
            title = requireText(title, "title");
            supportClass = Objects.requireNonNull(supportClass, "supportClass");
            expectedOutcome = Objects.requireNonNull(expectedOutcome, "expectedOutcome");
            rationale = requireText(rationale, "rationale");
            sampleUseCase = requireText(sampleUseCase, "sampleUseCase");
            status = Objects.requireNonNull(status, "status");
            message = requireText(message, "message");
            diffStatus = Objects.requireNonNull(diffStatus, "diffStatus");
        }
    }

    public record TopReason(String reason, int count, List<String> samplePatternIds) {
        public TopReason {
            reason = requireText(reason, "reason");
            if (count <= 0) {
                throw new IllegalArgumentException("count must be > 0");
            }
            samplePatternIds = List.copyOf(Objects.requireNonNull(samplePatternIds, "samplePatternIds"));
        }
    }

    public record Summary(
            int totalPatterns,
            int supportedPatterns,
            int supportedPass,
            double supportedPassRate,
            int supportedMismatchCount,
            int supportedErrorCount,
            int supportedUnsupportedCount,
            int mismatchCount,
            int errorCount,
            int unsupportedByPolicyCount,
            int unsupportedDeltaCount) {
        public Summary {
            if (totalPatterns < 0
                    || supportedPatterns < 0
                    || supportedPass < 0
                    || supportedMismatchCount < 0
                    || supportedErrorCount < 0
                    || supportedUnsupportedCount < 0
                    || mismatchCount < 0
                    || errorCount < 0
                    || unsupportedByPolicyCount < 0
                    || unsupportedDeltaCount < 0) {
                throw new IllegalArgumentException("summary counters must be >= 0");
            }
            if (!Double.isFinite(supportedPassRate) || supportedPassRate < 0.0d || supportedPassRate > 1.0d) {
                throw new IllegalArgumentException("supportedPassRate must be in range [0.0, 1.0]");
            }
        }
    }

    public record GateSummary(
            GateStatus status,
            List<String> failureReasons,
            int expectationFailureCount,
            double supportedPassRate) {
        public GateSummary {
            status = Objects.requireNonNull(status, "status");
            failureReasons = List.copyOf(Objects.requireNonNull(failureReasons, "failureReasons"));
            if (expectationFailureCount < 0) {
                throw new IllegalArgumentException("expectationFailureCount must be >= 0");
            }
            if (!Double.isFinite(supportedPassRate) || supportedPassRate < 0.0d || supportedPassRate > 1.0d) {
                throw new IllegalArgumentException("supportedPassRate must be in range [0.0, 1.0]");
            }
        }
    }

    public record RunConfig(
            Path outputDir,
            String seed,
            Integer patternLimit,
            String mongoUri,
            boolean failOnGate) {
        public RunConfig {
            outputDir = normalizePath(outputDir, "outputDir");
            seed = requireText(seed, "seed");
            mongoUri = requireText(mongoUri, "mongoUri");
            if (patternLimit != null && patternLimit <= 0) {
                throw new IllegalArgumentException("patternLimit must be > 0 when provided");
            }
        }

        static RunConfig fromArgs(final String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            String seed = DEFAULT_SEED;
            Integer patternLimit = null;
            String mongoUri = trimToNull(System.getenv(DEFAULT_MONGO_URI_ENV));
            boolean failOnGate = true;

            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
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
                if (arg.startsWith("--pattern-limit=")) {
                    patternLimit = parsePositiveInt(valueAfterPrefix(arg, "--pattern-limit="), "pattern-limit");
                    continue;
                }
                if (arg.startsWith("--mongo-uri=")) {
                    mongoUri = requireText(valueAfterPrefix(arg, "--mongo-uri="), "mongo-uri");
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
                throw new IllegalArgumentException("unknown argument: " + arg);
            }

            if (mongoUri == null || mongoUri.isBlank()) {
                throw new IllegalArgumentException("mongo-uri must be provided (arg or " + DEFAULT_MONGO_URI_ENV + ")");
            }

            return new RunConfig(outputDir, seed, patternLimit, mongoUri, failOnGate);
        }
    }

    public record RunResult(
            RunConfig config,
            String seed,
            long numericSeed,
            Instant generatedAt,
            String packVersion,
            String leftBackend,
            String rightBackend,
            List<PatternResult> patternResults,
            Summary summary,
            GateSummary gateSummary,
            List<TopReason> topReasons,
            List<DeterministicReplayBundles.Bundle> replayBundles) {
        public RunResult {
            config = Objects.requireNonNull(config, "config");
            seed = requireText(seed, "seed");
            generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            packVersion = requireText(packVersion, "packVersion");
            leftBackend = requireText(leftBackend, "leftBackend");
            rightBackend = requireText(rightBackend, "rightBackend");
            patternResults = List.copyOf(Objects.requireNonNull(patternResults, "patternResults"));
            summary = Objects.requireNonNull(summary, "summary");
            gateSummary = Objects.requireNonNull(gateSummary, "gateSummary");
            topReasons = List.copyOf(Objects.requireNonNull(topReasons, "topReasons"));
            replayBundles = List.copyOf(Objects.requireNonNull(replayBundles, "replayBundles"));
        }
    }

    private static final class ReasonBucket {
        private int count;
        private final List<String> samples = new ArrayList<>();
    }

    private static final class RecordingBackend implements DifferentialBackend {
        private final DifferentialBackend delegate;
        private final Map<String, ScenarioOutcome> outcomes;

        private RecordingBackend(final DifferentialBackend delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.outcomes = new LinkedHashMap<>();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public ScenarioOutcome execute(final Scenario scenario) {
            final ScenarioOutcome outcome = delegate.execute(scenario);
            outcomes.put(scenario.id(), outcome);
            return outcome;
        }

        private ScenarioOutcome outcome(final String scenarioId) {
            return outcomes.get(scenarioId);
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
