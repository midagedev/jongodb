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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jongodb.server.TcpMongoServer;
import org.jongodb.server.WireCommandIngress;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

/**
 * PoC runner for in-process integration-test template viability.
 */
public final class InProcessTemplatePocRunner {
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/in-process-template-poc");
    private static final String DEFAULT_SEED = "in-process-template-poc-v1";
    private static final int DEFAULT_COLD_START_SAMPLES = 7;
    private static final int DEFAULT_WARMUP_OPS = 100;
    private static final int DEFAULT_MEASURED_OPS = 500;
    private static final double DEFAULT_P95_IMPROVEMENT_THRESHOLD = 0.10d;
    private static final double DEFAULT_THROUGHPUT_IMPROVEMENT_THRESHOLD = 0.10d;

    private static final String JSON_ARTIFACT_FILE = "in-process-template-poc.json";
    private static final String MARKDOWN_ARTIFACT_FILE = "in-process-template-poc.md";

    private static final BsonDocument PING_COMMAND = BsonDocument.parse("{\"ping\":1,\"$db\":\"admin\"}");
    private static final BsonDocument SEED_COMMAND = BsonDocument.parse(
            "{\"insert\":\"users\",\"$db\":\"bench\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"},{\"_id\":3,\"name\":\"c\"}]}"
    );
    private static final List<BsonDocument> CRUD_COMMANDS = List.of(
            BsonDocument.parse("{\"find\":\"users\",\"$db\":\"bench\",\"filter\":{\"name\":\"a\"}}"),
            BsonDocument.parse(
                    "{\"update\":\"users\",\"$db\":\"bench\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"aa\"}},\"multi\":false}]}"),
            BsonDocument.parse("{\"find\":\"users\",\"$db\":\"bench\",\"filter\":{\"_id\":1}}"),
            BsonDocument.parse("{\"delete\":\"users\",\"$db\":\"bench\",\"deletes\":[{\"q\":{\"_id\":3},\"limit\":1}]}"),
            BsonDocument.parse("{\"insert\":\"users\",\"$db\":\"bench\",\"documents\":[{\"_id\":3,\"name\":\"c\"}]}")
    );
    private static final BsonDocument TRACE_FAILURE_COMMAND = BsonDocument.parse(
            "{\"doesNotExist\":1,\"$db\":\"admin\",\"lsid\":{\"id\":\"in-process-poc\"},\"txnNumber\":1}"
    );

    private final Clock clock;

    public InProcessTemplatePocRunner() {
        this(Clock.systemUTC());
    }

    InProcessTemplatePocRunner(final Clock clock) {
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

        final InProcessTemplatePocRunner runner = new InProcessTemplatePocRunner();
        final RunResult result = runner.runAndWrite(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("In-process template PoC generated.");
        System.out.println("- generatedAt: " + result.generatedAt());
        System.out.println("- seed: " + result.seed());
        System.out.println("- tcpColdStartP50Ms: " + formatMillis(result.benchmark().tcpColdStartP50Millis()));
        System.out.println("- inProcessColdStartP50Ms: " + formatMillis(result.benchmark().inProcessColdStartP50Millis()));
        System.out.println("- tcpP95Ms: " + formatMillis(result.benchmark().tcpP95LatencyMillis()));
        System.out.println("- inProcessP95Ms: " + formatMillis(result.benchmark().inProcessP95LatencyMillis()));
        System.out.println("- tcpOpsPerSecond: " + formatOps(result.benchmark().tcpThroughputOpsPerSecond()));
        System.out.println("- inProcessOpsPerSecond: " + formatOps(result.benchmark().inProcessThroughputOpsPerSecond()));
        System.out.println("- traceUseful: " + result.traceAnalysis().traceUseful());
        System.out.println("- decision: " + result.decision().status());
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());

        if (config.failOnNoGo() && result.decision().status() == DecisionStatus.NO_GO) {
            System.err.println("In-process template PoC decision is NO_GO.");
            System.exit(2);
        }
    }

    public RunResult runAndWrite(final RunConfig config) throws IOException {
        final RunResult result = run(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());
        Files.createDirectories(config.outputDir());
        Files.writeString(paths.jsonArtifact(), renderJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.markdownArtifact(), renderMarkdown(result), StandardCharsets.UTF_8);
        return result;
    }

    public RunResult run(final RunConfig config) {
        Objects.requireNonNull(config, "config");
        final BenchmarkSummary benchmark = runBenchmarks(config);
        final TraceAnalysisSummary traceAnalysis = runTraceAnalysis();
        final Decision decision = evaluateDecision(
                benchmark,
                traceAnalysis,
                config.p95ImprovementThreshold(),
                config.throughputImprovementThreshold());

        return new RunResult(
                Instant.now(clock),
                config.seed(),
                benchmark,
                traceAnalysis,
                decision);
    }

    static Decision evaluateDecision(
            final BenchmarkSummary benchmark,
            final TraceAnalysisSummary traceAnalysis,
            final double p95ImprovementThreshold,
            final double throughputImprovementThreshold) {
        Objects.requireNonNull(benchmark, "benchmark");
        Objects.requireNonNull(traceAnalysis, "traceAnalysis");
        validateThreshold(p95ImprovementThreshold, "p95ImprovementThreshold");
        validateThreshold(throughputImprovementThreshold, "throughputImprovementThreshold");

        final double p95ImprovementRatio =
                reductionRatio(benchmark.tcpP95LatencyMillis(), benchmark.inProcessP95LatencyMillis());
        final double throughputImprovementRatio =
                increaseRatio(benchmark.tcpThroughputOpsPerSecond(), benchmark.inProcessThroughputOpsPerSecond());

        final boolean p95Go = p95ImprovementRatio >= p95ImprovementThreshold;
        final boolean throughputGo = throughputImprovementRatio >= throughputImprovementThreshold;
        final boolean performanceGo = p95Go || throughputGo;
        final boolean traceGo = traceAnalysis.traceUseful();
        final DecisionStatus status = performanceGo && traceGo ? DecisionStatus.GO : DecisionStatus.NO_GO;

        final List<String> reasons = new ArrayList<>();
        reasons.add(
                "p95 improvement ratio=" + formatPercent(p95ImprovementRatio)
                        + " (threshold=" + formatPercent(p95ImprovementThreshold) + ")");
        reasons.add(
                "throughput improvement ratio=" + formatPercent(throughputImprovementRatio)
                        + " (threshold=" + formatPercent(throughputImprovementThreshold) + ")");
        reasons.add("trace usable=" + traceGo);
        if (!performanceGo) {
            reasons.add("performance gate failed");
        }
        if (!traceGo) {
            reasons.add("trace analysis gate failed");
        }
        if (performanceGo && traceGo) {
            reasons.add("all gates satisfied");
        }

        return new Decision(
                status,
                p95ImprovementRatio,
                throughputImprovementRatio,
                List.copyOf(reasons));
    }

    public static ArtifactPaths artifactPaths(final Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        final Path normalized = outputDir.normalize();
        return new ArtifactPaths(
                normalized.resolve(JSON_ARTIFACT_FILE),
                normalized.resolve(MARKDOWN_ARTIFACT_FILE));
    }

    private BenchmarkSummary runBenchmarks(final RunConfig config) {
        final MeasurementSummary inProcess = runInProcessBenchmark(config);
        final MeasurementSummary tcp = runTcpBenchmark(config);
        final double inProcessColdStartP50 = R1BenchmarkRunner.percentile(inProcess.coldStartSamplesMillis(), 0.50d);
        final double tcpColdStartP50 = R1BenchmarkRunner.percentile(tcp.coldStartSamplesMillis(), 0.50d);
        final double inProcessP95 = R1BenchmarkRunner.percentile(inProcess.latencySamplesMillis(), 0.95d);
        final double tcpP95 = R1BenchmarkRunner.percentile(tcp.latencySamplesMillis(), 0.95d);

        return new BenchmarkSummary(
                inProcess.coldStartSamplesMillis(),
                tcp.coldStartSamplesMillis(),
                inProcess.latencySamplesMillis(),
                tcp.latencySamplesMillis(),
                inProcessColdStartP50,
                tcpColdStartP50,
                inProcessP95,
                tcpP95,
                inProcess.throughputOpsPerSecond(),
                tcp.throughputOpsPerSecond(),
                config.warmupOperations(),
                config.measuredOperations());
    }

    private MeasurementSummary runInProcessBenchmark(final RunConfig config) {
        final List<Double> coldStartSamples = new ArrayList<>(config.coldStartSamples());
        final OpMsgCodec codec = new OpMsgCodec();
        int requestId = 1;
        for (int i = 0; i < config.coldStartSamples(); i++) {
            final long startedAt = System.nanoTime();
            final WireCommandIngress ingress = WireCommandIngress.inMemory();
            sendIngress(ingress, codec, requestId++, PING_COMMAND, false);
            coldStartSamples.add(toMillis(System.nanoTime() - startedAt));
        }

        final WireCommandIngress ingress = WireCommandIngress.inMemory();
        sendIngress(ingress, codec, requestId++, SEED_COMMAND, false);
        for (int i = 0; i < config.warmupOperations(); i++) {
            sendIngress(ingress, codec, requestId++, CRUD_COMMANDS.get(i % CRUD_COMMANDS.size()), false);
        }

        final List<Double> latencySamples = new ArrayList<>(config.measuredOperations());
        final long throughputStartedAt = System.nanoTime();
        for (int i = 0; i < config.measuredOperations(); i++) {
            final BsonDocument command = CRUD_COMMANDS.get(i % CRUD_COMMANDS.size());
            final long startedAt = System.nanoTime();
            sendIngress(ingress, codec, requestId++, command, false);
            latencySamples.add(toMillis(System.nanoTime() - startedAt));
        }
        final long throughputDurationNanos = Math.max(1L, System.nanoTime() - throughputStartedAt);
        final double throughput = R1BenchmarkRunner.throughputOpsPerSecond(
                config.measuredOperations(),
                throughputDurationNanos);
        return new MeasurementSummary(List.copyOf(coldStartSamples), List.copyOf(latencySamples), throughput);
    }

    private MeasurementSummary runTcpBenchmark(final RunConfig config) {
        final List<Double> coldStartSamples = new ArrayList<>(config.coldStartSamples());
        for (int i = 0; i < config.coldStartSamples(); i++) {
            final long startedAt = System.nanoTime();
            try (TcpMongoServer server = TcpMongoServer.inMemory()) {
                server.start();
                try (MongoClient client = MongoClients.create(server.connectionString("bench"))) {
                    runTcpCommand(client, PING_COMMAND);
                }
            }
            coldStartSamples.add(toMillis(System.nanoTime() - startedAt));
        }

        final List<Double> latencySamples = new ArrayList<>(config.measuredOperations());
        final double throughput;
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("bench"))) {
                runTcpCommand(client, SEED_COMMAND);
                for (int i = 0; i < config.warmupOperations(); i++) {
                    runTcpCommand(client, CRUD_COMMANDS.get(i % CRUD_COMMANDS.size()));
                }

                final long throughputStartedAt = System.nanoTime();
                for (int i = 0; i < config.measuredOperations(); i++) {
                    final BsonDocument command = CRUD_COMMANDS.get(i % CRUD_COMMANDS.size());
                    final long startedAt = System.nanoTime();
                    runTcpCommand(client, command);
                    latencySamples.add(toMillis(System.nanoTime() - startedAt));
                }
                final long throughputDurationNanos = Math.max(1L, System.nanoTime() - throughputStartedAt);
                throughput = R1BenchmarkRunner.throughputOpsPerSecond(
                        config.measuredOperations(),
                        throughputDurationNanos);
            }
        }

        return new MeasurementSummary(List.copyOf(coldStartSamples), List.copyOf(latencySamples), throughput);
    }

    private TraceAnalysisSummary runTraceAnalysis() {
        final WireCommandIngress ingress = WireCommandIngress.inMemory();
        final OpMsgCodec codec = new OpMsgCodec();
        int requestId = 80_000;
        sendIngress(ingress, codec, requestId++, PING_COMMAND, false);
        final BsonDocument failureBody = sendIngress(ingress, codec, requestId++, TRACE_FAILURE_COMMAND, true);
        final boolean failureObserved = isFailure(failureBody);

        final BsonDocument invariantReport = ingress.dumpInvariantReportDocument();
        final BsonDocument triageReport = ingress.dumpFailureTriageReportDocument();
        final int journalSize = ingress.commandJournal().size();
        final int violationCount = invariantReport.getInt32("violationCount", new org.bson.BsonInt32(0)).getValue();
        final String rootCauseType = readTriageRootCauseType(triageReport);
        final int reproLineCount = countNonBlankLines(ingress.exportReproJsonLines());
        final boolean traceUseful = failureObserved
                && journalSize >= 2
                && violationCount == 0
                && reproLineCount >= 2
                && !rootCauseType.isBlank();

        return new TraceAnalysisSummary(
                journalSize,
                violationCount,
                rootCauseType,
                reproLineCount,
                traceUseful);
    }

    private static BsonDocument sendIngress(
            final WireCommandIngress ingress,
            final OpMsgCodec codec,
            final int requestId,
            final BsonDocument command,
            final boolean allowFailure) {
        final OpMsg request = new OpMsg(requestId, 0, 0, command.clone());
        final BsonDocument response = codec.decode(ingress.handle(codec.encode(request))).body();
        if (!allowFailure && isFailure(response)) {
            throw new IllegalStateException("command failed in in-process benchmark: " + response.toJson());
        }
        return response;
    }

    private static void runTcpCommand(final MongoClient client, final BsonDocument command) {
        final String databaseName = command.containsKey("$db")
                ? command.getString("$db").getValue()
                : "admin";
        final BsonDocument sanitizedCommand = command.clone();
        sanitizedCommand.remove("$db");
        final MongoDatabase database = client.getDatabase(databaseName);
        database.runCommand(sanitizedCommand);
    }

    private static String readTriageRootCauseType(final BsonDocument triageReport) {
        if (triageReport == null) {
            return "";
        }
        final BsonValue summary = triageReport.get("summary");
        if (summary == null || !summary.isDocument()) {
            return "";
        }
        final BsonValue rootCauseType = summary.asDocument().get("rootCauseType");
        if (rootCauseType == null || !rootCauseType.isString()) {
            return "";
        }
        return rootCauseType.asString().getValue();
    }

    private static int countNonBlankLines(final String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        final String[] lines = text.split("\\R");
        int count = 0;
        for (final String line : lines) {
            if (line != null && !line.isBlank()) {
                count++;
            }
        }
        return count;
    }

    private static boolean isFailure(final BsonDocument responseBody) {
        if (responseBody == null) {
            return true;
        }
        final BsonValue okValue = responseBody.get("ok");
        return okValue == null || !okValue.isNumber() || okValue.asNumber().doubleValue() != 1.0d;
    }

    private static double reductionRatio(final double baseline, final double candidate) {
        if (!Double.isFinite(baseline) || baseline <= 0.0d) {
            return 0.0d;
        }
        if (!Double.isFinite(candidate) || candidate < 0.0d) {
            return 0.0d;
        }
        return (baseline - candidate) / baseline;
    }

    private static double increaseRatio(final double baseline, final double candidate) {
        if (!Double.isFinite(baseline) || baseline <= 0.0d) {
            return 0.0d;
        }
        if (!Double.isFinite(candidate) || candidate < 0.0d) {
            return 0.0d;
        }
        return (candidate - baseline) / baseline;
    }

    private static void validateThreshold(final double value, final String fieldName) {
        if (!Double.isFinite(value) || value < 0.0d || value > 1.0d) {
            throw new IllegalArgumentException(fieldName + " must be in range [0.0, 1.0]");
        }
    }

    private static double toMillis(final long nanos) {
        return Math.max(0L, nanos) / 1_000_000.0d;
    }

    private static boolean containsHelpFlag(final String[] args) {
        for (final String arg : args) {
            if ("--help".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static void printUsage() {
        System.out.println("Usage: InProcessTemplatePocRunner [options]");
        System.out.println("  --output-dir=<path>                     Output directory for JSON/MD artifacts");
        System.out.println("  --seed=<value>                          Seed label written to artifacts");
        System.out.println("  --cold-start-samples=<int>              Sample count for cold-start benchmark");
        System.out.println("  --warmup-ops=<int>                      Warmup operation count before latency sampling");
        System.out.println("  --measured-ops=<int>                    Measured operation count for latency sampling");
        System.out.println("  --p95-improvement-threshold=<0..1>      Minimum p95 improvement ratio for GO");
        System.out.println("  --throughput-improvement-threshold=<0..1>  Minimum throughput improvement ratio for GO");
        System.out.println("  --fail-on-no-go                         Exit non-zero when decision is NO_GO");
        System.out.println("  --help                                  Show usage");
    }

    private static String renderJson(final RunResult result) {
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("seed", result.seed());
        root.put("benchmark", result.benchmark().toMap());
        root.put("traceAnalysis", result.traceAnalysis().toMap());
        root.put("decision", result.decision().toMap());
        return DiffSummaryGenerator.JsonEncoder.encode(root);
    }

    private static String renderMarkdown(final RunResult result) {
        final StringBuilder markdown = new StringBuilder();
        markdown.append("# In-Process Template PoC\n\n");
        markdown.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        markdown.append("- seed: ").append(result.seed()).append('\n');
        markdown.append("- decision: ").append(result.decision().status()).append('\n');
        markdown.append('\n');
        markdown.append("## Performance\n\n");
        markdown.append("| Metric | TCP template | In-Process template |\n");
        markdown.append("| --- | ---: | ---: |\n");
        markdown.append("| Cold start P50 (ms) | ")
                .append(formatMillis(result.benchmark().tcpColdStartP50Millis()))
                .append(" | ")
                .append(formatMillis(result.benchmark().inProcessColdStartP50Millis()))
                .append(" |\n");
        markdown.append("| Steady-state P95 latency (ms) | ")
                .append(formatMillis(result.benchmark().tcpP95LatencyMillis()))
                .append(" | ")
                .append(formatMillis(result.benchmark().inProcessP95LatencyMillis()))
                .append(" |\n");
        markdown.append("| Throughput (ops/s) | ")
                .append(formatOps(result.benchmark().tcpThroughputOpsPerSecond()))
                .append(" | ")
                .append(formatOps(result.benchmark().inProcessThroughputOpsPerSecond()))
                .append(" |\n");
        markdown.append('\n');
        markdown.append("## Trace Analysis\n\n");
        markdown.append("- journalSize: ").append(result.traceAnalysis().journalSize()).append('\n');
        markdown.append("- invariantViolationCount: ").append(result.traceAnalysis().invariantViolationCount()).append('\n');
        markdown.append("- triageRootCauseType: ").append(result.traceAnalysis().triageRootCauseType()).append('\n');
        markdown.append("- reproLineCount: ").append(result.traceAnalysis().reproLineCount()).append('\n');
        markdown.append("- traceUseful: ").append(result.traceAnalysis().traceUseful()).append('\n');
        markdown.append('\n');
        markdown.append("## Decision Inputs\n\n");
        for (final String reason : result.decision().reasons()) {
            markdown.append("- ").append(reason).append('\n');
        }
        return markdown.toString();
    }

    private static String formatPercent(final double ratio) {
        return String.format(java.util.Locale.ROOT, "%.2f%%", ratio * 100.0d);
    }

    private static String formatMillis(final double millis) {
        return String.format(java.util.Locale.ROOT, "%.3f", millis);
    }

    private static String formatOps(final double ops) {
        return String.format(java.util.Locale.ROOT, "%.1f", ops);
    }

    public record RunConfig(
            Path outputDir,
            String seed,
            int coldStartSamples,
            int warmupOperations,
            int measuredOperations,
            double p95ImprovementThreshold,
            double throughputImprovementThreshold,
            boolean failOnNoGo) {
        public RunConfig {
            outputDir = Objects.requireNonNull(outputDir, "outputDir");
            seed = requireText(seed, "seed");
            if (coldStartSamples <= 0) {
                throw new IllegalArgumentException("coldStartSamples must be > 0");
            }
            if (warmupOperations < 0) {
                throw new IllegalArgumentException("warmupOperations must be >= 0");
            }
            if (measuredOperations <= 0) {
                throw new IllegalArgumentException("measuredOperations must be > 0");
            }
            validateThreshold(p95ImprovementThreshold, "p95ImprovementThreshold");
            validateThreshold(throughputImprovementThreshold, "throughputImprovementThreshold");
        }

        static RunConfig fromArgs(final String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            String seed = DEFAULT_SEED;
            int coldStartSamples = DEFAULT_COLD_START_SAMPLES;
            int warmupOperations = DEFAULT_WARMUP_OPS;
            int measuredOperations = DEFAULT_MEASURED_OPS;
            double p95ImprovementThreshold = DEFAULT_P95_IMPROVEMENT_THRESHOLD;
            double throughputImprovementThreshold = DEFAULT_THROUGHPUT_IMPROVEMENT_THRESHOLD;
            boolean failOnNoGo = false;

            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(valueAfterPrefix(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--seed=")) {
                    seed = valueAfterPrefix(arg, "--seed=");
                    continue;
                }
                if (arg.startsWith("--cold-start-samples=")) {
                    coldStartSamples = parsePositiveInt(valueAfterPrefix(arg, "--cold-start-samples="), "cold-start-samples");
                    continue;
                }
                if (arg.startsWith("--warmup-ops=")) {
                    warmupOperations = parseNonNegativeInt(valueAfterPrefix(arg, "--warmup-ops="), "warmup-ops");
                    continue;
                }
                if (arg.startsWith("--measured-ops=")) {
                    measuredOperations = parsePositiveInt(valueAfterPrefix(arg, "--measured-ops="), "measured-ops");
                    continue;
                }
                if (arg.startsWith("--p95-improvement-threshold=")) {
                    p95ImprovementThreshold = parseRatio(valueAfterPrefix(arg, "--p95-improvement-threshold="), "p95-improvement-threshold");
                    continue;
                }
                if (arg.startsWith("--throughput-improvement-threshold=")) {
                    throughputImprovementThreshold = parseRatio(
                            valueAfterPrefix(arg, "--throughput-improvement-threshold="),
                            "throughput-improvement-threshold");
                    continue;
                }
                if ("--fail-on-no-go".equals(arg)) {
                    failOnNoGo = true;
                    continue;
                }
                throw new IllegalArgumentException("unsupported argument: " + arg);
            }

            return new RunConfig(
                    outputDir,
                    seed,
                    coldStartSamples,
                    warmupOperations,
                    measuredOperations,
                    p95ImprovementThreshold,
                    throughputImprovementThreshold,
                    failOnNoGo);
        }

        private static int parsePositiveInt(final String value, final String fieldName) {
            final int parsed = parseInt(value, fieldName);
            if (parsed <= 0) {
                throw new IllegalArgumentException(fieldName + " must be > 0");
            }
            return parsed;
        }

        private static int parseNonNegativeInt(final String value, final String fieldName) {
            final int parsed = parseInt(value, fieldName);
            if (parsed < 0) {
                throw new IllegalArgumentException(fieldName + " must be >= 0");
            }
            return parsed;
        }

        private static int parseInt(final String value, final String fieldName) {
            try {
                return Integer.parseInt(value);
            } catch (final NumberFormatException numberFormatException) {
                throw new IllegalArgumentException(fieldName + " must be an integer", numberFormatException);
            }
        }

        private static double parseRatio(final String value, final String fieldName) {
            try {
                final double parsed = Double.parseDouble(value);
                validateThreshold(parsed, fieldName);
                return parsed;
            } catch (final NumberFormatException numberFormatException) {
                throw new IllegalArgumentException(fieldName + " must be a floating point number", numberFormatException);
            }
        }

        private static String valueAfterPrefix(final String arg, final String prefix) {
            final String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " must have a value");
            }
            return value;
        }
    }

    public record RunResult(
            Instant generatedAt,
            String seed,
            BenchmarkSummary benchmark,
            TraceAnalysisSummary traceAnalysis,
            Decision decision) {}

    public record ArtifactPaths(Path jsonArtifact, Path markdownArtifact) {}

    private record MeasurementSummary(
            List<Double> coldStartSamplesMillis,
            List<Double> latencySamplesMillis,
            double throughputOpsPerSecond) {}

    public record BenchmarkSummary(
            List<Double> inProcessColdStartSamplesMillis,
            List<Double> tcpColdStartSamplesMillis,
            List<Double> inProcessLatencySamplesMillis,
            List<Double> tcpLatencySamplesMillis,
            double inProcessColdStartP50Millis,
            double tcpColdStartP50Millis,
            double inProcessP95LatencyMillis,
            double tcpP95LatencyMillis,
            double inProcessThroughputOpsPerSecond,
            double tcpThroughputOpsPerSecond,
            int warmupOperations,
            int measuredOperations) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("inProcessColdStartSamplesMillis", inProcessColdStartSamplesMillis);
            root.put("tcpColdStartSamplesMillis", tcpColdStartSamplesMillis);
            root.put("inProcessLatencySamplesMillis", inProcessLatencySamplesMillis);
            root.put("tcpLatencySamplesMillis", tcpLatencySamplesMillis);
            root.put("inProcessColdStartP50Millis", inProcessColdStartP50Millis);
            root.put("tcpColdStartP50Millis", tcpColdStartP50Millis);
            root.put("inProcessP95LatencyMillis", inProcessP95LatencyMillis);
            root.put("tcpP95LatencyMillis", tcpP95LatencyMillis);
            root.put("inProcessThroughputOpsPerSecond", inProcessThroughputOpsPerSecond);
            root.put("tcpThroughputOpsPerSecond", tcpThroughputOpsPerSecond);
            root.put("warmupOperations", warmupOperations);
            root.put("measuredOperations", measuredOperations);
            return root;
        }
    }

    public record TraceAnalysisSummary(
            int journalSize,
            int invariantViolationCount,
            String triageRootCauseType,
            int reproLineCount,
            boolean traceUseful) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("journalSize", journalSize);
            root.put("invariantViolationCount", invariantViolationCount);
            root.put("triageRootCauseType", triageRootCauseType);
            root.put("reproLineCount", reproLineCount);
            root.put("traceUseful", traceUseful);
            return root;
        }
    }

    public record Decision(
            DecisionStatus status,
            double p95ImprovementRatio,
            double throughputImprovementRatio,
            List<String> reasons) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("status", status.name());
            root.put("p95ImprovementRatio", p95ImprovementRatio);
            root.put("throughputImprovementRatio", throughputImprovementRatio);
            root.put("reasons", reasons);
            return root;
        }
    }

    public enum DecisionStatus {
        GO,
        NO_GO
    }

    private static String requireText(final String value, final String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
