package org.jongodb.testkit;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.Document;

/**
 * Standardized fixture refresh workflow runner (full/incremental) with diff report and approval gate.
 */
public final class FixtureRefreshTool {
    private static final Pattern NDJSON_FILE = Pattern.compile("^([^.]+)\\.([^.]+)\\.ndjson$");
    private static final String REFRESH_OUTPUT_DIR = "refreshed";
    private static final String REPORT_JSON = "fixture-refresh-report.json";
    private static final String REPORT_MD = "fixture-refresh-report.md";

    private FixtureRefreshTool() {}

    public static void main(final String[] args) {
        final int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(final String[] args, final PrintStream out, final PrintStream err) {
        Objects.requireNonNull(args, "args");
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(err, "err");

        final Config config;
        try {
            config = Config.fromArgs(args);
        } catch (final IllegalArgumentException exception) {
            err.println(exception.getMessage());
            printUsage(err);
            return 2;
        }

        if (config.help()) {
            printUsage(out);
            return 0;
        }

        try {
            final Instant startedAt = Instant.now();
            final Map<String, List<Document>> baseline = loadCollections(config.baselineDir());
            final Map<String, List<Document>> candidate = loadCollections(config.candidateDir());

            Files.createDirectories(config.outputDir());
            final Path refreshOutputDir = config.outputDir().resolve(REFRESH_OUTPUT_DIR);
            Files.createDirectories(refreshOutputDir);

            final List<CollectionDiff> diffs = computeDiffs(baseline, candidate, config.mode(), refreshOutputDir);
            final boolean requiresApproval = diffs.stream().anyMatch(CollectionDiff::requiresApproval);
            final FixtureDriftAnalyzer.DriftReport driftReport = FixtureDriftAnalyzer.analyze(
                    baseline,
                    candidate,
                    config.warnThreshold(),
                    config.failThreshold());

            final Instant finishedAt = Instant.now();
            final RefreshReport report = RefreshReport.from(
                    startedAt,
                    finishedAt,
                    config.mode(),
                    baseline,
                    candidate,
                    diffs,
                    driftReport,
                    requiresApproval,
                    config.approved());

            Files.writeString(config.outputDir().resolve(REPORT_JSON), report.toJson(), StandardCharsets.UTF_8);
            Files.writeString(config.outputDir().resolve(REPORT_MD), report.toMarkdown(), StandardCharsets.UTF_8);
            Files.writeString(
                    config.outputDir().resolve(FixtureDriftAnalyzer.REPORT_JSON),
                    driftReport.toJson(),
                    StandardCharsets.UTF_8);
            Files.writeString(
                    config.outputDir().resolve(FixtureDriftAnalyzer.REPORT_MD),
                    driftReport.toMarkdown(),
                    StandardCharsets.UTF_8);

            out.println("Fixture refresh finished");
            out.println("- mode: " + config.mode().value());
            out.println("- baselineCollections: " + baseline.size());
            out.println("- candidateCollections: " + candidate.size());
            out.println("- changedCollections: " + diffs.stream().filter(CollectionDiff::isChanged).count());
            out.println("- requiresApproval: " + requiresApproval);
            out.println("- driftWarnings: " + driftReport.warningCollections());
            out.println("- driftFailures: " + driftReport.failingCollections());
            out.println("- outputDir: " + refreshOutputDir);
            out.println("- reportJson: " + config.outputDir().resolve(REPORT_JSON));
            out.println("- reportMd: " + config.outputDir().resolve(REPORT_MD));
            out.println("- driftJson: " + config.outputDir().resolve(FixtureDriftAnalyzer.REPORT_JSON));
            out.println("- driftMd: " + config.outputDir().resolve(FixtureDriftAnalyzer.REPORT_MD));

            if (config.requireApproval() && requiresApproval && !config.approved()) {
                err.println("refresh includes breaking changes; rerun with --approved to acknowledge");
                return 1;
            }
            if (config.failOnThreshold() && driftReport.hasFailures()) {
                err.println("drift threshold failed; adjust fixtures or raise threshold with explicit policy update");
                return 1;
            }
            return 0;
        } catch (final IOException | RuntimeException exception) {
            err.println("fixture refresh failed: " + exception.getMessage());
            return 1;
        }
    }

    private static List<CollectionDiff> computeDiffs(
            final Map<String, List<Document>> baseline,
            final Map<String, List<Document>> candidate,
            final RefreshMode mode,
            final Path outputDir) throws IOException {
        final Set<String> union = new LinkedHashSet<>();
        union.addAll(baseline.keySet());
        union.addAll(candidate.keySet());

        final List<String> namespaces = new ArrayList<>(union);
        namespaces.sort(String::compareTo);

        final List<CollectionDiff> result = new ArrayList<>(namespaces.size());
        for (final String namespace : namespaces) {
            final List<Document> baselineDocs = baseline.getOrDefault(namespace, List.of());
            final List<Document> candidateDocs = candidate.getOrDefault(namespace, List.of());

            final Map<String, Document> baselineIndex = indexDocuments(baselineDocs);
            final Map<String, Document> candidateIndex = indexDocuments(candidateDocs);

            final Set<String> keys = new LinkedHashSet<>();
            keys.addAll(baselineIndex.keySet());
            keys.addAll(candidateIndex.keySet());
            final List<String> orderedKeys = new ArrayList<>(keys);
            orderedKeys.sort(String::compareTo);

            int added = 0;
            int removed = 0;
            int changed = 0;
            int unchanged = 0;
            boolean hasSchemaBreak = false;

            final List<Document> incrementalDocs = new ArrayList<>();
            for (final String key : orderedKeys) {
                final Document before = baselineIndex.get(key);
                final Document after = candidateIndex.get(key);
                if (before == null && after != null) {
                    added++;
                    incrementalDocs.add(after);
                    continue;
                }
                if (before != null && after == null) {
                    removed++;
                    continue;
                }
                if (before == null) {
                    continue;
                }

                final String beforeCanonical = canonicalJson(before);
                final String afterCanonical = canonicalJson(after);
                if (beforeCanonical.equals(afterCanonical)) {
                    unchanged++;
                    continue;
                }
                changed++;
                incrementalDocs.add(after);
                if (hasFieldDrop(before, after)) {
                    hasSchemaBreak = true;
                }
            }

            final List<String> approvalReasons = new ArrayList<>();
            if (removed > 0) {
                approvalReasons.add("removed=" + removed);
            }
            if (hasSchemaBreak) {
                approvalReasons.add("field-drop");
            }
            final boolean requiresApproval = !approvalReasons.isEmpty();

            final List<Document> outputDocuments;
            if (mode == RefreshMode.FULL) {
                outputDocuments = sortForOutput(candidateIndex);
            } else {
                outputDocuments = sortDocuments(incrementalDocs);
            }

            final Path outputFile = outputDir.resolve(namespace + ".ndjson");
            if (!outputDocuments.isEmpty()) {
                writeNdjson(outputFile, outputDocuments);
            } else {
                Files.deleteIfExists(outputFile);
            }

            result.add(new CollectionDiff(
                    namespace,
                    baselineDocs.size(),
                    candidateDocs.size(),
                    added,
                    removed,
                    changed,
                    unchanged,
                    outputDocuments.size(),
                    requiresApproval,
                    List.copyOf(approvalReasons),
                    outputFile.toString()));
        }
        return List.copyOf(result);
    }

    private static List<Document> sortForOutput(final Map<String, Document> indexed) {
        final List<String> keys = new ArrayList<>(indexed.keySet());
        keys.sort(String::compareTo);
        final List<Document> docs = new ArrayList<>(keys.size());
        for (final String key : keys) {
            docs.add(indexed.get(key));
        }
        return List.copyOf(docs);
    }

    private static List<Document> sortDocuments(final List<Document> docs) {
        final List<Document> sorted = new ArrayList<>(docs);
        sorted.sort(Comparator.comparing(FixtureRefreshTool::documentKey));
        return List.copyOf(sorted);
    }

    private static void writeNdjson(final Path file, final List<Document> documents) throws IOException {
        final List<String> lines = new ArrayList<>(documents.size());
        for (final Document document : documents) {
            lines.add(canonicalJson(document));
        }
        final String content = lines.isEmpty() ? "" : String.join("\n", lines) + "\n";
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static Map<String, Document> indexDocuments(final List<Document> docs) {
        final Map<String, Document> index = new LinkedHashMap<>();
        for (final Document document : docs) {
            index.put(documentKey(document), document);
        }
        return Map.copyOf(index);
    }

    private static boolean hasFieldDrop(final Document before, final Document after) {
        final Set<String> beforeKeys = new LinkedHashSet<>(before.keySet());
        final Set<String> afterKeys = new LinkedHashSet<>(after.keySet());
        beforeKeys.removeAll(afterKeys);
        return !beforeKeys.isEmpty();
    }

    private static String documentKey(final Document document) {
        if (document.containsKey("_id")) {
            return "_id:" + DiffSummaryGenerator.JsonEncoder.encode(canonicalizeValue(document.get("_id")));
        }
        return "hash:" + sha256(canonicalJson(document));
    }

    private static String canonicalJson(final Document document) {
        return DiffSummaryGenerator.JsonEncoder.encode(canonicalizeValue(document));
    }

    private static String sha256(final String text) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
        digest.update(text.getBytes(StandardCharsets.UTF_8));
        final byte[] bytes = digest.digest();
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (final byte item : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", item));
        }
        return sb.toString();
    }

    private static Object canonicalizeValue(final Object value) {
        if (value instanceof Document document) {
            final Map<String, Object> sorted = new TreeMap<>();
            for (final Map.Entry<String, Object> entry : document.entrySet()) {
                sorted.put(entry.getKey(), canonicalizeValue(entry.getValue()));
            }
            final Map<String, Object> normalized = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : sorted.entrySet()) {
                normalized.put(entry.getKey(), entry.getValue());
            }
            return normalized;
        }
        if (value instanceof Map<?, ?> map) {
            final Map<String, Object> sorted = new TreeMap<>();
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() instanceof String key) {
                    sorted.put(key, canonicalizeValue(entry.getValue()));
                }
            }
            final Map<String, Object> normalized = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : sorted.entrySet()) {
                normalized.put(entry.getKey(), entry.getValue());
            }
            return normalized;
        }
        if (value instanceof List<?> list) {
            final List<Object> normalized = new ArrayList<>(list.size());
            for (final Object item : list) {
                normalized.add(canonicalizeValue(item));
            }
            return normalized;
        }
        return value;
    }

    private static Map<String, List<Document>> loadCollections(final Path inputDir) throws IOException {
        if (!Files.exists(inputDir) || !Files.isDirectory(inputDir)) {
            throw new IllegalArgumentException("input dir is missing: " + inputDir);
        }

        final Map<String, List<Document>> result = new LinkedHashMap<>();
        try (var stream = Files.list(inputDir)) {
            stream.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".ndjson"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .forEach(path -> {
                        final Matcher matcher = NDJSON_FILE.matcher(path.getFileName().toString());
                        if (!matcher.matches()) {
                            return;
                        }
                        final String namespace = matcher.group(1) + "." + matcher.group(2);
                        final List<Document> docs = readNdjson(path);
                        result.put(namespace, docs);
                    });
        }
        return Map.copyOf(result);
    }

    private static List<Document> readNdjson(final Path file) {
        final List<Document> docs = new ArrayList<>();
        final List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (final IOException exception) {
            throw new IllegalStateException("failed to read ndjson: " + file, exception);
        }
        for (final String line : lines) {
            if (line == null || line.isBlank()) {
                continue;
            }
            docs.add(Document.parse(line));
        }
        return List.copyOf(docs);
    }

    private static void printUsage(final PrintStream stream) {
        stream.println("Usage: FixtureRefreshTool --baseline-dir=<dir> --candidate-dir=<dir> --output-dir=<dir> [options]");
        stream.println("  --baseline-dir=<dir>         Previous fixture ndjson directory");
        stream.println("  --candidate-dir=<dir>        Newly extracted/sanitized ndjson directory");
        stream.println("  --output-dir=<dir>           Output directory for refreshed artifacts + reports");
        stream.println("  --mode=full|incremental      Refresh mode (default: full)");
        stream.println("  --require-approval           Fail when breaking changes exist without --approved");
        stream.println("  --approved                   Acknowledge and approve breaking refresh result");
        stream.println("  --warn-threshold=<value>     Drift warning threshold score (default: 0.15)");
        stream.println("  --fail-threshold=<value>     Drift failure threshold score (default: 0.30)");
        stream.println("  --fail-on-threshold          Fail when any collection crosses fail threshold");
        stream.println("  --no-fail-on-threshold       Do not fail on drift threshold (default)");
        stream.println("  --help                       Show usage");
    }

    private record Config(
            Path baselineDir,
            Path candidateDir,
            Path outputDir,
            RefreshMode mode,
            boolean requireApproval,
            boolean approved,
            double warnThreshold,
            double failThreshold,
            boolean failOnThreshold,
            boolean help) {
        static Config fromArgs(final String[] args) {
            Path baselineDir = null;
            Path candidateDir = null;
            Path outputDir = null;
            RefreshMode mode = RefreshMode.FULL;
            boolean requireApproval = false;
            boolean approved = false;
            double warnThreshold = 0.15d;
            double failThreshold = 0.30d;
            boolean failOnThreshold = false;
            boolean help = false;

            for (final String arg : args) {
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
                    continue;
                }
                if (arg.startsWith("--baseline-dir=")) {
                    baselineDir = Path.of(valueAfterPrefix(arg, "--baseline-dir="));
                    continue;
                }
                if (arg.startsWith("--candidate-dir=")) {
                    candidateDir = Path.of(valueAfterPrefix(arg, "--candidate-dir="));
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(valueAfterPrefix(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--mode=")) {
                    mode = RefreshMode.fromText(valueAfterPrefix(arg, "--mode="));
                    continue;
                }
                if ("--require-approval".equals(arg)) {
                    requireApproval = true;
                    continue;
                }
                if ("--approved".equals(arg)) {
                    approved = true;
                    continue;
                }
                if (arg.startsWith("--warn-threshold=")) {
                    warnThreshold = parseThreshold(valueAfterPrefix(arg, "--warn-threshold="), "--warn-threshold");
                    continue;
                }
                if (arg.startsWith("--fail-threshold=")) {
                    failThreshold = parseThreshold(valueAfterPrefix(arg, "--fail-threshold="), "--fail-threshold");
                    continue;
                }
                if ("--fail-on-threshold".equals(arg)) {
                    failOnThreshold = true;
                    continue;
                }
                if ("--no-fail-on-threshold".equals(arg)) {
                    failOnThreshold = false;
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }

            if (!help && baselineDir == null) {
                throw new IllegalArgumentException("--baseline-dir=<dir> is required");
            }
            if (!help && candidateDir == null) {
                throw new IllegalArgumentException("--candidate-dir=<dir> is required");
            }
            if (!help && outputDir == null) {
                throw new IllegalArgumentException("--output-dir=<dir> is required");
            }
            if (!help && warnThreshold < 0d) {
                throw new IllegalArgumentException("--warn-threshold must be >= 0");
            }
            if (!help && failThreshold < warnThreshold) {
                throw new IllegalArgumentException("--fail-threshold must be >= --warn-threshold");
            }

            return new Config(
                    baselineDir,
                    candidateDir,
                    outputDir,
                    mode,
                    requireApproval,
                    approved,
                    warnThreshold,
                    failThreshold,
                    failOnThreshold,
                    help);
        }

        private static double parseThreshold(final String rawValue, final String fieldName) {
            try {
                return Double.parseDouble(rawValue);
            } catch (final NumberFormatException exception) {
                throw new IllegalArgumentException(fieldName + " must be a decimal number");
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

    enum RefreshMode {
        FULL("full"),
        INCREMENTAL("incremental");

        private final String value;

        RefreshMode(final String value) {
            this.value = value;
        }

        String value() {
            return value;
        }

        static RefreshMode fromText(final String rawValue) {
            final String value = rawValue.trim().toLowerCase(Locale.ROOT);
            for (final RefreshMode mode : values()) {
                if (mode.value.equals(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("mode must be full|incremental");
        }
    }

    private record CollectionDiff(
            String namespace,
            int baselineCount,
            int candidateCount,
            int added,
            int removed,
            int changed,
            int unchanged,
            int outputDocuments,
            boolean requiresApproval,
            List<String> approvalReasons,
            String outputFile) {
        boolean isChanged() {
            return added > 0 || removed > 0 || changed > 0;
        }

        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("namespace", namespace);
            root.put("baselineCount", baselineCount);
            root.put("candidateCount", candidateCount);
            root.put("added", added);
            root.put("removed", removed);
            root.put("changed", changed);
            root.put("unchanged", unchanged);
            root.put("outputDocuments", outputDocuments);
            root.put("risk", requiresApproval ? "breaking" : "non-breaking");
            root.put("approvalReasons", approvalReasons);
            root.put("outputFile", outputFile);
            return root;
        }
    }

    private record RefreshReport(
            String startedAt,
            String finishedAt,
            String mode,
            int baselineCollections,
            int candidateCollections,
            int changedCollections,
            int driftWarningCollections,
            int driftFailingCollections,
            boolean driftHasFailures,
            boolean requiresApproval,
            boolean approved,
            List<CollectionDiff> collections) {
        static RefreshReport from(
                final Instant startedAt,
                final Instant finishedAt,
                final RefreshMode mode,
                final Map<String, List<Document>> baseline,
                final Map<String, List<Document>> candidate,
                final List<CollectionDiff> collections,
                final FixtureDriftAnalyzer.DriftReport driftReport,
                final boolean requiresApproval,
                final boolean approved) {
            final int changedCollections = (int) collections.stream().filter(CollectionDiff::isChanged).count();
            return new RefreshReport(
                    startedAt.toString(),
                    finishedAt.toString(),
                    mode.value(),
                    baseline.size(),
                    candidate.size(),
                    changedCollections,
                    driftReport.warningCollections(),
                    driftReport.failingCollections(),
                    driftReport.hasFailures(),
                    requiresApproval,
                    approved,
                    collections);
        }

        String toJson() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("startedAt", startedAt);
            root.put("finishedAt", finishedAt);
            root.put("mode", mode);
            root.put("baselineCollections", baselineCollections);
            root.put("candidateCollections", candidateCollections);
            root.put("changedCollections", changedCollections);
            root.put("driftWarningCollections", driftWarningCollections);
            root.put("driftFailingCollections", driftFailingCollections);
            root.put("driftHasFailures", driftHasFailures);
            root.put("requiresApproval", requiresApproval);
            root.put("approved", approved);
            final List<Map<String, Object>> items = new ArrayList<>(collections.size());
            for (final CollectionDiff collection : collections) {
                items.add(collection.toMap());
            }
            root.put("collections", items);
            return DiffSummaryGenerator.JsonEncoder.encode(root);
        }

        String toMarkdown() {
            final StringBuilder sb = new StringBuilder();
            sb.append("# Fixture Refresh Report\n\n");
            sb.append("- mode: ").append(mode).append("\n");
            sb.append("- baselineCollections: ").append(baselineCollections).append("\n");
            sb.append("- candidateCollections: ").append(candidateCollections).append("\n");
            sb.append("- changedCollections: ").append(changedCollections).append("\n");
            sb.append("- driftWarningCollections: ").append(driftWarningCollections).append("\n");
            sb.append("- driftFailingCollections: ").append(driftFailingCollections).append("\n");
            sb.append("- driftHasFailures: ").append(driftHasFailures).append("\n");
            sb.append("- requiresApproval: ").append(requiresApproval).append("\n");
            sb.append("- approved: ").append(approved).append("\n\n");

            sb.append("| namespace | baseline | candidate | added | removed | changed | risk | outputDocs |\n");
            sb.append("|---|---:|---:|---:|---:|---:|---|---:|\n");
            for (final CollectionDiff collection : collections) {
                sb.append("| ").append(collection.namespace())
                        .append(" | ").append(collection.baselineCount())
                        .append(" | ").append(collection.candidateCount())
                        .append(" | ").append(collection.added())
                        .append(" | ").append(collection.removed())
                        .append(" | ").append(collection.changed())
                        .append(" | ").append(collection.requiresApproval() ? "breaking" : "non-breaking")
                        .append(" | ").append(collection.outputDocuments())
                        .append(" |\n");
            }

            final List<CollectionDiff> breaking = collections.stream()
                    .filter(CollectionDiff::requiresApproval)
                    .toList();
            if (!breaking.isEmpty()) {
                sb.append("\n## Approval Required\n");
                for (final CollectionDiff diff : breaking) {
                    sb.append("- ").append(diff.namespace())
                            .append(": ").append(String.join(", ", diff.approvalReasons()))
                            .append("\n");
                }
            }
            return sb.toString();
        }
    }
}
