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
 * Deterministic fixture sanitization + normalization pipeline.
 */
public final class FixtureSanitizationTool {
    private static final Pattern EMAIL_PATTERN = Pattern.compile("(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}");
    private static final Pattern PHONE_PATTERN = Pattern.compile("\\+?\\d[\\d\\-\\s]{7,}\\d");
    private static final Pattern SSN_PATTERN = Pattern.compile("\\b\\d{3}-\\d{2}-\\d{4}\\b");
    private static final String REPORT_FILE = "fixture-sanitize-report.json";

    private FixtureSanitizationTool() {}

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
            Files.createDirectories(config.outputDir());
            final SanitizationPolicy policy = SanitizationPolicy.load(config.policyFile());
            final List<Path> inputFiles = discoverInputFiles(config.inputDir());
            if (inputFiles.isEmpty()) {
                throw new IllegalArgumentException("no ndjson files found in input dir: " + config.inputDir());
            }

            final List<FileReport> fileReports = new ArrayList<>();
            final List<String> piiViolations = new ArrayList<>();
            for (final Path inputFile : inputFiles) {
                final Path outputFile = config.outputDir().resolve(inputFile.getFileName().toString());
                final FileReport report = sanitizeFile(inputFile, outputFile, policy, config.seed(), piiViolations);
                fileReports.add(report);
                out.println("Sanitized " + inputFile.getFileName() + " docs=" + report.documents() + " sha256=" + report.sha256());
            }

            final SanitizationReport report = new SanitizationReport(
                    Instant.now().toString(),
                    fileReports,
                    List.copyOf(piiViolations));
            Files.writeString(
                    config.outputDir().resolve(REPORT_FILE),
                    report.toJson(),
                    StandardCharsets.UTF_8);

            if (config.failOnPii() && !piiViolations.isEmpty()) {
                err.println("PII lint failed. violations=" + piiViolations.size());
                for (final String violation : piiViolations) {
                    err.println("- " + violation);
                }
                return 1;
            }

            out.println("Fixture sanitization finished");
            out.println("- files: " + fileReports.size());
            out.println("- piiViolations: " + piiViolations.size());
            out.println("- report: " + config.outputDir().resolve(REPORT_FILE));
            return 0;
        } catch (final IOException | RuntimeException exception) {
            err.println("fixture sanitization failed: " + exception.getMessage());
            return 1;
        }
    }

    private static FileReport sanitizeFile(
            final Path inputFile,
            final Path outputFile,
            final SanitizationPolicy policy,
            final String seed,
            final List<String> piiViolations) throws IOException {
        final List<String> inputLines = Files.readAllLines(inputFile, StandardCharsets.UTF_8);
        final List<String> sanitizedLines = new ArrayList<>();
        final MessageDigest digest = sha256();

        int processed = 0;
        for (final String line : inputLines) {
            if (line == null || line.isBlank()) {
                continue;
            }
            final Document document = Document.parse(line);
            sanitizeDocument(document, policy, seed);
            normalizeDocument(document, policy.volatileFields());

            final String canonicalJson = DiffSummaryGenerator.JsonEncoder.encode(canonicalizeValue(document));
            scanPii(canonicalJson, inputFile.getFileName().toString(), processed + 1, piiViolations);
            sanitizedLines.add(canonicalJson);
            digest.update(canonicalJson.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) '\n');
            processed++;
        }

        sanitizedLines.sort(Comparator.naturalOrder());
        final String output = sanitizedLines.isEmpty() ? "" : String.join("\n", sanitizedLines) + "\n";
        Files.writeString(outputFile, output, StandardCharsets.UTF_8);
        return new FileReport(inputFile.getFileName().toString(), outputFile.toString(), processed, toHex(digest.digest()));
    }

    private static void sanitizeDocument(final Document document, final SanitizationPolicy policy, final String seed) {
        for (final FieldRule rule : policy.rules()) {
            applyRule(document, rule, seed);
        }
    }

    private static void applyRule(final Document root, final FieldRule rule, final String seed) {
        final PathTarget target = resolveTarget(root, rule.fieldPath());
        if (!target.exists()) {
            return;
        }

        final Object value = target.value();
        final Object nextValue = switch (rule.action()) {
            case DROP -> PathTarget.DROP_SENTINEL;
            case NULLIFY -> null;
            case HASH -> hashValue(value, seed);
            case TOKENIZE -> tokenizeValue(value, seed);
            case FAKE -> fakeValue(value, seed, rule.fakeType());
        };
        target.write(nextValue);
    }

    private static Object hashValue(final Object value, final String seed) {
        if (value == null) {
            return null;
        }
        final String source = seed + "::" + value;
        return "sha256:" + toHex(sha256().digest(source.getBytes(StandardCharsets.UTF_8)));
    }

    private static Object tokenizeValue(final Object value, final String seed) {
        if (value == null) {
            return null;
        }
        final String source = seed + "::token::" + value;
        final String hex = toHex(sha256().digest(source.getBytes(StandardCharsets.UTF_8)));
        return "tok_" + hex.substring(0, 16);
    }

    private static Object fakeValue(final Object value, final String seed, final FakeType fakeType) {
        if (value == null) {
            return null;
        }
        final String source = seed + "::fake::" + value;
        final String hex = toHex(sha256().digest(source.getBytes(StandardCharsets.UTF_8)));
        if (fakeType == FakeType.EMAIL) {
            return "email-fake-" + hex.substring(0, 12);
        }
        if (fakeType == FakeType.PHONE) {
            final String digits = hexDigits(hex, 10);
            return "+1-555-" + digits.substring(0, 3) + "-" + digits.substring(3);
        }
        if (fakeType == FakeType.NAME) {
            final String[] firstNames = {"Alex", "Casey", "Jordan", "Taylor", "Morgan", "Riley", "Avery", "Parker"};
            final String[] lastNames = {"Kim", "Lee", "Park", "Choi", "Jung", "Han", "Seo", "Kang"};
            final int firstIndex = Math.abs(hex.hashCode()) % firstNames.length;
            final int lastIndex = Math.abs((hex + "x").hashCode()) % lastNames.length;
            return firstNames[firstIndex] + " " + lastNames[lastIndex];
        }
        return "fake-" + hex.substring(0, 12);
    }

    private static String hexDigits(final String hex, final int length) {
        final StringBuilder digits = new StringBuilder(length);
        int index = 0;
        while (digits.length() < length) {
            final char item = hex.charAt(index % hex.length());
            if (Character.isDigit(item)) {
                digits.append(item);
            }
            index++;
        }
        return digits.toString();
    }

    private static void normalizeDocument(final Document root, final Set<String> volatileFields) {
        for (final String path : volatileFields) {
            final PathTarget target = resolveTarget(root, path);
            if (target.exists()) {
                target.write(PathTarget.DROP_SENTINEL);
            }
        }
    }

    private static PathTarget resolveTarget(final Document root, final String path) {
        final String[] segments = path.split("\\.");
        Object current = root;
        for (int i = 0; i < segments.length - 1; i++) {
            final String segment = segments[i];
            if (!(current instanceof Document currentDocument)) {
                return PathTarget.missing();
            }
            final Object next = currentDocument.get(segment);
            if (!(next instanceof Document nextDocument)) {
                return PathTarget.missing();
            }
            current = nextDocument;
        }
        if (!(current instanceof Document owner)) {
            return PathTarget.missing();
        }
        final String leaf = segments[segments.length - 1];
        return new PathTarget(owner, leaf);
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
        if (value instanceof List<?> list) {
            final List<Object> normalized = new ArrayList<>(list.size());
            for (final Object item : list) {
                normalized.add(canonicalizeValue(item));
            }
            return normalized;
        }
        return value;
    }

    private static void scanPii(
            final String canonicalJson,
            final String fileName,
            final int lineNumber,
            final List<String> violations) {
        matchAndCollect(EMAIL_PATTERN, canonicalJson, fileName, lineNumber, "email", violations);
        matchAndCollect(PHONE_PATTERN, canonicalJson, fileName, lineNumber, "phone", violations);
        matchAndCollect(SSN_PATTERN, canonicalJson, fileName, lineNumber, "ssn", violations);
    }

    private static void matchAndCollect(
            final Pattern pattern,
            final String text,
            final String fileName,
            final int lineNumber,
            final String type,
            final List<String> violations) {
        final Matcher matcher = pattern.matcher(text);
        if (!matcher.find()) {
            return;
        }
        violations.add(fileName + ":" + lineNumber + " type=" + type + " sample=" + matcher.group());
    }

    private static List<Path> discoverInputFiles(final Path inputDir) throws IOException {
        final List<Path> files = new ArrayList<>();
        try (var stream = Files.list(inputDir)) {
            stream.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".ndjson"))
                    .sorted()
                    .forEach(files::add);
        }
        return List.copyOf(files);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
    }

    private static String toHex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (final byte item : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", item));
        }
        return sb.toString();
    }

    private static void printUsage(final PrintStream stream) {
        stream.println("Usage: FixtureSanitizationTool --input-dir=<dir> --output-dir=<dir> --policy-file=<file> [options]");
        stream.println("  --input-dir=<dir>            Directory containing *.ndjson files");
        stream.println("  --output-dir=<dir>           Sanitized output directory");
        stream.println("  --policy-file=<file>         Sanitization policy JSON file");
        stream.println("  --seed=<value>               Deterministic seed (default: fixture-sanitize-v1)");
        stream.println("  --fail-on-pii                Fail when pii lint detects violations (default)");
        stream.println("  --no-fail-on-pii             Do not fail on pii lint");
        stream.println("  --help                       Show usage");
    }

    private record Config(
            Path inputDir,
            Path outputDir,
            Path policyFile,
            String seed,
            boolean failOnPii,
            boolean help) {
        static Config fromArgs(final String[] args) {
            Path inputDir = null;
            Path outputDir = null;
            Path policyFile = null;
            String seed = "fixture-sanitize-v1";
            boolean failOnPii = true;
            boolean help = false;

            for (final String arg : args) {
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
                    continue;
                }
                if ("--fail-on-pii".equals(arg)) {
                    failOnPii = true;
                    continue;
                }
                if ("--no-fail-on-pii".equals(arg)) {
                    failOnPii = false;
                    continue;
                }
                if (arg.startsWith("--input-dir=")) {
                    inputDir = Path.of(valueAfterPrefix(arg, "--input-dir="));
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(valueAfterPrefix(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--policy-file=")) {
                    policyFile = Path.of(valueAfterPrefix(arg, "--policy-file="));
                    continue;
                }
                if (arg.startsWith("--seed=")) {
                    seed = valueAfterPrefix(arg, "--seed=");
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }

            if (!help && inputDir == null) {
                throw new IllegalArgumentException("--input-dir=<dir> is required");
            }
            if (!help && outputDir == null) {
                throw new IllegalArgumentException("--output-dir=<dir> is required");
            }
            if (!help && policyFile == null) {
                throw new IllegalArgumentException("--policy-file=<file> is required");
            }
            return new Config(inputDir, outputDir, policyFile, seed, failOnPii, help);
        }

        private static String valueAfterPrefix(final String arg, final String prefix) {
            final String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " must have a value");
            }
            return value;
        }
    }

    private record FileReport(String inputFile, String outputFile, int documents, String sha256) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("inputFile", inputFile);
            root.put("outputFile", outputFile);
            root.put("documents", documents);
            root.put("sha256", sha256);
            return root;
        }
    }

    private record SanitizationReport(
            String generatedAt,
            List<FileReport> files,
            List<String> piiViolations) {
        String toJson() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("generatedAt", generatedAt);
            final List<Map<String, Object>> fileItems = new ArrayList<>(files.size());
            for (final FileReport file : files) {
                fileItems.add(file.toMap());
            }
            root.put("files", fileItems);
            root.put("piiViolations", piiViolations);
            return DiffSummaryGenerator.JsonEncoder.encode(root);
        }
    }

    private record SanitizationPolicy(
            List<FieldRule> rules,
            Set<String> volatileFields) {
        static SanitizationPolicy load(final Path policyFile) throws IOException {
            final String content = Files.readString(policyFile, StandardCharsets.UTF_8);
            final Document root = Document.parse(content);

            final List<FieldRule> rules = new ArrayList<>();
            final Object rulesRaw = root.get("rules");
            if (rulesRaw instanceof List<?> ruleItems) {
                for (final Object ruleRaw : ruleItems) {
                    if (!(ruleRaw instanceof Document ruleDocument)) {
                        continue;
                    }
                    final String fieldPath = String.valueOf(ruleDocument.get("path"));
                    final RuleAction action = RuleAction.fromText(String.valueOf(ruleDocument.get("action")));
                    final FakeType fakeType = FakeType.fromOptionalText((String) ruleDocument.get("fakeType"));
                    rules.add(new FieldRule(fieldPath, action, fakeType));
                }
            }

            final Set<String> volatileFields = new LinkedHashSet<>();
            final Object volatileFieldsRaw = root.get("volatileFields");
            if (volatileFieldsRaw instanceof List<?> items) {
                for (final Object item : items) {
                    if (item == null) {
                        continue;
                    }
                    final String text = item.toString().trim();
                    if (!text.isEmpty()) {
                        volatileFields.add(text);
                    }
                }
            }

            return new SanitizationPolicy(List.copyOf(rules), Set.copyOf(volatileFields));
        }
    }

    private record FieldRule(String fieldPath, RuleAction action, FakeType fakeType) {}

    enum RuleAction {
        DROP("drop"),
        HASH("hash"),
        TOKENIZE("tokenize"),
        FAKE("fake"),
        NULLIFY("nullify");

        private final String value;

        RuleAction(final String value) {
            this.value = value;
        }

        static RuleAction fromText(final String rawValue) {
            final String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
            for (final RuleAction action : values()) {
                if (action.value.equals(normalized)) {
                    return action;
                }
            }
            throw new IllegalArgumentException("unsupported rule action: " + rawValue);
        }
    }

    enum FakeType {
        GENERIC("generic"),
        EMAIL("email"),
        PHONE("phone"),
        NAME("name");

        private final String value;

        FakeType(final String value) {
            this.value = value;
        }

        static FakeType fromOptionalText(final String rawValue) {
            if (rawValue == null || rawValue.isBlank()) {
                return GENERIC;
            }
            final String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
            for (final FakeType fakeType : values()) {
                if (fakeType.value.equals(normalized)) {
                    return fakeType;
                }
            }
            throw new IllegalArgumentException("unsupported fakeType: " + rawValue);
        }
    }

    private static final class PathTarget {
        static final Object DROP_SENTINEL = new Object();
        private static final PathTarget MISSING = new PathTarget(null, null);

        private final Document owner;
        private final String field;

        private PathTarget(final Document owner, final String field) {
            this.owner = owner;
            this.field = field;
        }

        static PathTarget missing() {
            return MISSING;
        }

        boolean exists() {
            return owner != null && owner.containsKey(field);
        }

        Object value() {
            if (!exists()) {
                return null;
            }
            return owner.get(field);
        }

        void write(final Object value) {
            if (owner == null) {
                return;
            }
            if (value == DROP_SENTINEL) {
                owner.remove(field);
                return;
            }
            owner.put(field, value);
        }
    }
}
