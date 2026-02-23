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
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

/**
 * Aggregates external/internal Spring canary results into R2 certification evidence.
 */
public final class R2CanaryCertification {
    private static final Path DEFAULT_INPUT_JSON = Path.of("build/reports/spring-canary/projects.json");
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/r2-canary");
    private static final String CERT_JSON = "r2-canary-certification.json";
    private static final String CERT_MARKDOWN = "r2-canary-certification.md";
    private static final int MIN_PROJECTS = 3;

    private final Clock clock;

    public R2CanaryCertification() {
        this(Clock.systemUTC());
    }

    R2CanaryCertification(final Clock clock) {
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

        final R2CanaryCertification certification = new R2CanaryCertification();
        final Result result = certification.runAndWrite(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("R2 canary certification generated.");
        System.out.println("- overall: " + (result.passed() ? "PASS" : "FAIL"));
        System.out.println("- projects: " + result.projectCount());
        System.out.println("- canaryPass: " + result.canaryPassCount());
        System.out.println("- rollbackSuccess: " + result.rollbackSuccessCount());
        System.out.println("- jsonArtifact: " + paths.jsonArtifact());
        System.out.println("- markdownArtifact: " + paths.markdownArtifact());

        if (config.failOnGate() && !result.passed()) {
            System.err.println("R2 canary certification gate failed.");
            System.exit(2);
        }
    }

    public Result runAndWrite(final RunConfig config) throws IOException {
        final Result result = run(config);
        final ArtifactPaths paths = artifactPaths(config.outputDir());
        Files.createDirectories(config.outputDir());
        Files.writeString(paths.jsonArtifact(), renderJson(result), StandardCharsets.UTF_8);
        Files.writeString(paths.markdownArtifact(), renderMarkdown(result), StandardCharsets.UTF_8);
        return result;
    }

    public Result run(final RunConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        final List<String> diagnostics = new ArrayList<>();
        final List<ProjectCanary> projects = loadProjects(config.inputJson(), diagnostics);

        int canaryPass = 0;
        int rollbackSuccess = 0;
        int canaryFail = 0;
        long maxRecoverySeconds = 0L;
        for (final ProjectCanary project : projects) {
            if (project.canaryPassed()) {
                canaryPass++;
            } else {
                canaryFail++;
            }
            if (project.rollbackAttempted() && project.rollbackSucceeded()) {
                rollbackSuccess++;
            }
            maxRecoverySeconds = Math.max(maxRecoverySeconds, project.recoverySeconds());
        }

        if (projects.size() < MIN_PROJECTS) {
            diagnostics.add("expected at least " + MIN_PROJECTS + " projects but got " + projects.size());
        }
        if (canaryFail > 0) {
            diagnostics.add("canary failures detected: " + canaryFail);
        }
        if (rollbackSuccess != projects.size()) {
            diagnostics.add("rollback rehearsal must succeed for all projects");
        }

        final Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("projectCount", projects.size());
        metrics.put("canaryPass", canaryPass);
        metrics.put("canaryFail", canaryFail);
        metrics.put("rollbackSuccess", rollbackSuccess);
        metrics.put("maxRecoverySeconds", maxRecoverySeconds);

        return new Result(Instant.now(clock), List.copyOf(projects), metrics, List.copyOf(diagnostics));
    }

    static ArtifactPaths artifactPaths(final Path outputDir) {
        final Path normalized = Objects.requireNonNull(outputDir, "outputDir").normalize();
        return new ArtifactPaths(
                normalized.resolve(CERT_JSON),
                normalized.resolve(CERT_MARKDOWN));
    }

    String renderJson(final Result result) {
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", result.generatedAt().toString());
        root.put("overallStatus", result.passed() ? "PASS" : "FAIL");
        root.put("metrics", result.metrics());

        final List<Map<String, Object>> projects = new ArrayList<>();
        for (final ProjectCanary project : result.projects()) {
            final Map<String, Object> item = new LinkedHashMap<>();
            item.put("projectId", project.projectId());
            item.put("repository", project.repository());
            item.put("revision", project.revision());
            item.put("canaryPassed", project.canaryPassed());
            item.put("rollbackAttempted", project.rollbackAttempted());
            item.put("rollbackSucceeded", project.rollbackSucceeded());
            item.put("recoverySeconds", project.recoverySeconds());
            item.put("notes", project.notes());
            projects.add(item);
        }
        root.put("projects", projects);
        root.put("diagnostics", result.diagnostics());
        return QualityGateArtifactRenderer.JsonEncoder.encode(root);
    }

    String renderMarkdown(final Result result) {
        final StringBuilder sb = new StringBuilder();
        sb.append("# R2 Canary Certification\n\n");
        sb.append("- generatedAt: ").append(result.generatedAt()).append('\n');
        sb.append("- overall: ").append(result.passed() ? "PASS" : "FAIL").append('\n');
        sb.append("- projects: ").append(result.projectCount()).append('\n');
        sb.append("- canaryPass: ").append(result.canaryPassCount()).append('\n');
        sb.append("- rollbackSuccess: ").append(result.rollbackSuccessCount()).append("\n\n");

        sb.append("## Projects\n");
        for (final ProjectCanary project : result.projects()) {
            sb.append("- ")
                    .append(project.projectId())
                    .append(": canary=")
                    .append(project.canaryPassed() ? "PASS" : "FAIL")
                    .append(", rollback=")
                    .append(project.rollbackSucceeded() ? "PASS" : "FAIL")
                    .append(", recoverySeconds=")
                    .append(project.recoverySeconds())
                    .append('\n');
        }

        sb.append("\n## Diagnostics\n");
        if (result.diagnostics().isEmpty()) {
            sb.append("- none\n");
        } else {
            for (final String diagnostic : result.diagnostics()) {
                sb.append("- ").append(diagnostic).append('\n');
            }
        }
        return sb.toString();
    }

    private List<ProjectCanary> loadProjects(final Path inputJson, final List<String> diagnostics) throws IOException {
        if (!Files.exists(inputJson)) {
            diagnostics.add("missing input JSON: " + inputJson);
            return List.of();
        }

        final Document root;
        try {
            root = Document.parse(Files.readString(inputJson, StandardCharsets.UTF_8));
        } catch (final RuntimeException exception) {
            diagnostics.add("invalid input JSON: " + normalizeMessage(exception));
            return List.of();
        }

        final Object rawProjects = root.get("projects");
        if (!(rawProjects instanceof List<?> projectList)) {
            diagnostics.add("input JSON must include projects array");
            return List.of();
        }

        final List<ProjectCanary> projects = new ArrayList<>();
        for (final Object rawProject : projectList) {
            if (!(rawProject instanceof Document project)) {
                diagnostics.add("project entry must be a document");
                continue;
            }

            final String projectId = readText(project, "projectId");
            final String repository = readText(project, "repository");
            final String revision = readText(project, "revision");
            final Boolean canaryPassed = readBoolean(project, "canaryPassed");
            final Document rollback = readDocument(project, "rollback");

            if (projectId == null || repository == null || revision == null || canaryPassed == null || rollback == null) {
                diagnostics.add("project entry missing required fields: " + project.toJson());
                continue;
            }

            final Boolean rollbackAttempted = readBoolean(rollback, "attempted");
            final Boolean rollbackSucceeded = readBoolean(rollback, "success");
            final Long recoverySeconds = readLong(rollback, "recoverySeconds");
            if (rollbackAttempted == null || rollbackSucceeded == null || recoverySeconds == null) {
                diagnostics.add("rollback entry missing required fields for project " + projectId);
                continue;
            }
            projects.add(new ProjectCanary(
                    projectId,
                    repository,
                    revision,
                    canaryPassed,
                    rollbackAttempted,
                    rollbackSucceeded,
                    recoverySeconds,
                    readText(project, "notes")));
        }
        return List.copyOf(projects);
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
        System.out.println("Usage: R2CanaryCertification [options]");
        System.out.println("  --input-json=<path>        Canary project result JSON");
        System.out.println("  --output-dir=<path>        Output directory for certification artifacts");
        System.out.println("  --fail-on-gate             Exit non-zero when certification fails (default)");
        System.out.println("  --no-fail-on-gate          Always exit zero");
        System.out.println("  --help, -h                 Show help");
    }

    private static String readText(final Document document, final String key) {
        final Object value = document.get(key);
        if (!(value instanceof String stringValue) || stringValue.isBlank()) {
            return null;
        }
        return stringValue.trim();
    }

    private static Boolean readBoolean(final Document document, final String key) {
        final Object value = document.get(key);
        return value instanceof Boolean booleanValue ? booleanValue : null;
    }

    private static Long readLong(final Document document, final String key) {
        final Object value = document.get(key);
        return value instanceof Number numberValue ? numberValue.longValue() : null;
    }

    private static Document readDocument(final Document document, final String key) {
        final Object value = document.get(key);
        return value instanceof Document child ? child : null;
    }

    private static String normalizeMessage(final Throwable throwable) {
        final String message = throwable.getMessage();
        if (message != null && !message.isBlank()) {
            return message.trim();
        }
        return throwable.getClass().getSimpleName();
    }

    public record ArtifactPaths(Path jsonArtifact, Path markdownArtifact) {}

    public record ProjectCanary(
            String projectId,
            String repository,
            String revision,
            boolean canaryPassed,
            boolean rollbackAttempted,
            boolean rollbackSucceeded,
            long recoverySeconds,
            String notes) {
        public ProjectCanary {
            projectId = requireText(projectId, "projectId");
            repository = requireText(repository, "repository");
            revision = requireText(revision, "revision");
            if (recoverySeconds < 0L) {
                throw new IllegalArgumentException("recoverySeconds must be >= 0");
            }
            notes = notes == null ? "" : notes.trim();
        }

        private static String requireText(final String value, final String fieldName) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(fieldName + " must not be blank");
            }
            return value.trim();
        }
    }

    public record Result(
            Instant generatedAt,
            List<ProjectCanary> projects,
            Map<String, Object> metrics,
            List<String> diagnostics) {
        public Result {
            generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            projects = List.copyOf(Objects.requireNonNull(projects, "projects"));
            metrics = Map.copyOf(Objects.requireNonNull(metrics, "metrics"));
            diagnostics = List.copyOf(Objects.requireNonNull(diagnostics, "diagnostics"));
        }

        public boolean passed() {
            return diagnostics.isEmpty();
        }

        public int projectCount() {
            return projects.size();
        }

        public int canaryPassCount() {
            int count = 0;
            for (final ProjectCanary project : projects) {
                if (project.canaryPassed()) {
                    count++;
                }
            }
            return count;
        }

        public int rollbackSuccessCount() {
            int count = 0;
            for (final ProjectCanary project : projects) {
                if (project.rollbackAttempted() && project.rollbackSucceeded()) {
                    count++;
                }
            }
            return count;
        }
    }

    public record RunConfig(Path inputJson, Path outputDir, boolean failOnGate) {
        public RunConfig {
            inputJson = Objects.requireNonNull(inputJson, "inputJson").normalize();
            outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
        }

        static RunConfig fromArgs(final String[] args) {
            Path inputJson = DEFAULT_INPUT_JSON;
            Path outputDir = DEFAULT_OUTPUT_DIR;
            boolean failOnGate = true;

            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--input-json=")) {
                    inputJson = Path.of(readValue(arg, "--input-json="));
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(readValue(arg, "--output-dir="));
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
            return new RunConfig(inputJson, outputDir, failOnGate);
        }

        private static String readValue(final String arg, final String prefix) {
            final String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " requires a value");
            }
            return value;
        }
    }
}
