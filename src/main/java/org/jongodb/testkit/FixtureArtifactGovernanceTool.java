package org.jongodb.testkit;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.bson.Document;

/**
 * Artifact governance utility for versioning, retention, compatibility visibility and usage traceability.
 */
public final class FixtureArtifactGovernanceTool {
    private static final String REPORT_JSON = "fixture-artifact-governance-report.json";
    private static final String REPORT_MD = "fixture-artifact-governance-report.md";
    private static final String CHANGELOG_MD = "fixture-artifact-changelog.md";
    private static final Pattern SEMVER =
            Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:[-+][0-9A-Za-z.-]+)?$");

    private FixtureArtifactGovernanceTool() {}

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
            Files.createDirectories(config.reportDir());
            Files.createDirectories(config.artifactRoot());

            final Map<String, String> usageIndex = loadUsageIndex(config.usageIndexPath());
            if (config.registerConsumer() != null) {
                usageIndex.put(config.registerConsumer(), config.registerVersion());
                writeUsageIndex(config.usageIndexPath(), usageIndex);
            }

            final List<ArtifactEntry> artifacts = discoverArtifacts(config.artifactRoot());
            final List<ArtifactEntry> sorted = artifacts.stream()
                    .sorted(Comparator.comparing((ArtifactEntry item) -> item.versionSemver()).reversed())
                    .toList();

            final Set<String> keepVersions = new LinkedHashSet<>();
            for (int i = 0; i < sorted.size() && i < config.retain(); i++) {
                keepVersions.add(sorted.get(i).fixtureVersion());
            }
            keepVersions.addAll(config.freezeVersions());

            final List<ArtifactEntry> kept = new ArrayList<>();
            final List<ArtifactEntry> pruned = new ArrayList<>();
            for (final ArtifactEntry artifact : sorted) {
                if (keepVersions.contains(artifact.fixtureVersion())) {
                    kept.add(artifact);
                } else {
                    pruned.add(artifact);
                }
            }

            if (!config.dryRun()) {
                for (final ArtifactEntry artifact : pruned) {
                    deleteRecursively(artifact.artifactDir());
                }
            }

            final Map<String, String> unresolvedUsage = new LinkedHashMap<>();
            final Set<String> availableVersions = new LinkedHashSet<>();
            for (final ArtifactEntry artifact : kept) {
                availableVersions.add(artifact.fixtureVersion());
            }
            for (final Map.Entry<String, String> usage : usageIndex.entrySet()) {
                if (!availableVersions.contains(usage.getValue())) {
                    unresolvedUsage.put(usage.getKey(), usage.getValue());
                }
            }

            final GovernanceReport report = new GovernanceReport(
                    Instant.now().toString(),
                    config.retain(),
                    List.copyOf(config.freezeVersions()),
                    config.dryRun(),
                    kept,
                    pruned,
                    Map.copyOf(usageIndex),
                    Map.copyOf(unresolvedUsage));

            Files.writeString(config.reportDir().resolve(REPORT_JSON), report.toJson(), StandardCharsets.UTF_8);
            Files.writeString(config.reportDir().resolve(REPORT_MD), report.toMarkdown(), StandardCharsets.UTF_8);
            Files.writeString(config.reportDir().resolve(CHANGELOG_MD), report.toChangelogMarkdown(), StandardCharsets.UTF_8);

            out.println("Fixture artifact governance finished");
            out.println("- retain: " + config.retain());
            out.println("- freezeVersions: " + config.freezeVersions().size());
            out.println("- kept: " + kept.size());
            out.println("- pruned: " + pruned.size());
            out.println("- unresolvedUsage: " + unresolvedUsage.size());
            out.println("- reportJson: " + config.reportDir().resolve(REPORT_JSON));
            out.println("- reportMd: " + config.reportDir().resolve(REPORT_MD));
            out.println("- changelogMd: " + config.reportDir().resolve(CHANGELOG_MD));
            return 0;
        } catch (final IOException | RuntimeException exception) {
            err.println("fixture artifact governance failed: " + exception.getMessage());
            return 1;
        }
    }

    private static List<ArtifactEntry> discoverArtifacts(final Path artifactRoot) throws IOException {
        if (!Files.exists(artifactRoot) || !Files.isDirectory(artifactRoot)) {
            return List.of();
        }

        final List<ArtifactEntry> entries = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(artifactRoot, 3)) {
            stream.filter(path -> Files.isRegularFile(path)
                            && FixtureArtifactBundle.MANIFEST_FILE.equals(path.getFileName().toString()))
                    .forEach(path -> {
                        final Document manifest = readManifest(path);
                        final String fixtureVersion = manifest.getString("fixtureVersion");
                        if (fixtureVersion == null || !SEMVER.matcher(fixtureVersion).matches()) {
                            return;
                        }
                        final Path artifactDir = path.getParent();
                        final String createdAt = manifest.getString("createdAt") == null
                                ? ""
                                : manifest.getString("createdAt");
                        entries.add(new ArtifactEntry(
                                fixtureVersion,
                                parseSemver(fixtureVersion),
                                artifactDir,
                                path,
                                manifest.getString("artifactFormatVersion"),
                                manifest.getString("portableFormatVersion"),
                                manifest.getString("fastFormatVersion"),
                                manifest.getString("engineVersion"),
                                manifest.getString("dataSchemaHash"),
                                createdAt,
                                extractChangelog(manifest)));
                    });
        }
        return List.copyOf(entries);
    }

    private static Document readManifest(final Path path) {
        try {
            return Document.parse(Files.readString(path, StandardCharsets.UTF_8));
        } catch (final IOException exception) {
            throw new IllegalStateException("failed to read manifest: " + path, exception);
        }
    }

    private static List<String> extractChangelog(final Document manifest) {
        final Object raw = manifest.get("changelog");
        if (!(raw instanceof List<?> list)) {
            return List.of();
        }
        final List<String> lines = new ArrayList<>();
        for (final Object item : list) {
            if (item instanceof String text && !text.isBlank()) {
                lines.add(text);
            }
        }
        return List.copyOf(lines);
    }

    private static Map<String, String> loadUsageIndex(final Path usageIndexPath) throws IOException {
        if (!Files.exists(usageIndexPath)) {
            return new LinkedHashMap<>();
        }
        final Document document = Document.parse(Files.readString(usageIndexPath, StandardCharsets.UTF_8));
        final Object raw = document.get("consumers");
        if (!(raw instanceof Document consumers)) {
            return new LinkedHashMap<>();
        }
        final Map<String, String> index = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : consumers.entrySet()) {
            if (entry.getValue() instanceof String version && !version.isBlank()) {
                index.put(entry.getKey(), version);
            }
        }
        return index;
    }

    private static void writeUsageIndex(final Path usageIndexPath, final Map<String, String> usageIndex) throws IOException {
        final Document root = new Document();
        root.put("updatedAt", Instant.now().toString());
        root.put("consumers", new Document(usageIndex));
        Files.createDirectories(usageIndexPath.getParent());
        Files.writeString(usageIndexPath, DiffSummaryGenerator.JsonEncoder.encode(root), StandardCharsets.UTF_8);
    }

    private static void deleteRecursively(final Path target) throws IOException {
        if (!Files.exists(target)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(target)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException exception) {
                    throw new IllegalStateException("failed to delete " + path, exception);
                }
            });
        }
    }

    private static SemVer parseSemver(final String rawVersion) {
        final Matcher matcher = SEMVER.matcher(rawVersion);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("invalid fixture semver: " + rawVersion);
        }
        return new SemVer(
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)));
    }

    private static void printUsage(final PrintStream stream) {
        stream.println("Usage: FixtureArtifactGovernanceTool --artifact-root=<dir> [options]");
        stream.println("  --artifact-root=<dir>          Root directory containing versioned artifact folders");
        stream.println("  --retain=<n>                   Number of recent versions to keep (default: 5)");
        stream.println("  --freeze-version=v1,v2         Frozen versions to keep regardless of retention");
        stream.println("  --usage-index=<file>           Usage index json path (default: <artifact-root>/fixture-usage-index.json)");
        stream.println("  --register-consumer=<id>       Register consumer id in usage index");
        stream.println("  --register-version=<semver>    Version value for register-consumer");
        stream.println("  --report-dir=<dir>             Report output directory (default: artifact-root)");
        stream.println("  --dry-run                      Report only; do not delete old artifact directories");
        stream.println("  --help                         Show usage");
    }

    private record Config(
            Path artifactRoot,
            int retain,
            Set<String> freezeVersions,
            Path usageIndexPath,
            String registerConsumer,
            String registerVersion,
            Path reportDir,
            boolean dryRun,
            boolean help) {
        static Config fromArgs(final String[] args) {
            Path artifactRoot = null;
            int retain = 5;
            final Set<String> freezeVersions = new LinkedHashSet<>();
            Path usageIndexPath = null;
            String registerConsumer = null;
            String registerVersion = null;
            Path reportDir = null;
            boolean dryRun = false;
            boolean help = false;

            for (final String arg : args) {
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
                    continue;
                }
                if (arg.startsWith("--artifact-root=")) {
                    artifactRoot = Path.of(valueAfterPrefix(arg, "--artifact-root="));
                    continue;
                }
                if (arg.startsWith("--retain=")) {
                    retain = Integer.parseInt(valueAfterPrefix(arg, "--retain="));
                    continue;
                }
                if (arg.startsWith("--freeze-version=")) {
                    for (final String token : valueAfterPrefix(arg, "--freeze-version=").split(",")) {
                        final String version = token.trim();
                        if (!version.isBlank()) {
                            freezeVersions.add(version);
                        }
                    }
                    continue;
                }
                if (arg.startsWith("--usage-index=")) {
                    usageIndexPath = Path.of(valueAfterPrefix(arg, "--usage-index="));
                    continue;
                }
                if (arg.startsWith("--register-consumer=")) {
                    registerConsumer = valueAfterPrefix(arg, "--register-consumer=");
                    continue;
                }
                if (arg.startsWith("--register-version=")) {
                    registerVersion = valueAfterPrefix(arg, "--register-version=");
                    continue;
                }
                if (arg.startsWith("--report-dir=")) {
                    reportDir = Path.of(valueAfterPrefix(arg, "--report-dir="));
                    continue;
                }
                if ("--dry-run".equals(arg)) {
                    dryRun = true;
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }

            if (!help && artifactRoot == null) {
                throw new IllegalArgumentException("--artifact-root=<dir> is required");
            }
            if (!help && retain < 1) {
                throw new IllegalArgumentException("--retain must be >= 1");
            }
            if (!help && registerConsumer != null && (registerVersion == null || registerVersion.isBlank())) {
                throw new IllegalArgumentException("--register-version is required when --register-consumer is set");
            }
            if (!help && registerVersion != null && !SEMVER.matcher(registerVersion).matches()) {
                throw new IllegalArgumentException("--register-version must be semantic version");
            }

            if (usageIndexPath == null && artifactRoot != null) {
                usageIndexPath = artifactRoot.resolve("fixture-usage-index.json");
            }
            if (reportDir == null && artifactRoot != null) {
                reportDir = artifactRoot;
            }

            return new Config(
                    artifactRoot,
                    retain,
                    Set.copyOf(freezeVersions),
                    usageIndexPath,
                    registerConsumer,
                    registerVersion,
                    reportDir,
                    dryRun,
                    help);
        }

        private static String valueAfterPrefix(final String arg, final String prefix) {
            final String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " must have a value");
            }
            return value;
        }
    }

    private record ArtifactEntry(
            String fixtureVersion,
            SemVer versionSemver,
            Path artifactDir,
            Path manifestPath,
            String artifactFormatVersion,
            String portableFormatVersion,
            String fastFormatVersion,
            String engineVersion,
            String dataSchemaHash,
            String createdAt,
            List<String> changelog) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("fixtureVersion", fixtureVersion);
            root.put("artifactDir", artifactDir.toString());
            root.put("manifestPath", manifestPath.toString());
            root.put("artifactFormatVersion", artifactFormatVersion);
            root.put("portableFormatVersion", portableFormatVersion);
            root.put("fastFormatVersion", fastFormatVersion);
            root.put("engineVersion", engineVersion);
            root.put("dataSchemaHash", dataSchemaHash);
            root.put("createdAt", createdAt);
            root.put("changelog", changelog);
            return root;
        }
    }

    private record GovernanceReport(
            String generatedAt,
            int retain,
            List<String> freezeVersions,
            boolean dryRun,
            List<ArtifactEntry> kept,
            List<ArtifactEntry> pruned,
            Map<String, String> usageIndex,
            Map<String, String> unresolvedUsage) {
        String toJson() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("generatedAt", generatedAt);
            root.put("retain", retain);
            root.put("freezeVersions", freezeVersions);
            root.put("dryRun", dryRun);
            root.put("keptCount", kept.size());
            root.put("prunedCount", pruned.size());
            root.put("usageIndex", usageIndex);
            root.put("unresolvedUsage", unresolvedUsage);

            final List<Map<String, Object>> keptItems = new ArrayList<>(kept.size());
            for (final ArtifactEntry item : kept) {
                keptItems.add(item.toMap());
            }
            root.put("kept", keptItems);

            final List<Map<String, Object>> prunedItems = new ArrayList<>(pruned.size());
            for (final ArtifactEntry item : pruned) {
                prunedItems.add(item.toMap());
            }
            root.put("pruned", prunedItems);

            return DiffSummaryGenerator.JsonEncoder.encode(root);
        }

        String toMarkdown() {
            final StringBuilder sb = new StringBuilder();
            sb.append("# Fixture Artifact Governance Report\n\n");
            sb.append("- retain: ").append(retain).append("\n");
            sb.append("- freezeVersions: ").append(freezeVersions).append("\n");
            sb.append("- dryRun: ").append(dryRun).append("\n");
            sb.append("- keptCount: ").append(kept.size()).append("\n");
            sb.append("- prunedCount: ").append(pruned.size()).append("\n");
            sb.append("- unresolvedUsage: ").append(unresolvedUsage.size()).append("\n\n");

            sb.append("## Kept Versions\n");
            for (final ArtifactEntry item : kept) {
                sb.append("- ").append(item.fixtureVersion())
                        .append(" (engine=").append(item.engineVersion())
                        .append(", hash=").append(item.dataSchemaHash())
                        .append(")\n");
            }

            sb.append("\n## Pruned Versions\n");
            if (pruned.isEmpty()) {
                sb.append("- none\n");
            } else {
                for (final ArtifactEntry item : pruned) {
                    sb.append("- ").append(item.fixtureVersion()).append(" -> ").append(item.artifactDir()).append("\n");
                }
            }

            sb.append("\n## Usage Index\n");
            if (usageIndex.isEmpty()) {
                sb.append("- empty\n");
            } else {
                for (final Map.Entry<String, String> entry : usageIndex.entrySet()) {
                    sb.append("- ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                }
            }

            if (!unresolvedUsage.isEmpty()) {
                sb.append("\n## Unresolved Usage\n");
                for (final Map.Entry<String, String> entry : unresolvedUsage.entrySet()) {
                    sb.append("- ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                }
            }
            return sb.toString();
        }

        String toChangelogMarkdown() {
            final StringBuilder sb = new StringBuilder();
            sb.append("# Fixture Artifact Changelog\n\n");
            for (final ArtifactEntry item : kept) {
                sb.append("## ").append(item.fixtureVersion()).append("\n");
                if (item.changelog().isEmpty()) {
                    sb.append("- no changelog entries\n\n");
                    continue;
                }
                for (final String line : item.changelog()) {
                    sb.append("- ").append(line).append("\n");
                }
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    private record SemVer(int major, int minor, int patch) implements Comparable<SemVer> {
        @Override
        public int compareTo(final SemVer other) {
            int compare = Integer.compare(major, other.major);
            if (compare != 0) {
                return compare;
            }
            compare = Integer.compare(minor, other.minor);
            if (compare != 0) {
                return compare;
            }
            return Integer.compare(patch, other.patch);
        }

        @Override
        public String toString() {
            return major + "." + minor + "." + patch;
        }
    }
}
