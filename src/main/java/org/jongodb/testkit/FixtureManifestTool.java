package org.jongodb.testkit;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Objects;

/**
 * CLI utility for fixture manifest validation and deterministic plan rendering.
 */
public final class FixtureManifestTool {
    private FixtureManifestTool() {}

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
            config = parseArgs(args);
        } catch (final IllegalArgumentException e) {
            err.println(e.getMessage());
            printUsage(err);
            return 2;
        }

        if (config.help()) {
            printUsage(out);
            return 0;
        }

        try {
            final FixtureManifest manifest = FixtureManifestLoader.load(config.manifestPath());
            final FixtureExtractionPlan plan = FixtureExtractionPlanner.plan(manifest, config.profile());
            renderSummary(config.manifestPath(), plan, out);
            if (config.jsonOutput()) {
                out.println();
                out.println(plan.toJson());
            }
            return 0;
        } catch (final IOException | RuntimeException e) {
            err.println("fixture manifest tool failed: " + e.getMessage());
            return 1;
        }
    }

    private static Config parseArgs(final String[] args) {
        Path manifestPath = null;
        FixtureManifest.ScenarioProfile profile = FixtureManifest.ScenarioProfile.DEV;
        boolean jsonOutput = false;
        boolean help = false;

        for (final String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                help = true;
                continue;
            }
            if ("--json".equals(arg)) {
                jsonOutput = true;
                continue;
            }
            if (arg.startsWith("--manifest=")) {
                manifestPath = Path.of(valueAfterPrefix(arg, "--manifest="));
                continue;
            }
            if (arg.startsWith("--profile=")) {
                profile = FixtureManifest.ScenarioProfile.fromText(valueAfterPrefix(arg, "--profile="));
                continue;
            }
            throw new IllegalArgumentException("unknown argument: " + arg);
        }

        if (!help && manifestPath == null) {
            throw new IllegalArgumentException("--manifest=<path> is required");
        }
        return new Config(manifestPath, profile, jsonOutput, help);
    }

    private static void renderSummary(
            final Path manifestPath,
            final FixtureExtractionPlan plan,
            final PrintStream out) {
        out.println("Fixture manifest validated");
        out.println("- manifest: " + manifestPath.toAbsolutePath().normalize());
        out.println("- schemaVersion: " + plan.schemaVersion());
        out.println("- sourceUriAlias: " + plan.sourceUriAlias());
        out.println("- profile: " + plan.profile().value());
        out.println("- refreshMode: " + plan.refreshMode().value());
        out.println("- fingerprint: " + plan.fingerprint());
        out.println("- collectionCount: " + plan.collections().size());
        for (final FixtureExtractionPlan.CollectionPlan collection : plan.collections()) {
            out.println("  - " + collection.database() + "." + collection.collection()
                    + " limit=" + (collection.limit() == null ? "-" : collection.limit())
                    + " sample=" + (collection.sample() == null ? "-" : collection.sample().size()));
        }
    }

    private static String valueAfterPrefix(final String arg, final String prefix) {
        final String value = arg.substring(prefix.length()).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(prefix + " must have a value");
        }
        return value;
    }

    private static void printUsage(final PrintStream stream) {
        stream.println("Usage: FixtureManifestTool --manifest=<path> [--profile=dev|smoke|full] [--json]");
        stream.println("  --manifest=<path>     Manifest JSON/YAML path (required)");
        stream.println("  --profile=<name>      Profile to render (default: dev)");
        stream.println("  --json                Print canonical plan JSON");
        stream.println("  --help                Show usage");
    }

    private record Config(
            Path manifestPath,
            FixtureManifest.ScenarioProfile profile,
            boolean jsonOutput,
            boolean help) {}
}
