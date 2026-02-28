package org.jongodb.testkit;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Packs fixture ndjson files into dual artifacts (portable + fast).
 */
public final class FixtureArtifactTool {
    private static final Pattern SEMVER =
            Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:[-+][0-9A-Za-z.-]+)?$");

    private FixtureArtifactTool() {}

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
            final FixtureArtifactBundle.WriteResult result = FixtureArtifactBundle.writeBundleFromNdjson(
                    config.inputDir(),
                    config.outputDir(),
                    config.engineVersion(),
                    config.fixtureVersion(),
                    config.previousManifestPath());
            out.println("Fixture artifact pack finished");
            out.println("- engineVersion: " + result.engineVersion());
            out.println("- fixtureVersion: " + result.fixtureVersion());
            out.println("- dataSchemaHash: " + result.dataSchemaHash());
            out.println("- collections: " + result.collections());
            out.println("- documents: " + result.documents());
            out.println("- portable: " + result.portablePath());
            out.println("- fast: " + result.fastPath());
            out.println("- manifest: " + result.manifestPath());
            return 0;
        } catch (final IOException | RuntimeException exception) {
            err.println("fixture artifact pack failed: " + exception.getMessage());
            return 1;
        }
    }

    private static void printUsage(final PrintStream stream) {
        stream.println("Usage: FixtureArtifactTool --input-dir=<dir> --output-dir=<dir> [options]");
        stream.println("  --input-dir=<dir>           Fixture ndjson directory");
        stream.println("  --output-dir=<dir>          Output artifact directory");
        stream.println("  --engine-version=<value>    Engine compatibility version (default: runtime version)");
        stream.println("  --fixture-version=<semver>  Fixture semantic version (default: 0.1.0)");
        stream.println("  --previous-manifest=<file>  Previous manifest path for changelog generation");
        stream.println("  --help                      Show usage");
    }

    private record Config(
            Path inputDir,
            Path outputDir,
            String engineVersion,
            String fixtureVersion,
            Path previousManifestPath,
            boolean help) {
        static Config fromArgs(final String[] args) {
            Path inputDir = null;
            Path outputDir = null;
            String engineVersion = null;
            String fixtureVersion = "0.1.0";
            Path previousManifestPath = null;
            boolean help = false;

            for (final String arg : args) {
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
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
                if (arg.startsWith("--engine-version=")) {
                    engineVersion = valueAfterPrefix(arg, "--engine-version=");
                    continue;
                }
                if (arg.startsWith("--fixture-version=")) {
                    fixtureVersion = valueAfterPrefix(arg, "--fixture-version=");
                    continue;
                }
                if (arg.startsWith("--previous-manifest=")) {
                    previousManifestPath = Path.of(valueAfterPrefix(arg, "--previous-manifest="));
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
            if (engineVersion == null || engineVersion.isBlank()) {
                engineVersion = FixtureArtifactBundle.currentEngineVersion();
            }
            if (!help && !SEMVER.matcher(fixtureVersion).matches()) {
                throw new IllegalArgumentException("--fixture-version must be a semantic version (example: 1.2.3)");
            }
            return new Config(inputDir, outputDir, engineVersion, fixtureVersion, previousManifestPath, help);
        }

        private static String valueAfterPrefix(final String arg, final String prefix) {
            final String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " must have a value");
            }
            return value;
        }
    }
}
