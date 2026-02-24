package org.jongodb.testkit;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Replays one deterministic failure bundle and validates the recorded probe path.
 */
public final class DeterministicReplayBundleRunner {
    private static final Path DEFAULT_BUNDLE_DIR = Path.of(
            "build/reports/unified-spec",
            DeterministicReplayBundles.DEFAULT_BUNDLE_DIR_NAME);

    private DeterministicReplayBundleRunner() {}

    public static void main(final String[] args) throws Exception {
        if (containsHelpFlag(args)) {
            printUsage();
            return;
        }

        final ReplayConfig config;
        try {
            config = ReplayConfig.fromArgs(args);
        } catch (final IllegalArgumentException exception) {
            System.err.println("Invalid argument: " + exception.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        final ReplayResult result = replay(config, new WireCommandIngressBackend("wire-backend-replay"));
        System.out.println("Deterministic replay executed.");
        System.out.println("- failureId: " + result.failureId());
        System.out.println("- probePath: " + result.probePath());
        System.out.println("- probeMatched: " + result.probeMatched());
        System.out.println("- success: " + result.outcome().success());
        System.out.println("- errorMessage: " + result.outcome().errorMessage().orElse(""));
        if (!result.probeMatched()) {
            System.out.println("- expectedProbeValue: " + result.expectedValue());
            System.out.println("- actualProbeValue: " + result.actualValue());
            System.exit(2);
        }
    }

    static ReplayResult replay(final ReplayConfig config, final DifferentialBackend backend) throws Exception {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(backend, "backend");
        final DeterministicReplayBundles.Bundle bundle =
                DeterministicReplayBundles.readBundle(config.bundleDir(), config.failureId());
        final List<ScenarioCommand> commands = new ArrayList<>(bundle.commands().size());
        for (final DeterministicReplayBundles.BundleCommand command : bundle.commands()) {
            commands.add(new ScenarioCommand(command.commandName(), command.payload()));
        }

        final Scenario scenario = new Scenario(bundle.failureId(), "deterministic-replay", commands);
        final ScenarioOutcome outcome = backend.execute(scenario);
        final Map<String, Object> replayState = toReplayState(outcome);
        final Object actualProbeValue = readProbePath(replayState, bundle.replayProbe().path());
        final boolean probeMatched = valuesSemanticallyEqual(bundle.replayProbe().expectedValue(), actualProbeValue);

        return new ReplayResult(
                bundle.failureId(),
                bundle.replayProbe().path(),
                bundle.replayProbe().expectedValue(),
                actualProbeValue,
                probeMatched,
                outcome);
    }

    private static Map<String, Object> toReplayState(final ScenarioOutcome outcome) {
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("success", outcome.success());
        root.put("commandResults", outcome.commandResults());
        root.put("errorMessage", outcome.errorMessage().orElse(null));
        return root;
    }

    private static Object readProbePath(final Map<String, Object> root, final String probePath) {
        if ("$".equals(probePath)) {
            return root;
        }
        if (probePath == null || !probePath.startsWith("$")) {
            throw new IllegalArgumentException("probePath must start with '$': " + probePath);
        }

        Object cursor = root;
        int index = 1;
        while (index < probePath.length()) {
            final char current = probePath.charAt(index);
            if (current == '.') {
                final int keyStart = index + 1;
                int keyEnd = keyStart;
                while (keyEnd < probePath.length()) {
                    final char token = probePath.charAt(keyEnd);
                    if (token == '.' || token == '[') {
                        break;
                    }
                    keyEnd++;
                }
                final String key = probePath.substring(keyStart, keyEnd);
                if (key.isEmpty()) {
                    throw new IllegalArgumentException("invalid probePath: " + probePath);
                }
                if (!(cursor instanceof Map<?, ?> map)) {
                    return null;
                }
                cursor = map.get(key);
                index = keyEnd;
                continue;
            }
            if (current == '[') {
                final int endBracket = probePath.indexOf(']', index);
                if (endBracket <= index + 1) {
                    throw new IllegalArgumentException("invalid probePath index segment: " + probePath);
                }
                final String indexToken = probePath.substring(index + 1, endBracket);
                final int arrayIndex;
                try {
                    arrayIndex = Integer.parseInt(indexToken);
                } catch (final NumberFormatException exception) {
                    throw new IllegalArgumentException("invalid probePath array index: " + probePath, exception);
                }
                if (!(cursor instanceof List<?> list) || arrayIndex < 0 || arrayIndex >= list.size()) {
                    return null;
                }
                cursor = list.get(arrayIndex);
                index = endBracket + 1;
                continue;
            }
            throw new IllegalArgumentException("invalid probePath token at '" + current + "': " + probePath);
        }
        return cursor;
    }

    private static boolean valuesSemanticallyEqual(final Object left, final Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return numbersEqual(leftNumber, rightNumber);
        }
        if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
            if (leftMap.size() != rightMap.size()) {
                return false;
            }
            for (final Map.Entry<?, ?> entry : leftMap.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                if (!rightMap.containsKey(key)) {
                    return false;
                }
                if (!valuesSemanticallyEqual(entry.getValue(), rightMap.get(key))) {
                    return false;
                }
            }
            return true;
        }
        if (left instanceof List<?> leftList && right instanceof List<?> rightList) {
            if (leftList.size() != rightList.size()) {
                return false;
            }
            for (int i = 0; i < leftList.size(); i++) {
                if (!valuesSemanticallyEqual(leftList.get(i), rightList.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return Objects.equals(left, right);
    }

    private static boolean numbersEqual(final Number left, final Number right) {
        try {
            return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) == 0;
        } catch (final NumberFormatException ignored) {
            return Objects.equals(left, right);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: DeterministicReplayBundleRunner [options]");
        System.out.println("  --bundle-dir=<path>    Bundle directory (default: build/reports/unified-spec/failure-replay-bundles)");
        System.out.println("  --failure-id=<id>      Failure id to replay (required)");
        System.out.println("  --help, -h             Show this help");
    }

    private static boolean containsHelpFlag(final String[] args) {
        for (final String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    public record ReplayConfig(Path bundleDir, String failureId) {
        public ReplayConfig {
            bundleDir = normalizePath(bundleDir, "bundleDir");
            failureId = requireText(failureId, "failureId");
        }

        static ReplayConfig fromArgs(final String[] args) {
            Path bundleDir = DEFAULT_BUNDLE_DIR;
            String failureId = null;
            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--bundle-dir=")) {
                    bundleDir = Path.of(requireText(valueAfterPrefix(arg, "--bundle-dir="), "bundle-dir"));
                    continue;
                }
                if (arg.startsWith("--failure-id=")) {
                    failureId = requireText(valueAfterPrefix(arg, "--failure-id="), "failure-id");
                    continue;
                }
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
            if (failureId == null) {
                throw new IllegalArgumentException("failure-id must be provided");
            }
            return new ReplayConfig(bundleDir, failureId);
        }
    }

    public record ReplayResult(
            String failureId,
            String probePath,
            Object expectedValue,
            Object actualValue,
            boolean probeMatched,
            ScenarioOutcome outcome) {
        public ReplayResult {
            failureId = requireText(failureId, "failureId");
            probePath = requireText(probePath, "probePath");
            outcome = Objects.requireNonNull(outcome, "outcome");
        }
    }

    private static String requireText(final String value, final String fieldName) {
        final String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String valueAfterPrefix(final String arg, final String prefix) {
        return arg.substring(prefix.length());
    }

    private static Path normalizePath(final Path path, final String fieldName) {
        Objects.requireNonNull(path, fieldName);
        return path.normalize();
    }
}
