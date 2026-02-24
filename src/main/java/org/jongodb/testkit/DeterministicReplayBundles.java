package org.jongodb.testkit;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import org.bson.Document;

/**
 * Deterministic per-failure replay bundle model and file store helpers.
 */
public final class DeterministicReplayBundles {
    public static final String DEFAULT_BUNDLE_DIR_NAME = "failure-replay-bundles";
    public static final String MANIFEST_FILE_NAME = "manifest.json";

    private static final String BUNDLE_SCHEMA_VERSION = "deterministic-replay-bundle.v1";
    private static final String MANIFEST_SCHEMA_VERSION = "deterministic-replay-manifest.v1";
    private static final int DIFF_PREVIEW_LIMIT = 5;

    private DeterministicReplayBundles() {}

    static Bundle fromFailure(final String suiteId, final DiffResult result, final List<ScenarioCommand> commands) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(commands, "commands");
        if (result.status() == DiffStatus.MATCH) {
            throw new IllegalArgumentException("MATCH results cannot be converted to replay bundles");
        }

        final List<BundleCommand> bundleCommands = new ArrayList<>(commands.size());
        for (int index = 0; index < commands.size(); index++) {
            final ScenarioCommand command = commands.get(index);
            bundleCommands.add(new BundleCommand(index, command.commandName(), canonicalizeMap(command.payload())));
        }

        final List<DiffPreview> preview = new ArrayList<>();
        final int diffLimit = Math.min(result.entries().size(), DIFF_PREVIEW_LIMIT);
        for (int i = 0; i < diffLimit; i++) {
            final DiffEntry entry = result.entries().get(i);
            preview.add(new DiffPreview(
                    requireText(entry.path(), "entry.path"),
                    canonicalizeValue(entry.leftValue()),
                    canonicalizeValue(entry.rightValue()),
                    normalizeText(entry.note())));
        }

        final String message;
        if (result.status() == DiffStatus.ERROR) {
            message = result.errorMessage().orElse("unknown differential harness error");
        } else if (result.entries().isEmpty()) {
            message = "mismatch without diff entries";
        } else {
            final DiffEntry first = result.entries().get(0);
            final String note = normalizeText(first.note());
            message = first.path() + ": " + (note == null ? "value mismatch" : note);
        }

        final ExpectedActualSummary expectedActualSummary = new ExpectedActualSummary(
                result.leftBackend() + " output should align with " + result.rightBackend(),
                result.status() == DiffStatus.ERROR
                        ? result.errorMessage().orElse("unknown differential harness error")
                        : result.rightBackend() + " diverged on " + result.entries().size() + " diff path(s)",
                result.status() == DiffStatus.ERROR ? 0 : result.entries().size(),
                List.copyOf(preview));

        final ReplayProbe replayProbe;
        if (result.status() == DiffStatus.ERROR) {
            replayProbe = new ReplayProbe(
                    "$.errorMessage",
                    canonicalizeValue(result.errorMessage().orElse("unknown differential harness error")));
        } else if (!result.entries().isEmpty()) {
            final DiffEntry first = result.entries().get(0);
            replayProbe = new ReplayProbe(first.path(), canonicalizeValue(first.rightValue()));
        } else {
            replayProbe = new ReplayProbe("$.status", result.status().name());
        }

        return new Bundle(
                BUNDLE_SCHEMA_VERSION,
                requireText(suiteId, "suiteId"),
                result.scenarioId(),
                result.status(),
                result.leftBackend(),
                result.rightBackend(),
                message,
                bundleCommands,
                expectedActualSummary,
                buildStateSnapshot(bundleCommands),
                replayProbe);
    }

    static void writeBundles(final Path bundleDir, final List<Bundle> bundles) throws IOException {
        Objects.requireNonNull(bundleDir, "bundleDir");
        Objects.requireNonNull(bundles, "bundles");
        Files.createDirectories(bundleDir);
        clearBundleDirectory(bundleDir);

        final List<Bundle> ordered = new ArrayList<>(bundles);
        ordered.sort(Comparator.comparing(Bundle::failureId));

        final List<ManifestEntry> manifestEntries = new ArrayList<>(ordered.size());
        for (final Bundle bundle : ordered) {
            final String file = bundleFileName(bundle.failureId());
            final Path path = bundleDir.resolve(file);
            Files.writeString(path, bundle.toJson(), StandardCharsets.UTF_8);
            manifestEntries.add(new ManifestEntry(bundle.failureId(), bundle.status(), file));
        }

        final Manifest manifest = new Manifest(MANIFEST_SCHEMA_VERSION, ordered.size(), manifestEntries);
        Files.writeString(bundleDir.resolve(MANIFEST_FILE_NAME), manifest.toJson(), StandardCharsets.UTF_8);
    }

    static Manifest readManifest(final Path bundleDir) throws IOException {
        Objects.requireNonNull(bundleDir, "bundleDir");
        final Path manifestPath = bundleDir.resolve(MANIFEST_FILE_NAME);
        if (!Files.exists(manifestPath)) {
            throw new IllegalArgumentException("replay bundle manifest does not exist: " + manifestPath);
        }
        return Manifest.fromJson(Files.readString(manifestPath, StandardCharsets.UTF_8));
    }

    static Bundle readBundle(final Path bundleDir, final String failureId) throws IOException {
        Objects.requireNonNull(bundleDir, "bundleDir");
        final String normalizedId = requireText(failureId, "failureId");
        final Manifest manifest = readManifest(bundleDir);

        Optional<ManifestEntry> manifestEntry = manifest.failures().stream()
                .filter(entry -> entry.failureId().equals(normalizedId))
                .findFirst();
        final Path bundlePath = manifestEntry
                .map(entry -> bundleDir.resolve(entry.file()))
                .orElse(bundleDir.resolve(bundleFileName(normalizedId)));

        if (!Files.exists(bundlePath)) {
            throw new IllegalArgumentException("replay bundle for failureId '" + normalizedId + "' not found in " + bundleDir);
        }

        final Bundle bundle = Bundle.fromJson(Files.readString(bundlePath, StandardCharsets.UTF_8));
        if (!bundle.failureId().equals(normalizedId)) {
            throw new IllegalArgumentException("bundle failureId mismatch: expected " + normalizedId + ", got " + bundle.failureId());
        }
        return bundle;
    }

    static String bundleFileName(final String failureId) {
        final String normalizedId = requireText(failureId, "failureId");
        String slug = normalizedId.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("^-+", "")
                .replaceAll("-+$", "");
        if (slug.isEmpty()) {
            slug = "failure";
        }
        if (slug.length() > 72) {
            slug = slug.substring(0, 72);
        }
        final String hash = toUnsignedHex(fnv1a64(normalizedId)).substring(0, 12);
        return slug + "-" + hash + ".json";
    }

    private static StateSnapshot buildStateSnapshot(final List<BundleCommand> commands) {
        final List<String> commandNames = new ArrayList<>(commands.size());
        int lsidCount = 0;
        int txnNumberCount = 0;
        int startTransactionCount = 0;
        int terminalTxnCommandCount = 0;
        int terminalTxnWithNumber = 0;
        int startTxnAutocommitFalseCount = 0;

        boolean contiguousIndexes = true;
        for (int index = 0; index < commands.size(); index++) {
            final BundleCommand command = commands.get(index);
            commandNames.add(command.commandName());
            if (command.index() != index) {
                contiguousIndexes = false;
            }
            if (command.payload().containsKey("lsid")) {
                lsidCount++;
            }
            if (command.payload().containsKey("txnNumber")) {
                txnNumberCount++;
            }

            final Object startTxn = command.payload().get("startTransaction");
            if (Boolean.TRUE.equals(startTxn)) {
                startTransactionCount++;
                final Object autocommit = command.payload().get("autocommit");
                if (autocommit == null || Boolean.FALSE.equals(autocommit)) {
                    startTxnAutocommitFalseCount++;
                }
            }

            final String commandLower = command.commandName().toLowerCase(Locale.ROOT);
            if ("committransaction".equals(commandLower) || "aborttransaction".equals(commandLower)) {
                terminalTxnCommandCount++;
                if (command.payload().containsKey("txnNumber")) {
                    terminalTxnWithNumber++;
                }
            }
        }

        final List<InvariantCheck> invariants = List.of(
                new InvariantCheck("commands_non_empty", !commands.isEmpty(), "commandCount=" + commands.size()),
                new InvariantCheck(
                        "command_indexes_contiguous",
                        contiguousIndexes,
                        contiguousIndexes ? "0.." + Math.max(0, commands.size() - 1) : "indexes are not contiguous"),
                new InvariantCheck(
                        "terminal_txn_has_txn_number",
                        terminalTxnCommandCount == terminalTxnWithNumber,
                        terminalTxnWithNumber + "/" + terminalTxnCommandCount),
                new InvariantCheck(
                        "start_txn_autocommit_false_when_present",
                        startTransactionCount == startTxnAutocommitFalseCount,
                        startTxnAutocommitFalseCount + "/" + startTransactionCount));

        final Map<String, Integer> envelopeCounts = new LinkedHashMap<>();
        envelopeCounts.put("lsidCommands", lsidCount);
        envelopeCounts.put("txnNumberCommands", txnNumberCount);
        envelopeCounts.put("startTransactionCommands", startTransactionCount);
        envelopeCounts.put("terminalTransactionCommands", terminalTxnCommandCount);

        final List<Map<String, Object>> digestInput = new ArrayList<>(commands.size());
        for (final BundleCommand command : commands) {
            final Map<String, Object> item = new LinkedHashMap<>();
            item.put("index", command.index());
            item.put("commandName", command.commandName());
            item.put("payload", command.payload());
            digestInput.add(item);
        }
        final String canonical = DiffSummaryGenerator.JsonEncoder.encode(digestInput);
        final String digest = toUnsignedHex(fnv1a64(canonical));

        return new StateSnapshot(commands.size(), commandNames, digest, envelopeCounts, invariants);
    }

    private static void clearBundleDirectory(final Path bundleDir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(bundleDir)) {
            for (final Path path : stream) {
                if (Files.isRegularFile(path)) {
                    Files.delete(path);
                }
            }
        }
    }

    private static long fnv1a64(final String value) {
        final String normalized = Objects.requireNonNull(value, "value");
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < normalized.length(); i++) {
            hash ^= normalized.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private static String toUnsignedHex(final long value) {
        String hex = Long.toUnsignedString(value, 16);
        if (hex.length() >= 16) {
            return hex;
        }
        final StringBuilder sb = new StringBuilder(16);
        for (int i = hex.length(); i < 16; i++) {
            sb.append('0');
        }
        sb.append(hex);
        return sb.toString();
    }

    private static String requireText(final String value, final String fieldName) {
        final String normalized = normalizeText(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String normalizeText(final String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static int requireInt(final Map<String, Object> source, final String key) {
        final Object value = source.get(key);
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(key + " must be numeric");
        }
        return number.intValue();
    }

    private static boolean requireBoolean(final Map<String, Object> source, final String key) {
        final Object value = source.get(key);
        if (!(value instanceof Boolean flag)) {
            throw new IllegalArgumentException(key + " must be boolean");
        }
        return flag;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asStringMap(final Object value, final String fieldName) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException(fieldName + " must be an object");
        }
        final Map<String, Object> normalized = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), canonicalizeValue(entry.getValue()));
        }
        return normalized;
    }

    private static List<Object> asList(final Object value, final String fieldName) {
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException(fieldName + " must be an array");
        }
        return Collections.unmodifiableList(new ArrayList<>(list));
    }

    private static Map<String, Object> canonicalizeMap(final Map<String, Object> source) {
        final Map<String, Object> canonical = new LinkedHashMap<>();
        final TreeSet<String> keys = new TreeSet<>(source.keySet());
        for (final String key : keys) {
            canonical.put(key, canonicalizeValue(source.get(key)));
        }
        return Collections.unmodifiableMap(canonical);
    }

    private static Object canonicalizeValue(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> mapValue) {
            final Map<String, Object> canonical = new LinkedHashMap<>();
            final TreeSet<String> keys = new TreeSet<>();
            for (final Object key : mapValue.keySet()) {
                keys.add(String.valueOf(key));
            }
            for (final String key : keys) {
                canonical.put(key, canonicalizeValue(mapValue.get(key)));
            }
            return Collections.unmodifiableMap(canonical);
        }
        if (value instanceof Collection<?> collectionValue) {
            final List<Object> canonical = new ArrayList<>(collectionValue.size());
            for (final Object item : collectionValue) {
                canonical.add(canonicalizeValue(item));
            }
            return Collections.unmodifiableList(canonical);
        }
        return value;
    }

    private static boolean valuesEqual(final Object left, final Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            try {
                return new BigDecimal(leftNumber.toString()).compareTo(new BigDecimal(rightNumber.toString())) == 0;
            } catch (NumberFormatException ignored) {
                return Objects.equals(left, right);
            }
        }
        if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
            if (leftMap.size() != rightMap.size()) {
                return false;
            }
            for (final Map.Entry<?, ?> entry : leftMap.entrySet()) {
                if (!rightMap.containsKey(entry.getKey())) {
                    return false;
                }
                if (!valuesEqual(entry.getValue(), rightMap.get(entry.getKey()))) {
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
                if (!valuesEqual(leftList.get(i), rightList.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return Objects.equals(left, right);
    }

    public record Bundle(
            String schemaVersion,
            String suiteId,
            String failureId,
            DiffStatus status,
            String leftBackend,
            String rightBackend,
            String message,
            List<BundleCommand> commands,
            ExpectedActualSummary expectedActualSummary,
            StateSnapshot stateSnapshot,
            ReplayProbe replayProbe) {
        public Bundle {
            schemaVersion = requireText(schemaVersion, "schemaVersion");
            suiteId = requireText(suiteId, "suiteId");
            failureId = requireText(failureId, "failureId");
            status = Objects.requireNonNull(status, "status");
            leftBackend = requireText(leftBackend, "leftBackend");
            rightBackend = requireText(rightBackend, "rightBackend");
            message = requireText(message, "message");
            commands = List.copyOf(Objects.requireNonNull(commands, "commands"));
            expectedActualSummary = Objects.requireNonNull(expectedActualSummary, "expectedActualSummary");
            stateSnapshot = Objects.requireNonNull(stateSnapshot, "stateSnapshot");
            replayProbe = Objects.requireNonNull(replayProbe, "replayProbe");
        }

        String toJson() {
            return DiffSummaryGenerator.JsonEncoder.encode(toMap());
        }

        private Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("schemaVersion", schemaVersion);
            root.put("suiteId", suiteId);
            root.put("failureId", failureId);
            root.put("status", status.name());
            root.put("leftBackend", leftBackend);
            root.put("rightBackend", rightBackend);
            root.put("message", message);

            final List<Map<String, Object>> commandItems = new ArrayList<>(commands.size());
            for (final BundleCommand command : commands) {
                final Map<String, Object> commandItem = new LinkedHashMap<>();
                commandItem.put("index", command.index());
                commandItem.put("commandName", command.commandName());
                commandItem.put("payload", command.payload());
                commandItems.add(commandItem);
            }
            root.put("commands", commandItems);

            final Map<String, Object> expectedActual = new LinkedHashMap<>();
            expectedActual.put("expectedSummary", expectedActualSummary.expectedSummary());
            expectedActual.put("actualSummary", expectedActualSummary.actualSummary());
            expectedActual.put("diffCount", expectedActualSummary.diffCount());
            final List<Map<String, Object>> diffItems = new ArrayList<>(expectedActualSummary.diffs().size());
            for (final DiffPreview diffPreview : expectedActualSummary.diffs()) {
                final Map<String, Object> item = new LinkedHashMap<>();
                item.put("path", diffPreview.path());
                item.put("expectedValue", diffPreview.expectedValue());
                item.put("actualValue", diffPreview.actualValue());
                item.put("note", diffPreview.note());
                diffItems.add(item);
            }
            expectedActual.put("diffs", diffItems);
            root.put("expectedActualSummary", expectedActual);

            final Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("commandCount", stateSnapshot.commandCount());
            snapshot.put("commandNames", stateSnapshot.commandNames());
            snapshot.put("commandDigest", stateSnapshot.commandDigest());
            snapshot.put("envelopeCounts", stateSnapshot.envelopeCounts());
            final List<Map<String, Object>> invariantItems = new ArrayList<>(stateSnapshot.invariants().size());
            for (final InvariantCheck invariant : stateSnapshot.invariants()) {
                final Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", invariant.id());
                item.put("passed", invariant.passed());
                item.put("detail", invariant.detail());
                invariantItems.add(item);
            }
            snapshot.put("invariants", invariantItems);
            root.put("stateSnapshot", snapshot);

            final Map<String, Object> probe = new LinkedHashMap<>();
            probe.put("path", replayProbe.path());
            probe.put("expectedValue", replayProbe.expectedValue());
            root.put("replayProbe", probe);

            return root;
        }

        static Bundle fromJson(final String json) {
            final Document document = Document.parse(Objects.requireNonNull(json, "json"));
            final Map<String, Object> root = asStringMap(document, "bundle");

            final String schemaVersion = requireText((String) root.get("schemaVersion"), "schemaVersion");
            final String suiteId = requireText((String) root.get("suiteId"), "suiteId");
            final String failureId = requireText((String) root.get("failureId"), "failureId");
            final DiffStatus status = DiffStatus.valueOf(requireText((String) root.get("status"), "status"));
            final String leftBackend = requireText((String) root.get("leftBackend"), "leftBackend");
            final String rightBackend = requireText((String) root.get("rightBackend"), "rightBackend");
            final String message = requireText((String) root.get("message"), "message");

            final List<Object> commandRaw = asList(root.get("commands"), "commands");
            final List<BundleCommand> commands = new ArrayList<>(commandRaw.size());
            for (final Object raw : commandRaw) {
                final Map<String, Object> item = asStringMap(raw, "commands[]");
                final int index = requireInt(item, "index");
                final String commandName = requireText((String) item.get("commandName"), "commandName");
                final Map<String, Object> payload = asStringMap(item.get("payload"), "payload");
                commands.add(new BundleCommand(index, commandName, payload));
            }

            final Map<String, Object> expectedActualRoot = asStringMap(root.get("expectedActualSummary"), "expectedActualSummary");
            final String expectedSummary = requireText((String) expectedActualRoot.get("expectedSummary"), "expectedSummary");
            final String actualSummary = requireText((String) expectedActualRoot.get("actualSummary"), "actualSummary");
            final int diffCount = requireInt(expectedActualRoot, "diffCount");
            final List<Object> diffRaw = asList(expectedActualRoot.get("diffs"), "diffs");
            final List<DiffPreview> diffs = new ArrayList<>(diffRaw.size());
            for (final Object raw : diffRaw) {
                final Map<String, Object> item = asStringMap(raw, "diffs[]");
                diffs.add(new DiffPreview(
                        requireText((String) item.get("path"), "path"),
                        item.get("expectedValue"),
                        item.get("actualValue"),
                        normalizeText((String) item.get("note"))));
            }
            final ExpectedActualSummary expectedActualSummary =
                    new ExpectedActualSummary(expectedSummary, actualSummary, diffCount, diffs);

            final Map<String, Object> snapshotRoot = asStringMap(root.get("stateSnapshot"), "stateSnapshot");
            final int commandCount = requireInt(snapshotRoot, "commandCount");
            final List<Object> commandNameRaw = asList(snapshotRoot.get("commandNames"), "commandNames");
            final List<String> commandNames = new ArrayList<>(commandNameRaw.size());
            for (final Object raw : commandNameRaw) {
                commandNames.add(requireText(String.valueOf(raw), "commandName"));
            }
            final String commandDigest = requireText((String) snapshotRoot.get("commandDigest"), "commandDigest");
            final Map<String, Object> envelopeRoot = asStringMap(snapshotRoot.get("envelopeCounts"), "envelopeCounts");
            final Map<String, Integer> envelopeCounts = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : envelopeRoot.entrySet()) {
                if (!(entry.getValue() instanceof Number number)) {
                    throw new IllegalArgumentException("envelopeCounts values must be numeric");
                }
                envelopeCounts.put(entry.getKey(), number.intValue());
            }
            final List<Object> invariantRaw = asList(snapshotRoot.get("invariants"), "invariants");
            final List<InvariantCheck> invariants = new ArrayList<>(invariantRaw.size());
            for (final Object raw : invariantRaw) {
                final Map<String, Object> item = asStringMap(raw, "invariants[]");
                invariants.add(new InvariantCheck(
                        requireText((String) item.get("id"), "id"),
                        requireBoolean(item, "passed"),
                        requireText((String) item.get("detail"), "detail")));
            }
            final StateSnapshot stateSnapshot =
                    new StateSnapshot(commandCount, commandNames, commandDigest, envelopeCounts, invariants);

            final Map<String, Object> probeRoot = asStringMap(root.get("replayProbe"), "replayProbe");
            final ReplayProbe replayProbe = new ReplayProbe(
                    requireText((String) probeRoot.get("path"), "path"),
                    probeRoot.get("expectedValue"));

            final Bundle bundle = new Bundle(
                    schemaVersion,
                    suiteId,
                    failureId,
                    status,
                    leftBackend,
                    rightBackend,
                    message,
                    commands,
                    expectedActualSummary,
                    stateSnapshot,
                    replayProbe);
            if (!valuesEqual(bundle.toMap(), root)) {
                // Ensures parse/write normalization remains deterministic.
                return new Bundle(
                        bundle.schemaVersion(),
                        bundle.suiteId(),
                        bundle.failureId(),
                        bundle.status(),
                        bundle.leftBackend(),
                        bundle.rightBackend(),
                        bundle.message(),
                        bundle.commands(),
                        bundle.expectedActualSummary(),
                        bundle.stateSnapshot(),
                        bundle.replayProbe());
            }
            return bundle;
        }
    }

    public record BundleCommand(int index, String commandName, Map<String, Object> payload) {
        public BundleCommand {
            if (index < 0) {
                throw new IllegalArgumentException("index must be >= 0");
            }
            commandName = requireText(commandName, "commandName");
            payload = canonicalizeMap(Objects.requireNonNull(payload, "payload"));
        }
    }

    public record DiffPreview(String path, Object expectedValue, Object actualValue, String note) {
        public DiffPreview {
            path = requireText(path, "path");
            expectedValue = canonicalizeValue(expectedValue);
            actualValue = canonicalizeValue(actualValue);
            note = note == null ? "" : note;
        }
    }

    public record ExpectedActualSummary(
            String expectedSummary, String actualSummary, int diffCount, List<DiffPreview> diffs) {
        public ExpectedActualSummary {
            expectedSummary = requireText(expectedSummary, "expectedSummary");
            actualSummary = requireText(actualSummary, "actualSummary");
            if (diffCount < 0) {
                throw new IllegalArgumentException("diffCount must be >= 0");
            }
            diffs = List.copyOf(Objects.requireNonNull(diffs, "diffs"));
        }
    }

    public record StateSnapshot(
            int commandCount,
            List<String> commandNames,
            String commandDigest,
            Map<String, Integer> envelopeCounts,
            List<InvariantCheck> invariants) {
        public StateSnapshot {
            if (commandCount < 0) {
                throw new IllegalArgumentException("commandCount must be >= 0");
            }
            commandNames = List.copyOf(Objects.requireNonNull(commandNames, "commandNames"));
            commandDigest = requireText(commandDigest, "commandDigest");
            envelopeCounts = Map.copyOf(Objects.requireNonNull(envelopeCounts, "envelopeCounts"));
            invariants = List.copyOf(Objects.requireNonNull(invariants, "invariants"));
        }
    }

    public record InvariantCheck(String id, boolean passed, String detail) {
        public InvariantCheck {
            id = requireText(id, "id");
            detail = requireText(detail, "detail");
        }
    }

    public record ReplayProbe(String path, Object expectedValue) {
        public ReplayProbe {
            path = requireText(path, "path");
            expectedValue = canonicalizeValue(expectedValue);
        }
    }

    public record Manifest(String schemaVersion, int bundleCount, List<ManifestEntry> failures) {
        public Manifest {
            schemaVersion = requireText(schemaVersion, "schemaVersion");
            if (bundleCount < 0) {
                throw new IllegalArgumentException("bundleCount must be >= 0");
            }
            failures = List.copyOf(Objects.requireNonNull(failures, "failures"));
        }

        String toJson() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("schemaVersion", schemaVersion);
            root.put("bundleCount", bundleCount);
            final List<Map<String, Object>> failureItems = new ArrayList<>(failures.size());
            for (final ManifestEntry failure : failures) {
                final Map<String, Object> item = new LinkedHashMap<>();
                item.put("failureId", failure.failureId());
                item.put("status", failure.status().name());
                item.put("file", failure.file());
                failureItems.add(item);
            }
            root.put("failures", failureItems);
            return DiffSummaryGenerator.JsonEncoder.encode(root);
        }

        static Manifest fromJson(final String json) {
            final Document document = Document.parse(Objects.requireNonNull(json, "json"));
            final Map<String, Object> root = asStringMap(document, "manifest");
            final String schemaVersion = requireText((String) root.get("schemaVersion"), "schemaVersion");
            final int bundleCount = requireInt(root, "bundleCount");
            final List<Object> failuresRaw = asList(root.get("failures"), "failures");
            final List<ManifestEntry> failures = new ArrayList<>(failuresRaw.size());
            for (final Object raw : failuresRaw) {
                final Map<String, Object> item = asStringMap(raw, "failures[]");
                final String failureId = requireText((String) item.get("failureId"), "failureId");
                final DiffStatus status = DiffStatus.valueOf(requireText((String) item.get("status"), "status"));
                final String file = requireText((String) item.get("file"), "file");
                failures.add(new ManifestEntry(failureId, status, file));
            }
            return new Manifest(schemaVersion, bundleCount, failures);
        }
    }

    public record ManifestEntry(String failureId, DiffStatus status, String file) {
        public ManifestEntry {
            failureId = requireText(failureId, "failureId");
            status = Objects.requireNonNull(status, "status");
            file = requireText(file, "file");
        }
    }
}
