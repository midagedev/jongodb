package org.jongodb.testkit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.bson.Document;

/**
 * Portable + fast dual fixture artifact encoder/decoder.
 */
final class FixtureArtifactBundle {
    static final String MANIFEST_FILE = "fixture-artifact-manifest.json";
    static final String PORTABLE_FILE = "fixture-portable.ejsonl.gz";
    static final String FAST_FILE = "fixture-fast-snapshot.bin";
    static final String SCHEMA_VERSION = "fixture-artifact.v1";
    static final String ARTIFACT_FORMAT_VERSION = "dual-artifact.v1";
    static final String PORTABLE_FORMAT_VERSION = "portable-ejsonl-gzip.v1";
    static final String FAST_FORMAT_VERSION = "fast-snapshot.v1";

    private static final int FAST_MAGIC = 0x4a464658;
    private static final int FAST_LAYOUT_VERSION = 1;
    private static final Pattern NDJSON_FILE = Pattern.compile("^([^.]+)\\.([^.]+)\\.ndjson$");

    private FixtureArtifactBundle() {}

    static WriteResult writeBundleFromNdjson(
            final Path inputDir,
            final Path outputDir,
            final String engineVersion,
            final String fixtureVersion,
            final Path previousManifestPath) throws IOException {
        final Map<String, List<Document>> collections = loadNdjsonCollections(inputDir);
        if (collections.isEmpty()) {
            throw new IllegalArgumentException("no ndjson files found in input dir: " + inputDir);
        }

        Files.createDirectories(outputDir);

        final Path portablePath = outputDir.resolve(PORTABLE_FILE);
        final int totalDocuments = writePortable(collections, portablePath);

        final Path fastPath = outputDir.resolve(FAST_FILE);
        writeFast(collections, fastPath);

        final String portableSha = sha256Hex(portablePath);
        final String fastSha = sha256Hex(fastPath);
        final String dataSchemaHash = dataSchemaHash(collections);

        writeManifest(
                outputDir.resolve(MANIFEST_FILE),
                manifestDocument(
                        collections,
                        totalDocuments,
                        engineVersion,
                        fixtureVersion,
                        dataSchemaHash,
                        portableSha,
                        fastSha,
                        buildChangelog(previousManifestPath, collections, totalDocuments)));

        return new WriteResult(
                outputDir.resolve(MANIFEST_FILE),
                portablePath,
                fastPath,
                collections.size(),
                totalDocuments,
                portableSha,
                fastSha,
                engineVersion,
                fixtureVersion,
                dataSchemaHash);
    }

    static LoadResult tryLoadBundle(
            final Path inputDir,
            final boolean regenerateFastCache,
            final String currentEngineVersion,
            final String requiredFixtureVersion,
            final PrintStream out) throws IOException {
        final Path manifestPath = inputDir.resolve(MANIFEST_FILE);
        if (!Files.exists(manifestPath)) {
            return null;
        }

        final List<String> diagnostics = new ArrayList<>();
        final Document manifest = Document.parse(Files.readString(manifestPath, StandardCharsets.UTF_8));
        final String schemaVersion = manifest.getString("schemaVersion");
        if (!SCHEMA_VERSION.equals(schemaVersion)) {
            diagnostics.add("artifact manifest schemaVersion is unsupported: " + schemaVersion);
            return new LoadResult(SourceFormat.NDJSON, Map.of(), List.copyOf(diagnostics));
        }
        enforceRequiredFixtureVersion(manifest, requiredFixtureVersion);

        final String artifactFormatVersion = manifest.getString("artifactFormatVersion");
        if (artifactFormatVersion != null && !artifactFormatVersion.isBlank()
                && !ARTIFACT_FORMAT_VERSION.equals(artifactFormatVersion)) {
            throw new IllegalArgumentException(
                    "unsupported artifactFormatVersion="
                            + artifactFormatVersion
                            + " (supported="
                            + ARTIFACT_FORMAT_VERSION
                            + ")");
        }

        final String manifestFastVersion = manifest.getString("fastFormatVersion");
        final String manifestEngineVersion = manifest.getString("engineVersion");
        final ManifestEntry fastEntry = entryFrom(manifest, "fast", FAST_FILE);
        final ManifestEntry portableEntry = entryFrom(manifest, "portable", PORTABLE_FILE);

        final Path fastPath = inputDir.resolve(fastEntry.file());
        final Path portablePath = inputDir.resolve(portableEntry.file());

        final boolean fastCompatible = FAST_FORMAT_VERSION.equals(manifestFastVersion)
                && Objects.equals(currentEngineVersion, manifestEngineVersion);
        if (fastCompatible && Files.exists(fastPath)) {
            verifyChecksum(fastPath, fastEntry.sha256(), "fast");
            final Map<String, List<Document>> collections = readFast(fastPath);
            diagnostics.add("loaded fast snapshot");
            return new LoadResult(SourceFormat.FAST, collections, List.copyOf(diagnostics));
        }

        if (!fastCompatible) {
            diagnostics.add("fast snapshot compatibility mismatch (manifest engine="
                    + manifestEngineVersion
                    + ", runtime engine="
                    + currentEngineVersion
                    + ", format="
                    + manifestFastVersion
                    + ")");
        }

        if (Files.exists(portablePath)) {
            verifyChecksum(portablePath, portableEntry.sha256(), "portable");
            final Map<String, List<Document>> collections = readPortable(portablePath);
            diagnostics.add("loaded portable bundle");

            if (regenerateFastCache) {
                writeFast(collections, fastPath);
                final String regeneratedFastSha = sha256Hex(fastPath);

                final Document nextManifest = new Document(manifest);
                nextManifest.put("engineVersion", currentEngineVersion);
                nextManifest.put("fastFormatVersion", FAST_FORMAT_VERSION);
                nextManifest.put("updatedAt", Instant.now().toString());
                nextManifest.put(
                        "fast",
                        new Document("file", fastEntry.file())
                                .append("sha256", regeneratedFastSha)
                                .append("documents", countDocuments(collections)));
                writeManifest(manifestPath, nextManifest);
                diagnostics.add("regenerated fast snapshot from portable bundle");
                out.println("Regenerated fast fixture cache: " + fastPath);
            }

            return new LoadResult(SourceFormat.PORTABLE_FALLBACK, collections, List.copyOf(diagnostics));
        }

        diagnostics.add("portable bundle is missing; fallback to ndjson files");
        return new LoadResult(SourceFormat.NDJSON, Map.of(), List.copyOf(diagnostics));
    }

    static String currentEngineVersion() {
        final Package pkg = FixtureArtifactBundle.class.getPackage();
        final String packageVersion = pkg == null ? null : pkg.getImplementationVersion();
        if (packageVersion != null && !packageVersion.isBlank()) {
            return packageVersion.trim();
        }

        final String propertyVersion = System.getProperty("jongodb.version", "").trim();
        if (!propertyVersion.isEmpty()) {
            return propertyVersion;
        }
        return "dev";
    }

    private static Map<String, List<Document>> loadNdjsonCollections(final Path inputDir) throws IOException {
        final Map<String, List<Document>> collections = new LinkedHashMap<>();
        if (!Files.exists(inputDir) || !Files.isDirectory(inputDir)) {
            throw new IllegalArgumentException("input dir is missing: " + inputDir);
        }

        try (var stream = Files.list(inputDir)) {
            stream.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".ndjson"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .forEach(path -> {
                        final Matcher matcher = NDJSON_FILE.matcher(path.getFileName().toString());
                        if (!matcher.matches()) {
                            return;
                        }
                        final String namespace = matcher.group(1) + "." + matcher.group(2);
                        final List<Document> documents = readNdjsonFile(path);
                        collections.put(namespace, documents);
                    });
        }
        return Map.copyOf(collections);
    }

    private static List<Document> readNdjsonFile(final Path file) {
        final List<Document> documents = new ArrayList<>();
        try {
            for (final String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line == null || line.isBlank()) {
                    continue;
                }
                documents.add(Document.parse(line));
            }
        } catch (final IOException ioException) {
            throw new IllegalStateException("failed to read ndjson file: " + file, ioException);
        }
        return List.copyOf(documents);
    }

    private static int writePortable(
            final Map<String, List<Document>> collections,
            final Path portablePath) throws IOException {
        int totalDocuments = 0;
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new GZIPOutputStream(Files.newOutputStream(portablePath)),
                StandardCharsets.UTF_8))) {
            final List<String> namespaces = new ArrayList<>(collections.keySet());
            namespaces.sort(String::compareTo);
            for (final String namespace : namespaces) {
                for (final Document document : collections.getOrDefault(namespace, List.of())) {
                    final Map<String, Object> envelope = new LinkedHashMap<>();
                    envelope.put("ns", namespace);
                    envelope.put("doc", canonicalizeValue(document));
                    writer.write(DiffSummaryGenerator.JsonEncoder.encode(envelope));
                    writer.newLine();
                    totalDocuments++;
                }
            }
        }
        return totalDocuments;
    }

    private static void writeFast(
            final Map<String, List<Document>> collections,
            final Path fastPath) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(fastPath)))) {
            out.writeInt(FAST_MAGIC);
            out.writeInt(FAST_LAYOUT_VERSION);

            final List<String> namespaces = new ArrayList<>(collections.keySet());
            namespaces.sort(String::compareTo);
            out.writeInt(namespaces.size());
            for (final String namespace : namespaces) {
                out.writeUTF(namespace);
                final List<Document> documents = collections.getOrDefault(namespace, List.of());
                out.writeInt(documents.size());
                for (final Document document : documents) {
                    final String canonicalJson = DiffSummaryGenerator.JsonEncoder.encode(canonicalizeValue(document));
                    final byte[] bytes = canonicalJson.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(bytes.length);
                    out.write(bytes);
                }
            }
        }
    }

    private static Map<String, List<Document>> readPortable(final Path portablePath) throws IOException {
        final Map<String, List<Document>> mutable = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(Files.newInputStream(portablePath)),
                StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                final Document envelope = Document.parse(line);
                final String namespace = envelope.getString("ns");
                if (namespace == null || namespace.isBlank()) {
                    throw new IllegalArgumentException("portable entry missing 'ns' field");
                }
                final Object docRaw = envelope.get("doc");
                if (!(docRaw instanceof Document document)) {
                    throw new IllegalArgumentException("portable entry missing 'doc' object for namespace=" + namespace);
                }
                mutable.computeIfAbsent(namespace, key -> new ArrayList<>()).add(document);
            }
        }
        return immutableCopy(mutable);
    }

    private static Map<String, List<Document>> readFast(final Path fastPath) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(fastPath)))) {
            final int magic = in.readInt();
            if (magic != FAST_MAGIC) {
                throw new IllegalArgumentException("invalid fast snapshot magic");
            }
            final int layoutVersion = in.readInt();
            if (layoutVersion != FAST_LAYOUT_VERSION) {
                throw new IllegalArgumentException("unsupported fast snapshot layout version: " + layoutVersion);
            }

            final int namespaceCount = in.readInt();
            final Map<String, List<Document>> mutable = new LinkedHashMap<>();
            for (int i = 0; i < namespaceCount; i++) {
                final String namespace = in.readUTF();
                final int documentCount = in.readInt();
                final List<Document> docs = new ArrayList<>(documentCount);
                for (int j = 0; j < documentCount; j++) {
                    final int payloadLength = in.readInt();
                    if (payloadLength < 0) {
                        throw new IllegalArgumentException("invalid fast snapshot payload length: " + payloadLength);
                    }
                    final byte[] payload = in.readNBytes(payloadLength);
                    if (payload.length != payloadLength) {
                        throw new IllegalArgumentException("unexpected EOF while reading fast snapshot payload");
                    }
                    final String json = new String(payload, StandardCharsets.UTF_8);
                    docs.add(Document.parse(json));
                }
                mutable.put(namespace, List.copyOf(docs));
            }
            return Map.copyOf(mutable);
        }
    }

    private static Document manifestDocument(
            final Map<String, List<Document>> collections,
            final int totalDocuments,
            final String engineVersion,
            final String fixtureVersion,
            final String dataSchemaHash,
            final String portableSha,
            final String fastSha,
            final List<String> changelog) {
        final Document root = new Document();
        root.put("schemaVersion", SCHEMA_VERSION);
        root.put("artifactFormatVersion", ARTIFACT_FORMAT_VERSION);
        root.put("portableFormatVersion", PORTABLE_FORMAT_VERSION);
        root.put("fastFormatVersion", FAST_FORMAT_VERSION);
        root.put("engineVersion", engineVersion);
        root.put("fixtureVersion", fixtureVersion);
        root.put("dataSchemaHash", dataSchemaHash);
        root.put("createdAt", Instant.now().toString());

        root.put(
                "portable",
                new Document("file", PORTABLE_FILE)
                        .append("sha256", portableSha)
                        .append("documents", totalDocuments));
        root.put(
                "fast",
                new Document("file", FAST_FILE)
                        .append("sha256", fastSha)
                        .append("documents", totalDocuments));

        final List<Document> collectionSummaries = new ArrayList<>();
        final List<String> namespaces = new ArrayList<>(collections.keySet());
        namespaces.sort(String::compareTo);
        for (final String namespace : namespaces) {
            collectionSummaries.add(new Document("namespace", namespace)
                    .append("documents", collections.getOrDefault(namespace, List.of()).size()));
        }
        root.put("collections", collectionSummaries);

        root.put("totals", new Document("collections", collectionSummaries.size()).append("documents", totalDocuments));
        root.put("changelog", changelog);
        return root;
    }

    private static void writeManifest(final Path manifestPath, final Document document) throws IOException {
        Files.writeString(
                manifestPath,
                DiffSummaryGenerator.JsonEncoder.encode(canonicalizeValue(document)),
                StandardCharsets.UTF_8);
    }

    private static ManifestEntry entryFrom(
            final Document manifest,
            final String key,
            final String defaultFileName) {
        final Object raw = manifest.get(key);
        if (!(raw instanceof Document entry)) {
            return new ManifestEntry(defaultFileName, "");
        }

        final String file = entry.getString("file") == null || entry.getString("file").isBlank()
                ? defaultFileName
                : entry.getString("file");
        final String sha256 = entry.getString("sha256") == null ? "" : entry.getString("sha256");
        return new ManifestEntry(file, sha256);
    }

    private static void enforceRequiredFixtureVersion(
            final Document manifest,
            final String requiredFixtureVersion) {
        if (requiredFixtureVersion == null || requiredFixtureVersion.isBlank()) {
            return;
        }
        final String actualVersion = manifest.getString("fixtureVersion");
        if (actualVersion == null || actualVersion.isBlank()) {
            throw new IllegalArgumentException(
                    "incompatible fixture version: manifest does not declare fixtureVersion (required="
                            + requiredFixtureVersion
                            + ")");
        }
        if (!requiredFixtureVersion.trim().equals(actualVersion.trim())) {
            throw new IllegalArgumentException(
                    "incompatible fixture version: required="
                            + requiredFixtureVersion
                            + ", actual="
                            + actualVersion
                            + ". Use matching artifact version or regenerate fixture bundle.");
        }
    }

    private static String dataSchemaHash(final Map<String, List<Document>> collections) {
        final MessageDigest digest = sha256();
        final List<String> namespaces = new ArrayList<>(collections.keySet());
        namespaces.sort(String::compareTo);
        for (final String namespace : namespaces) {
            digest.update(namespace.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) '\n');

            final List<Document> documents = collections.getOrDefault(namespace, List.of());
            final List<String> fields = new ArrayList<>();
            for (final Document document : documents) {
                fields.addAll(document.keySet());
            }
            fields.stream().distinct().sorted().forEach(field -> {
                digest.update(field.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) '\n');
            });
            digest.update(("docs=" + documents.size()).getBytes(StandardCharsets.UTF_8));
            digest.update((byte) '\n');
        }
        return toHex(digest.digest());
    }

    private static List<String> buildChangelog(
            final Path previousManifestPath,
            final Map<String, List<Document>> collections,
            final int totalDocuments) throws IOException {
        if (previousManifestPath == null || !Files.exists(previousManifestPath)) {
            return List.of("initial artifact publication");
        }

        final Document previous = Document.parse(Files.readString(previousManifestPath, StandardCharsets.UTF_8));
        final Document previousTotals = previous.get("totals", Document.class);
        final int previousCollectionCount = previousTotals == null ? 0 : previousTotals.getInteger("collections", 0);
        final int previousDocumentCount = previousTotals == null ? 0 : previousTotals.getInteger("documents", 0);
        final Map<String, Integer> previousCollectionDocs = collectionCountMap(previous.get("collections"));

        final List<String> changes = new ArrayList<>();
        if (previousCollectionCount != collections.size()) {
            changes.add("collections: " + previousCollectionCount + " -> " + collections.size());
        }
        if (previousDocumentCount != totalDocuments) {
            changes.add("documents: " + previousDocumentCount + " -> " + totalDocuments);
        }

        final Map<String, Integer> currentCollectionDocs = new LinkedHashMap<>();
        final List<String> currentNamespaces = new ArrayList<>(collections.keySet());
        currentNamespaces.sort(String::compareTo);
        for (final String namespace : currentNamespaces) {
            currentCollectionDocs.put(namespace, collections.getOrDefault(namespace, List.of()).size());
        }

        final List<String> unionNamespaces = new ArrayList<>();
        unionNamespaces.addAll(previousCollectionDocs.keySet());
        for (final String namespace : currentCollectionDocs.keySet()) {
            if (!unionNamespaces.contains(namespace)) {
                unionNamespaces.add(namespace);
            }
        }
        unionNamespaces.sort(String::compareTo);
        for (final String namespace : unionNamespaces) {
            final int before = previousCollectionDocs.getOrDefault(namespace, 0);
            final int after = currentCollectionDocs.getOrDefault(namespace, 0);
            if (before != after) {
                changes.add("namespace " + namespace + ": " + before + " -> " + after);
            }
        }

        if (changes.isEmpty()) {
            changes.add("no collection-level delta");
        }
        return List.copyOf(changes);
    }

    private static Map<String, Integer> collectionCountMap(final Object rawCollections) {
        if (!(rawCollections instanceof List<?> list)) {
            return Map.of();
        }
        final Map<String, Integer> counts = new LinkedHashMap<>();
        for (final Object item : list) {
            if (!(item instanceof Document document)) {
                continue;
            }
            final String namespace = document.getString("namespace");
            final Integer count = document.getInteger("documents");
            if (namespace == null || namespace.isBlank() || count == null) {
                continue;
            }
            counts.put(namespace, count);
        }
        return Map.copyOf(counts);
    }

    private static void verifyChecksum(
            final Path file,
            final String expectedSha256,
            final String label) throws IOException {
        if (expectedSha256 == null || expectedSha256.isBlank()) {
            return;
        }
        final String actual = sha256Hex(file);
        if (!expectedSha256.equals(actual)) {
            throw new IllegalArgumentException(
                    label + " checksum mismatch: expected=" + expectedSha256 + " actual=" + actual);
        }
    }

    private static String sha256Hex(final Path file) throws IOException {
        final MessageDigest digest = sha256();
        try (InputStream in = Files.newInputStream(file)) {
            final byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read == 0) {
                    continue;
                }
                digest.update(buffer, 0, read);
            }
        }
        return toHex(digest.digest());
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

    private static int countDocuments(final Map<String, List<Document>> collections) {
        int total = 0;
        for (final List<Document> docs : collections.values()) {
            total += docs.size();
        }
        return total;
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
                final Object rawKey = entry.getKey();
                if (!(rawKey instanceof String key)) {
                    continue;
                }
                sorted.put(key, canonicalizeValue(entry.getValue()));
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

    private static Map<String, List<Document>> immutableCopy(final Map<String, List<Document>> source) {
        final Map<String, List<Document>> result = new LinkedHashMap<>();
        for (final Map.Entry<String, List<Document>> entry : source.entrySet()) {
            result.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(result);
    }

    record WriteResult(
            Path manifestPath,
            Path portablePath,
            Path fastPath,
            int collections,
            int documents,
            String portableSha256,
            String fastSha256,
            String engineVersion,
            String fixtureVersion,
            String dataSchemaHash) {}

    record LoadResult(
            SourceFormat sourceFormat,
            Map<String, List<Document>> collections,
            List<String> diagnostics) {}

    record ManifestEntry(
            String file,
            String sha256) {}

    enum SourceFormat {
        FAST,
        PORTABLE_FALLBACK,
        NDJSON
    }
}
