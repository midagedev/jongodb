package org.jongodb.engine;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/**
 * Reads and writes an engine's full state in the {@code mongodump} directory layout:
 * {@code <dir>/<db>/<collection>.bson} (concatenated BSON documents) plus
 * {@code <collection>.metadata.json} (indexes). {@code .gz} variants from {@code mongodump --gzip} are read too.
 */
public final class EngineStateFiles {
    private static final DocumentCodec CODEC = new DocumentCodec();
    private static final JsonWriterSettings JSON = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
    private static final Set<String> SKIPPED_DATABASES = Set.of("admin", "local", "config");
    private static final String ID_INDEX = "_id_";

    private EngineStateFiles() {}

    /** Writes {@code store} (except admin/local/config) to {@code dir}, replacing its previous contents only once the new dump is complete. */
    public static void export(final InMemoryEngineStore store, final Path dir) {
        Objects.requireNonNull(store, "store");
        final Path target = dir.toAbsolutePath().normalize();
        final Path staging = target.resolveSibling(target.getFileName() + ".tmp-" + System.nanoTime());
        try {
            for (final var entry : store.statesForExport().entrySet()) {
                if (SKIPPED_DATABASES.contains(entry.getKey().database())) {
                    continue;
                }
                final Path dbDir = Files.createDirectories(staging.resolve(entry.getKey().database()));
                final String collection = entry.getKey().collection();
                try (OutputStream out = Files.newOutputStream(dbDir.resolve(collection + ".bson"))) {
                    for (final Document document : entry.getValue().documents()) {
                        out.write(encode(document));
                    }
                }
                Files.writeString(
                        dbDir.resolve(collection + ".metadata.json"),
                        metadata(collection, entry.getValue().indexes()).toJson(JSON),
                        StandardCharsets.UTF_8);
            }
            Files.createDirectories(staging);
            final Path previous = target.resolveSibling(target.getFileName() + ".old-" + System.nanoTime());
            if (Files.exists(target)) {
                Files.move(target, previous, StandardCopyOption.ATOMIC_MOVE);
            }
            Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
            deleteRecursively(previous);
        } catch (final IOException exception) {
            deleteQuietly(staging);
            throw new UncheckedIOException("failed to export engine state to " + target, exception);
        }
    }

    /** Replaces {@code store}'s contents with the dump at {@code dir}. */
    public static void load(final InMemoryEngineStore store, final Path dir) {
        Objects.requireNonNull(store, "store");
        store.restoreStates(read(dir));
    }

    static Map<Namespace, InMemoryCollectionStore.CollectionState> read(final Path dir) {
        final Path root = dir.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("state directory does not exist: " + root);
        }
        final Map<Namespace, InMemoryCollectionStore.CollectionState> states = new LinkedHashMap<>();
        try (Stream<Path> databases = Files.list(root)) {
            for (final Path dbDir : databases.filter(Files::isDirectory).sorted().toList()) {
                final String database = dbDir.getFileName().toString();
                if (SKIPPED_DATABASES.contains(database)) {
                    continue;
                }
                try (Stream<Path> files = Files.list(dbDir)) {
                    for (final Path file : files.sorted().toList()) {
                        final String name = file.getFileName().toString();
                        final String collection = name.endsWith(".bson")
                                ? name.substring(0, name.length() - ".bson".length())
                                : name.endsWith(".bson.gz") ? name.substring(0, name.length() - ".bson.gz".length()) : null;
                        if (collection == null || collection.startsWith("system.")) {
                            continue;
                        }
                        states.put(
                                Namespace.of(database, collection),
                                new InMemoryCollectionStore.CollectionState(
                                        readDocuments(file), readIndexes(dbDir, collection)));
                    }
                }
            }
        } catch (final IOException exception) {
            throw new UncheckedIOException("failed to read engine state from " + root, exception);
        }
        return states;
    }

    private static List<Document> readDocuments(final Path file) throws IOException {
        final List<Document> documents = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(open(file))) {
            while (true) {
                final byte[] lengthBytes = new byte[4];
                try {
                    in.readFully(lengthBytes);
                } catch (final EOFException endOfFile) {
                    return documents;
                }
                final int length = ByteBuffer.wrap(lengthBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
                if (length < 5) {
                    throw new IOException("corrupt BSON length " + length + " in " + file);
                }
                final byte[] bytes = new byte[length];
                System.arraycopy(lengthBytes, 0, bytes, 0, 4);
                in.readFully(bytes, 4, length - 4);
                documents.add(CODEC.decode(
                        new RawBsonDocument(bytes).asBsonReader(), DecoderContext.builder().build()));
            }
        }
    }

    private static List<CollectionStore.IndexDefinition> readIndexes(final Path dbDir, final String collection)
            throws IOException {
        Path metadataFile = dbDir.resolve(collection + ".metadata.json");
        if (!Files.exists(metadataFile)) {
            metadataFile = dbDir.resolve(collection + ".metadata.json.gz");
        }
        final List<CollectionStore.IndexDefinition> indexes = new ArrayList<>();
        indexes.add(new CollectionStore.IndexDefinition(ID_INDEX, new Document("_id", 1), true, false, null, null, null));
        if (!Files.exists(metadataFile)) {
            return indexes;
        }
        final String json;
        try (InputStream in = open(metadataFile); ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            in.transferTo(buffer);
            json = buffer.toString(StandardCharsets.UTF_8);
        }
        final Object rawIndexes = Document.parse(json).get("indexes");
        if (!(rawIndexes instanceof List<?> list)) {
            return indexes;
        }
        for (final Object raw : list) {
            if (!(raw instanceof Document index) || ID_INDEX.equals(index.getString("name"))) {
                continue;
            }
            final Object ttl = index.get("expireAfterSeconds");
            indexes.add(new CollectionStore.IndexDefinition(
                    index.getString("name"),
                    index.get("key", Document.class),
                    Boolean.TRUE.equals(index.get("unique")),
                    Boolean.TRUE.equals(index.get("sparse")),
                    index.get("partialFilterExpression", Document.class),
                    index.get("collation", Document.class),
                    ttl instanceof Number number ? number.longValue() : null));
        }
        return indexes;
    }

    private static Document metadata(final String collection, final List<CollectionStore.IndexDefinition> indexes) {
        final List<Document> specs = new ArrayList<>();
        for (final CollectionStore.IndexDefinition index : indexes) {
            final Document spec = new Document("v", 2).append("key", index.key()).append("name", index.name());
            // mongorestore rejects `unique` on the _id index, which is implicitly unique.
            if (index.unique() && !ID_INDEX.equals(index.name())) {
                spec.append("unique", true);
            }
            if (index.sparse()) {
                spec.append("sparse", true);
            }
            if (index.partialFilterExpression() != null) {
                spec.append("partialFilterExpression", index.partialFilterExpression());
            }
            if (index.collation() != null) {
                spec.append("collation", index.collation());
            }
            if (index.expireAfterSeconds() != null) {
                spec.append("expireAfterSeconds", index.expireAfterSeconds());
            }
            specs.add(spec);
        }
        return new Document("indexes", specs).append("collectionName", collection);
    }

    private static byte[] encode(final Document document) {
        final BasicOutputBuffer buffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
            CODEC.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        }
        return buffer.toByteArray();
    }

    private static InputStream open(final Path file) throws IOException {
        final InputStream in = Files.newInputStream(file);
        return file.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(in) : in;
    }

    private static void deleteRecursively(final Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            for (final Path entry : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(entry);
            }
        }
    }

    private static void deleteQuietly(final Path path) {
        try {
            deleteRecursively(path);
        } catch (final IOException ignored) {
            // Best effort: the staging directory is only debris once the export has already failed.
        }
    }
}
