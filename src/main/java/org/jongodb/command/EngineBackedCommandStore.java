package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.jongodb.engine.CollectionStore;
import org.jongodb.engine.DeleteManyResult;
import org.jongodb.engine.EngineStore;
import org.jongodb.engine.InMemoryEngineStore;
import org.jongodb.engine.UpdateManyResult;

/**
 * Bridges command handlers (BSON documents) with the engine store (Document API).
 */
public final class EngineBackedCommandStore implements CommandStore {
    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();
    private static final CodecRegistry DOCUMENT_CODEC_REGISTRY = CodecRegistries.fromCodecs(DOCUMENT_CODEC);

    private final EngineStore engineStore;

    public EngineBackedCommandStore(final EngineStore engineStore) {
        this.engineStore = Objects.requireNonNull(engineStore, "engineStore");
    }

    @Override
    public CommandStore snapshotForTransaction() {
        if (!(engineStore instanceof InMemoryEngineStore inMemoryEngineStore)) {
            throw new IllegalStateException("transaction snapshots require InMemoryEngineStore");
        }
        return new EngineBackedCommandStore(inMemoryEngineStore.snapshot());
    }

    @Override
    public void publishTransactionSnapshot(final CommandStore snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (!(engineStore instanceof InMemoryEngineStore inMemoryEngineStore)) {
            throw new IllegalStateException("transaction publish requires InMemoryEngineStore");
        }
        if (!(snapshot instanceof EngineBackedCommandStore engineSnapshot)
                || !(engineSnapshot.engineStore instanceof InMemoryEngineStore inMemorySnapshot)) {
            throw new IllegalArgumentException("snapshot must be an EngineBackedCommandStore backed by InMemoryEngineStore");
        }
        inMemoryEngineStore.replaceWith(inMemorySnapshot);
    }

    @Override
    public int insert(final String database, final String collection, final List<BsonDocument> documents) {
        Objects.requireNonNull(documents, "documents");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        final List<Document> converted = new ArrayList<>(documents.size());
        for (final BsonDocument document : documents) {
            converted.add(toDocument(Objects.requireNonNull(document, "documents entries must not be null")));
        }

        collectionStore.insertMany(converted);
        return converted.size();
    }

    @Override
    public List<BsonDocument> find(final String database, final String collection, final BsonDocument filter) {
        final CollectionStore collectionStore = engineStore.collection(database, collection);
        final Document convertedFilter = filter == null ? new Document() : toDocument(filter);
        final List<Document> foundDocuments = collectionStore.find(convertedFilter);

        final List<BsonDocument> converted = new ArrayList<>();
        for (final Document document : foundDocuments) {
            converted.add(toBsonDocument(document));
        }
        return List.copyOf(converted);
    }

    @Override
    public CreateIndexesResult createIndexes(
            final String database, final String collection, final List<IndexRequest> indexes) {
        Objects.requireNonNull(indexes, "indexes");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        final List<CollectionStore.IndexDefinition> converted = new ArrayList<>(indexes.size());
        for (final IndexRequest index : indexes) {
            Objects.requireNonNull(index, "indexes entries must not be null");
            converted.add(new CollectionStore.IndexDefinition(
                    Objects.requireNonNull(index.name(), "index name"),
                    toDocument(Objects.requireNonNull(index.key(), "index key")),
                    index.unique()));
        }

        final CollectionStore.CreateIndexesResult result = collectionStore.createIndexes(List.copyOf(converted));
        return new CreateIndexesResult(result.numIndexesBefore(), result.numIndexesAfter());
    }

    @Override
    public UpdateResult update(final String database, final String collection, final List<UpdateRequest> updates) {
        Objects.requireNonNull(updates, "updates");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        int matchedCount = 0;
        int modifiedCount = 0;
        for (final UpdateRequest updateRequest : updates) {
            final Document query = toDocument(Objects.requireNonNull(updateRequest.query(), "query"));
            final Document update = toDocument(Objects.requireNonNull(updateRequest.update(), "update"));

            if (updateRequest.multi()) {
                final UpdateManyResult result = collectionStore.updateMany(query, update);
                matchedCount += toBoundedInt(result.matchedCount());
                modifiedCount += toBoundedInt(result.modifiedCount());
                continue;
            }

            final Document firstMatch = firstMatch(collectionStore, query);
            if (firstMatch == null) {
                continue;
            }

            final Document oneFilter = oneDocumentFilter(firstMatch);
            final UpdateManyResult oneResult = collectionStore.updateMany(oneFilter, update);
            matchedCount += Math.min(1, toBoundedInt(oneResult.matchedCount()));
            modifiedCount += Math.min(1, toBoundedInt(oneResult.modifiedCount()));
        }
        return new UpdateResult(matchedCount, modifiedCount);
    }

    @Override
    public int delete(final String database, final String collection, final List<DeleteRequest> deletes) {
        Objects.requireNonNull(deletes, "deletes");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        int deletedCount = 0;
        for (final DeleteRequest deleteRequest : deletes) {
            final Document query = toDocument(Objects.requireNonNull(deleteRequest.query(), "query"));
            if (deleteRequest.limit() == 0) {
                final DeleteManyResult result = collectionStore.deleteMany(query);
                deletedCount += toBoundedInt(result.deletedCount());
                continue;
            }

            final Document firstMatch = firstMatch(collectionStore, query);
            if (firstMatch == null) {
                continue;
            }
            final Document oneFilter = oneDocumentFilter(firstMatch);
            final DeleteManyResult oneResult = collectionStore.deleteMany(oneFilter);
            deletedCount += Math.min(1, toBoundedInt(oneResult.deletedCount()));
        }
        return deletedCount;
    }

    private static Document toDocument(final BsonDocument source) {
        return DOCUMENT_CODEC.decode(new BsonDocumentReader(source), DecoderContext.builder().build());
    }

    private static BsonDocument toBsonDocument(final Document source) {
        return source.toBsonDocument(BsonDocument.class, DOCUMENT_CODEC_REGISTRY);
    }

    private static int toBoundedInt(final long value) {
        if (value < 0) {
            return 0;
        }
        return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    private static Document firstMatch(final CollectionStore store, final Document filter) {
        final List<Document> matches = store.find(filter);
        if (matches.isEmpty()) {
            return null;
        }
        return matches.get(0);
    }

    private static Document oneDocumentFilter(final Document document) {
        if (document.containsKey("_id")) {
            return new Document("_id", document.get("_id"));
        }
        return toDocument(toBsonDocument(document));
    }
}
