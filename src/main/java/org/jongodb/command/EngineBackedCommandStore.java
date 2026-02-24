package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.jongodb.engine.AggregationPipeline;
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
    private final InMemoryEngineStore transactionBaselineSnapshot;

    public EngineBackedCommandStore(final EngineStore engineStore) {
        this(engineStore, null);
    }

    private EngineBackedCommandStore(
            final EngineStore engineStore, final InMemoryEngineStore transactionBaselineSnapshot) {
        this.engineStore = Objects.requireNonNull(engineStore, "engineStore");
        this.transactionBaselineSnapshot = transactionBaselineSnapshot;
    }

    @Override
    public CommandStore snapshotForTransaction() {
        if (!(engineStore instanceof InMemoryEngineStore inMemoryEngineStore)) {
            throw new IllegalStateException("transaction snapshots require InMemoryEngineStore");
        }
        final InMemoryEngineStore baselineSnapshot = inMemoryEngineStore.snapshot();
        final InMemoryEngineStore transactionStore = baselineSnapshot.snapshot();
        return new EngineBackedCommandStore(transactionStore, baselineSnapshot);
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
        if (engineSnapshot.transactionBaselineSnapshot != null) {
            inMemoryEngineStore.mergeTransactionSnapshot(engineSnapshot.transactionBaselineSnapshot, inMemorySnapshot);
            return;
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
    public List<BsonDocument> aggregate(
            final String database, final String collection, final List<BsonDocument> pipeline) {
        Objects.requireNonNull(pipeline, "pipeline");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        final List<Document> convertedPipeline = new ArrayList<>(pipeline.size());
        for (final BsonDocument stage : pipeline) {
            convertedPipeline.add(toDocument(Objects.requireNonNull(stage, "pipeline entries must not be null")));
        }

        final List<Document> sourceDocuments = collectionStore.findAll();
        final List<Document> aggregatedDocuments = AggregationPipeline.execute(
                sourceDocuments,
                List.copyOf(convertedPipeline),
                foreignCollectionName -> engineStore.collection(database, foreignCollectionName).findAll());
        final List<BsonDocument> converted = new ArrayList<>(aggregatedDocuments.size());
        for (final Document document : aggregatedDocuments) {
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
                    index.unique(),
                    index.sparse(),
                    index.partialFilterExpression() == null ? null : toDocument(index.partialFilterExpression()),
                    index.collation() == null ? null : toDocument(index.collation()),
                    index.expireAfterSeconds()));
        }

        final CollectionStore.CreateIndexesResult result = collectionStore.createIndexes(List.copyOf(converted));
        return new CreateIndexesResult(result.numIndexesBefore(), result.numIndexesAfter());
    }

    @Override
    public List<IndexMetadata> listIndexes(final String database, final String collection) {
        final CollectionStore collectionStore = engineStore.collection(database, collection);
        final List<CollectionStore.IndexDefinition> indexes = collectionStore.listIndexes();
        final List<IndexMetadata> converted = new ArrayList<>(indexes.size());
        for (final CollectionStore.IndexDefinition index : indexes) {
            converted.add(new IndexMetadata(
                    index.name(),
                    toBsonDocument(index.key()),
                    index.unique(),
                    index.sparse(),
                    index.partialFilterExpression() == null ? null : toBsonDocument(index.partialFilterExpression()),
                    index.collation() == null ? null : toBsonDocument(index.collation()),
                    index.expireAfterSeconds()));
        }
        return List.copyOf(converted);
    }

    @Override
    public UpdateResult update(final String database, final String collection, final List<UpdateRequest> updates) {
        Objects.requireNonNull(updates, "updates");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        int matchedCount = 0;
        int modifiedCount = 0;
        final List<Upserted> upserted = new ArrayList<>();
        for (int index = 0; index < updates.size(); index++) {
            final UpdateRequest updateRequest = updates.get(index);
            final Document query = toDocument(Objects.requireNonNull(updateRequest.query(), "query"));
            final Document update = toDocument(Objects.requireNonNull(updateRequest.update(), "update"));

            final UpdateManyResult result =
                    collectionStore.update(query, update, updateRequest.multi(), updateRequest.upsert());
            matchedCount += toBoundedInt(result.matchedCount());
            modifiedCount += toBoundedInt(result.modifiedCount());
            if (result.upserted()) {
                upserted.add(new Upserted(index, toBsonValue(result.upsertedId())));
            }
        }
        return new UpdateResult(matchedCount, modifiedCount, List.copyOf(upserted));
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

    private static BsonValue toBsonValue(final Object value) {
        return toBsonDocument(new Document("value", value)).get("value");
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
