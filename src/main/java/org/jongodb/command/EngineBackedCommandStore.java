package org.jongodb.command;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.jongodb.engine.AggregationPipeline;
import org.jongodb.engine.CollectionStore;
import org.jongodb.engine.CollationSupport;
import org.jongodb.engine.DeleteManyResult;
import org.jongodb.engine.EngineStore;
import org.jongodb.engine.InMemoryEngineStore;
import org.jongodb.engine.UnsupportedFeatureException;
import org.jongodb.engine.UpdateManyResult;

/**
 * Bridges command handlers (BSON documents) with the engine store (Document API).
 */
public final class EngineBackedCommandStore implements CommandStore {
    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();
    private static final DecoderContext DECODER_CONTEXT = DecoderContext.builder().build();
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
        return new CopyOnWriteTransactionCommandStore(baselineSnapshot);
    }

    @Override
    public void publishTransactionSnapshot(final CommandStore snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (!(engineStore instanceof InMemoryEngineStore inMemoryEngineStore)) {
            throw new IllegalStateException("transaction publish requires InMemoryEngineStore");
        }
        if (!(snapshot instanceof EngineBackedCommandStore engineSnapshot)
                || !(engineSnapshot.engineStore instanceof InMemoryEngineStore inMemorySnapshot)) {
            if (snapshot instanceof CopyOnWriteTransactionCommandStore copyOnWriteSnapshot) {
                inMemoryEngineStore.mergeTransactionSnapshot(
                        copyOnWriteSnapshot.baselineSnapshot(),
                        copyOnWriteSnapshot.transactionSnapshot());
                return;
            }
            throw new IllegalArgumentException("snapshot must be an EngineBackedCommandStore backed by InMemoryEngineStore");
        }
        if (engineSnapshot.transactionBaselineSnapshot != null) {
            inMemoryEngineStore.mergeTransactionSnapshot(engineSnapshot.transactionBaselineSnapshot, inMemorySnapshot);
            return;
        }
        inMemoryEngineStore.replaceWith(inMemorySnapshot);
    }

    @Override
    public void reset() {
        if (!(engineStore instanceof InMemoryEngineStore inMemoryEngineStore)) {
            throw new IllegalStateException("reset requires InMemoryEngineStore");
        }
        inMemoryEngineStore.replaceWith(new InMemoryEngineStore());
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
        return find(database, collection, filter, CollationSupport.Config.simple());
    }

    @Override
    public List<BsonDocument> find(
            final String database,
            final String collection,
            final BsonDocument filter,
            final CollationSupport.Config collation) {
        final CollectionStore collectionStore = engineStore.collection(database, collection);
        final Document convertedFilter = toDocumentOrEmpty(filter);
        final List<Document> foundDocuments = collectionStore.find(convertedFilter, collation);

        final List<BsonDocument> converted = new ArrayList<>(foundDocuments.size());
        for (final Document document : foundDocuments) {
            converted.add(toBsonDocument(document));
        }
        return List.copyOf(converted);
    }

    @Override
    public List<BsonDocument> aggregate(
            final String database, final String collection, final List<BsonDocument> pipeline) {
        return aggregate(database, collection, pipeline, CollationSupport.Config.simple());
    }

    @Override
    public List<BsonDocument> aggregate(
            final String database,
            final String collection,
            final List<BsonDocument> pipeline,
            final CollationSupport.Config collation) {
        Objects.requireNonNull(pipeline, "pipeline");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        final List<Document> convertedPipeline = new ArrayList<>(pipeline.size());
        for (final BsonDocument stage : pipeline) {
            convertedPipeline.add(toDocument(Objects.requireNonNull(stage, "pipeline entries must not be null")));
        }
        final TerminalWriteStagePlan writeStagePlan = resolveTerminalWriteStagePlan(List.copyOf(convertedPipeline));

        final Iterable<Document> sourceDocuments = collectionStore.scanAll();
        final List<Document> aggregatedDocuments = AggregationPipeline.execute(
                sourceDocuments,
                writeStagePlan.pipelineWithoutTerminalWrite(),
                foreignCollectionName -> engineStore.collection(database, foreignCollectionName).scanAll(),
                collation);
        if (writeStagePlan.outputCollection() != null) {
            final CollectionStore outputCollection = engineStore.collection(database, writeStagePlan.outputCollection());
            persistTerminalWrite(outputCollection, aggregatedDocuments, writeStagePlan.mode());
            return List.of();
        }
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
        final boolean existedBefore = engineStore.collectionExists(database, collection);
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
        return new CreateIndexesResult(result.numIndexesBefore(), result.numIndexesAfter(), !existedBefore);
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
            final List<Document> arrayFilters = new ArrayList<>(updateRequest.arrayFilters().size());
            for (final BsonDocument arrayFilter : updateRequest.arrayFilters()) {
                arrayFilters.add(toDocument(Objects.requireNonNull(arrayFilter, "arrayFilters entries must not be null")));
            }

            final UpdateManyResult result = collectionStore.update(
                    query, update, updateRequest.multi(), updateRequest.upsert(), List.copyOf(arrayFilters));
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
            final Document query = toDocumentOrEmpty(Objects.requireNonNull(deleteRequest.query(), "query"));
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

    private static Document toDocumentOrEmpty(final BsonDocument source) {
        return source == null || source.isEmpty() ? new Document() : toDocument(source);
    }

    private static Document toDocument(final BsonDocument source) {
        if (source.isEmpty()) {
            return new Document();
        }
        return DOCUMENT_CODEC.decode(new BsonDocumentReader(source), DECODER_CONTEXT);
    }

    private static BsonDocument toBsonDocument(final Document source) {
        if (source.isEmpty()) {
            return new BsonDocument();
        }
        return source.toBsonDocument(BsonDocument.class, DOCUMENT_CODEC_REGISTRY);
    }

    private static int toBoundedInt(final long value) {
        if (value < 0) {
            return 0;
        }
        return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    private static BsonValue toBsonValue(final Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }
        if (value instanceof BsonValue bsonValue) {
            return bsonValue;
        }
        if (value instanceof Integer intValue) {
            return new BsonInt32(intValue);
        }
        if (value instanceof Long longValue) {
            return new BsonInt64(longValue);
        }
        if (value instanceof Double doubleValue) {
            return new BsonDouble(doubleValue);
        }
        if (value instanceof String stringValue) {
            return new BsonString(stringValue);
        }
        if (value instanceof Boolean booleanValue) {
            return BsonBoolean.valueOf(booleanValue);
        }
        if (value instanceof ObjectId objectIdValue) {
            return new BsonObjectId(objectIdValue);
        }
        if (value instanceof Decimal128 decimal128Value) {
            return new BsonDecimal128(decimal128Value);
        }
        if (value instanceof Date dateValue) {
            return new BsonDateTime(dateValue.getTime());
        }
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

    private static void persistTerminalWrite(
            final CollectionStore outputCollection,
            final List<Document> aggregatedDocuments,
            final TerminalWriteMode mode) {
        if (mode == TerminalWriteMode.MERGE) {
            mergeIntoCollectionById(outputCollection, aggregatedDocuments);
            return;
        }
        outputCollection.deleteMany(new Document());
        if (!aggregatedDocuments.isEmpty()) {
            outputCollection.insertMany(aggregatedDocuments);
        }
    }

    private static void mergeIntoCollectionById(
            final CollectionStore outputCollection, final List<Document> aggregatedDocuments) {
        final List<Document> existingDocuments = outputCollection.find(new Document());
        final List<Document> mergedDocuments = new ArrayList<>(existingDocuments.size() + aggregatedDocuments.size());
        final Map<Object, Integer> indexById = new HashMap<>();

        for (final Document existingDocument : existingDocuments) {
            final Document copied = deepCopyDocument(existingDocument);
            mergedDocuments.add(copied);
            if (copied.containsKey("_id")) {
                indexById.putIfAbsent(copied.get("_id"), mergedDocuments.size() - 1);
            }
        }

        for (final Document aggregatedDocument : aggregatedDocuments) {
            final Document copied = deepCopyDocument(aggregatedDocument);
            final Object id = copied.get("_id");
            if (id == null) {
                mergedDocuments.add(copied);
                continue;
            }

            final Integer existingIndex = indexById.get(id);
            if (existingIndex == null) {
                indexById.put(id, mergedDocuments.size());
                mergedDocuments.add(copied);
                continue;
            }

            final Document merged = deepCopyDocument(mergedDocuments.get(existingIndex));
            for (final Map.Entry<String, Object> entry : copied.entrySet()) {
                merged.put(entry.getKey(), deepCopyAny(entry.getValue()));
            }
            mergedDocuments.set(existingIndex, merged);
        }

        outputCollection.deleteMany(new Document());
        if (!mergedDocuments.isEmpty()) {
            outputCollection.insertMany(mergedDocuments);
        }
    }

    private static Document deepCopyDocument(final Document document) {
        return toDocument(toBsonDocument(document));
    }

    private static Object deepCopyAny(final Object value) {
        if (value == null) {
            return null;
        }
        return toDocument(toBsonDocument(new Document("value", value))).get("value");
    }

    private static TerminalWriteStagePlan resolveTerminalWriteStagePlan(final List<Document> pipeline) {
        String outputCollection = null;
        TerminalWriteMode mode = TerminalWriteMode.NONE;
        List<Document> pipelineWithoutTerminalWrite = pipeline;
        for (int index = 0; index < pipeline.size(); index++) {
            final Document stage = pipeline.get(index);
            final boolean hasOut = stage.containsKey("$out");
            final boolean hasMerge = stage.containsKey("$merge");
            if (!hasOut && !hasMerge) {
                continue;
            }
            if (stage.size() != 1) {
                throw new IllegalArgumentException("each pipeline stage must contain exactly one field");
            }
            if (index != pipeline.size() - 1) {
                throw new UnsupportedFeatureException(
                        hasOut ? "aggregation.stage.$out.position" : "aggregation.stage.$merge.position",
                        hasOut
                                ? "$out stage must be the final pipeline stage"
                                : "$merge stage must be the final pipeline stage");
            }
            if (outputCollection != null) {
                throw new UnsupportedFeatureException(
                        hasOut ? "aggregation.stage.$out.multiple" : "aggregation.stage.$merge.multiple",
                        hasOut
                                ? "multiple $out stages are not supported"
                                : "multiple $merge stages are not supported");
            }
            if (hasOut) {
                final Object outDefinition = stage.get("$out");
                if (!(outDefinition instanceof String outCollectionName) || outCollectionName.isBlank()) {
                    throw new UnsupportedFeatureException(
                            "aggregation.stage.$out.form",
                            "$out stage currently supports string collection targets only");
                }
                outputCollection = outCollectionName.trim();
                mode = TerminalWriteMode.OUT;
            } else {
                outputCollection = parseMergeTargetCollection(stage.get("$merge"));
                mode = TerminalWriteMode.MERGE;
            }
            pipelineWithoutTerminalWrite = List.copyOf(pipeline.subList(0, index));
        }
        return new TerminalWriteStagePlan(pipelineWithoutTerminalWrite, outputCollection, mode);
    }

    private static String parseMergeTargetCollection(final Object mergeDefinition) {
        if (mergeDefinition instanceof String collectionName && !collectionName.isBlank()) {
            return collectionName.trim();
        }
        if (!(mergeDefinition instanceof Document mergeDocument)) {
            throw new UnsupportedFeatureException(
                    "aggregation.stage.$merge.form",
                    "$merge stage currently supports string targets or {into: <collection>} form only");
        }

        final Object intoDefinition = mergeDocument.get("into");
        if (intoDefinition instanceof String collectionName && !collectionName.isBlank()) {
            return collectionName.trim();
        }
        if (intoDefinition instanceof Document intoDocument) {
            final Object db = intoDocument.get("db");
            if (db instanceof String dbName && !dbName.isBlank()) {
                throw new UnsupportedFeatureException(
                        "aggregation.stage.$merge.into.db",
                        "$merge into.db targets are not supported");
            }
            final Object coll = intoDocument.get("coll");
            if (coll instanceof String collectionName && !collectionName.isBlank()) {
                return collectionName.trim();
            }
        }
        throw new UnsupportedFeatureException(
                "aggregation.stage.$merge.form",
                "$merge stage currently supports string targets or {into: <collection>} form only");
    }

    private static boolean containsTerminalWriteStage(final List<BsonDocument> pipeline) {
        Objects.requireNonNull(pipeline, "pipeline");
        for (final BsonDocument stage : pipeline) {
            if (stage != null && (stage.containsKey("$out") || stage.containsKey("$merge"))) {
                return true;
            }
        }
        return false;
    }

    static final class CopyOnWriteTransactionCommandStore implements CommandStore {
        private final InMemoryEngineStore baselineSnapshot;
        private final EngineBackedCommandStore readDelegate;
        private volatile EngineBackedCommandStore writeDelegate;

        private CopyOnWriteTransactionCommandStore(final InMemoryEngineStore baselineSnapshot) {
            this.baselineSnapshot = Objects.requireNonNull(baselineSnapshot, "baselineSnapshot");
            this.readDelegate = new EngineBackedCommandStore(baselineSnapshot, baselineSnapshot);
        }

        InMemoryEngineStore baselineSnapshot() {
            return baselineSnapshot;
        }

        InMemoryEngineStore transactionSnapshot() {
            final EngineBackedCommandStore writable = writeDelegate;
            if (writable == null) {
                return baselineSnapshot;
            }
            if (!(writable.engineStore instanceof InMemoryEngineStore inMemoryWritableStore)) {
                throw new IllegalStateException("transaction write snapshot must be InMemoryEngineStore");
            }
            return inMemoryWritableStore;
        }

        boolean materializedWriteSnapshot() {
            return writeDelegate != null;
        }

        private EngineBackedCommandStore activeReadDelegate() {
            final EngineBackedCommandStore writable = writeDelegate;
            return writable == null ? readDelegate : writable;
        }

        private synchronized EngineBackedCommandStore materializeWriteDelegate() {
            if (writeDelegate == null) {
                final InMemoryEngineStore writableSnapshot = baselineSnapshot.snapshot();
                writeDelegate = new EngineBackedCommandStore(writableSnapshot, baselineSnapshot);
            }
            return writeDelegate;
        }

        @Override
        public int insert(final String database, final String collection, final List<BsonDocument> documents) {
            return materializeWriteDelegate().insert(database, collection, documents);
        }

        @Override
        public List<BsonDocument> find(final String database, final String collection, final BsonDocument filter) {
            return activeReadDelegate().find(database, collection, filter);
        }

        @Override
        public List<BsonDocument> find(
                final String database,
                final String collection,
                final BsonDocument filter,
                final CollationSupport.Config collation) {
            return activeReadDelegate().find(database, collection, filter, collation);
        }

        @Override
        public List<BsonDocument> aggregate(
                final String database, final String collection, final List<BsonDocument> pipeline) {
            return aggregate(database, collection, pipeline, CollationSupport.Config.simple());
        }

        @Override
        public List<BsonDocument> aggregate(
                final String database,
                final String collection,
                final List<BsonDocument> pipeline,
                final CollationSupport.Config collation) {
            if (containsTerminalWriteStage(pipeline)) {
                return materializeWriteDelegate().aggregate(database, collection, pipeline, collation);
            }
            return activeReadDelegate().aggregate(database, collection, pipeline, collation);
        }

        @Override
        public void reset() {
            materializeWriteDelegate().reset();
        }

        @Override
        public CommandStore snapshotForTransaction() {
            return activeReadDelegate().snapshotForTransaction();
        }

        @Override
        public void publishTransactionSnapshot(final CommandStore snapshot) {
            activeReadDelegate().publishTransactionSnapshot(snapshot);
        }

        @Override
        public CreateIndexesResult createIndexes(
                final String database, final String collection, final List<IndexRequest> indexes) {
            return materializeWriteDelegate().createIndexes(database, collection, indexes);
        }

        @Override
        public List<IndexMetadata> listIndexes(final String database, final String collection) {
            return activeReadDelegate().listIndexes(database, collection);
        }

        @Override
        public UpdateResult update(final String database, final String collection, final List<UpdateRequest> updates) {
            return materializeWriteDelegate().update(database, collection, updates);
        }

        @Override
        public int delete(final String database, final String collection, final List<DeleteRequest> deletes) {
            return materializeWriteDelegate().delete(database, collection, deletes);
        }
    }

    private record TerminalWriteStagePlan(
            List<Document> pipelineWithoutTerminalWrite,
            String outputCollection,
            TerminalWriteMode mode) {}

    private enum TerminalWriteMode {
        NONE,
        OUT,
        MERGE
    }

}
