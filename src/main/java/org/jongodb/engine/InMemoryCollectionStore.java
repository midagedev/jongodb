package org.jongodb.engine;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * Basic synchronized in-memory collection store.
 */
public final class InMemoryCollectionStore implements CollectionStore {
    private final List<Document> documents = new ArrayList<>();
    private final Map<String, IndexMetadata> indexesByName = new LinkedHashMap<>();
    private final Clock clock;
    // Only the live store of an engine records changes; snapshots (transaction copies) stay silent.
    private Namespace changeNamespace;
    private ChangeEventLog changeLog;

    InMemoryCollectionStore() {
        this(Clock.systemUTC());
    }

    InMemoryCollectionStore(final Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        final Document idIndexKey = new Document("_id", 1);
        indexesByName.put(
                "_id_",
                new IndexMetadata("_id_", idIndexKey, true, false, null, null, null, List.of("_id")));
    }

    private InMemoryCollectionStore(final InMemoryCollectionStore source) {
        this.clock = source.clock;
        for (final Document sourceDocument : source.documents) {
            this.documents.add(DocumentCopies.copy(sourceDocument));
        }
        for (final Map.Entry<String, IndexMetadata> entry : source.indexesByName.entrySet()) {
            final IndexMetadata index = entry.getValue();
            this.indexesByName.put(
                    entry.getKey(),
                    new IndexMetadata(
                            index.name(),
                            DocumentCopies.copy(index.key()),
                            index.unique(),
                            index.sparse(),
                            index.partialFilterExpression() == null
                                    ? null
                                    : DocumentCopies.copy(index.partialFilterExpression()),
                            index.collation() == null ? null : DocumentCopies.copy(index.collation()),
                            index.expireAfterSeconds(),
                            List.copyOf(index.uniqueFieldPaths())));
        }
    }

    synchronized void attachChangeLog(final Namespace namespace, final ChangeEventLog log) {
        this.changeNamespace = Objects.requireNonNull(namespace, "namespace");
        this.changeLog = Objects.requireNonNull(log, "log");
    }

    synchronized InMemoryCollectionStore snapshot() {
        pruneExpiredDocuments();
        return new InMemoryCollectionStore(this);
    }

    synchronized CollectionState snapshotState() {
        pruneExpiredDocuments();
        return new CollectionState(copyDocuments(documents), listIndexes());
    }

    synchronized void replaceState(final CollectionState state) {
        Objects.requireNonNull(state, "state");

        final List<Document> copiedDocuments = copyDocuments(state.documents());
        final Map<String, IndexMetadata> copiedIndexes = toIndexMetadataMap(state.indexes());
        validateUniqueConstraints(copiedDocuments, copiedIndexes.values());

        if (changeLog != null) {
            changeLog.recordStateChange(changeNamespace, documents, copiedDocuments);
        }
        documents.clear();
        documents.addAll(copiedDocuments);
        indexesByName.clear();
        indexesByName.putAll(copiedIndexes);
    }

    static boolean statesEqual(final CollectionState left, final CollectionState right) {
        if (left == null || right == null) {
            return left == right;
        }
        return left.documents().equals(right.documents()) && left.indexes().equals(right.indexes());
    }

    static CollectionState mergeTransactionState(
            final CollectionState baselineState,
            final CollectionState transactionState,
            final CollectionState currentState) {
        final CollectionState baseline = baselineState == null ? emptyState() : baselineState;
        final CollectionState transaction = transactionState == null ? emptyState() : transactionState;
        final CollectionState current = currentState == null ? emptyState() : currentState;

        final List<Document> mergedDocuments = mergeTransactionDocuments(
                baseline.documents(), transaction.documents(), current.documents());

        final List<CollectionStore.IndexDefinition> mergedIndexes = mergeIndexes(
                baseline.indexes(),
                transaction.indexes(),
                current.indexes());

        return new CollectionState(mergedDocuments, mergedIndexes);
    }

    public synchronized <T> T inLock(final Supplier<T> action) {
        return Objects.requireNonNull(action, "action").get();
    }

    @Override
    public synchronized void insertMany(List<Document> documents) {
        Objects.requireNonNull(documents, "documents");
        pruneExpiredDocuments();

        List<Document> copiedDocuments = new ArrayList<>(documents.size());
        for (Document document : documents) {
            if (document == null) {
                throw new IllegalArgumentException("documents must not contain null");
            }
            final Document copiedDocument = DocumentCopies.copy(document);
            if (!copiedDocument.containsKey("_id")) {
                copiedDocument.put("_id", new ObjectId());
            }
            copiedDocuments.add(copiedDocument);
        }

        List<Document> candidateDocuments = new ArrayList<>(this.documents.size() + copiedDocuments.size());
        candidateDocuments.addAll(this.documents);
        candidateDocuments.addAll(copiedDocuments);
        validateUniqueConstraints(candidateDocuments, indexesByName.values());

        this.documents.addAll(copiedDocuments);
        for (final Document inserted : copiedDocuments) {
            recordInsert(inserted);
        }
    }

    @Override
    public synchronized CreateIndexesResult createIndexes(List<IndexDefinition> indexes) {
        Objects.requireNonNull(indexes, "indexes");
        pruneExpiredDocuments();

        final int numIndexesBefore = indexesByName.size();
        final Map<String, IndexMetadata> candidateIndexes = new LinkedHashMap<>(indexesByName);
        for (IndexDefinition index : indexes) {
            if (index == null) {
                throw new IllegalArgumentException("indexes must not contain null");
            }

            final String name = requireNonBlankIndexName(index.name());
            final Document key = requireNonEmptyIndexKey(index.key());
            final IndexMetadata existing = candidateIndexes.get(name);
            if (existing != null) {
                if (sameIndexSpec(existing, key, index) || idIndexRecreation(name, key, index)) {
                    continue;
                }
                if (sameIndexIdentity(existing, key, index)) {
                    throw new IndexConflictException(
                            85,
                            "IndexOptionsConflict",
                            "An equivalent index already exists with the same name but different options.");
                }
                throw new IndexConflictException(
                        86,
                        "IndexKeySpecsConflict",
                        "An existing index has the same name as the requested index.");
            }

            for (final IndexMetadata other : candidateIndexes.values()) {
                if (!sameIndexIdentity(other, key, index)) {
                    continue;
                }
                if (Objects.equals(other.expireAfterSeconds(), index.expireAfterSeconds())) {
                    throw new IndexConflictException(
                            85,
                            "IndexOptionsConflict",
                            "Index already exists with a different name: " + other.name());
                }
                throw new IndexConflictException(
                        85,
                        "IndexOptionsConflict",
                        "An equivalent index already exists with a different name and options.");
            }

            final List<String> uniqueFieldPaths = index.unique() ? indexFieldPaths(key) : List.of();
            final Document partialFilterExpression =
                    index.partialFilterExpression() == null ? null : DocumentCopies.copy(index.partialFilterExpression());
            final Document collation = index.collation() == null ? null : DocumentCopies.copy(index.collation());
            candidateIndexes.put(
                    name,
                    new IndexMetadata(
                            name,
                            key,
                            index.unique(),
                            index.sparse(),
                            partialFilterExpression,
                            collation,
                            index.expireAfterSeconds(),
                            uniqueFieldPaths));
        }

        validateUniqueConstraints(documents, candidateIndexes.values());

        indexesByName.clear();
        indexesByName.putAll(candidateIndexes);
        return new CreateIndexesResult(numIndexesBefore, indexesByName.size());
    }

    @Override
    public synchronized List<IndexDefinition> listIndexes() {
        pruneExpiredDocuments();
        final List<IndexDefinition> listed = new ArrayList<>(indexesByName.size());
        for (final IndexMetadata metadata : indexesByName.values()) {
            listed.add(new IndexDefinition(
                    metadata.name(),
                    metadata.key(),
                    metadata.unique(),
                    metadata.sparse(),
                    metadata.partialFilterExpression(),
                    metadata.collation(),
                    metadata.expireAfterSeconds()));
        }
        return List.copyOf(listed);
    }

    @Override
    public synchronized void dropIndexes(final List<String> names) {
        for (final String name : names) {
            if (!"_id_".equals(name)) {
                indexesByName.remove(name);
            }
        }
    }

    @Override
    public synchronized Long modifyIndexTtl(final String name, final Long expireAfterSeconds) {
        final IndexMetadata index = indexesByName.get(name);
        if (index == null) {
            throw new IllegalArgumentException("cannot find index " + name);
        }
        indexesByName.put(
                name,
                new IndexMetadata(
                        index.name(),
                        index.key(),
                        index.unique(),
                        index.sparse(),
                        index.partialFilterExpression(),
                        index.collation(),
                        expireAfterSeconds,
                        index.uniqueFieldPaths()));
        return index.expireAfterSeconds();
    }

    @Override
    public synchronized List<Document> findAll() {
        pruneExpiredDocuments();
        return copyMatchingDocuments(new Document(), CollationSupport.Config.simple());
    }

    @Override
    public synchronized Iterable<Document> scanAll() {
        pruneExpiredDocuments();
        final List<Document> snapshot = List.copyOf(documents);
        return () -> new Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < snapshot.size();
            }

            @Override
            public Document next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("no more documents");
                }
                return DocumentCopies.copy(snapshot.get(index++));
            }
        };
    }

    @Override
    public synchronized List<Document> find(Document filter) {
        return find(filter, CollationSupport.Config.simple());
    }

    @Override
    public synchronized List<Document> find(final Document filter, final CollationSupport.Config collation) {
        pruneExpiredDocuments();
        final Document effectiveFilter = filter == null ? new Document() : filter;
        final CollationSupport.Config effectiveCollation =
                collation == null ? CollationSupport.Config.simple() : collation;
        return copyMatchingDocuments(effectiveFilter, effectiveCollation);
    }

    @Override
    public synchronized List<Document> aggregate(final List<Document> pipeline) {
        Objects.requireNonNull(pipeline, "pipeline");
        pruneExpiredDocuments();

        final List<Document> copiedPipeline = new ArrayList<>(pipeline.size());
        for (final Document stage : pipeline) {
            if (stage == null) {
                throw new IllegalArgumentException("pipeline stages must not contain null");
            }
            copiedPipeline.add(DocumentCopies.copy(stage));
        }

        final List<Document> source = new ArrayList<>(documents.size());
        for (final Document document : documents) {
            source.add(DocumentCopies.copy(document));
        }
        return AggregationPipeline.execute(source, copiedPipeline);
    }

    @Override
    public synchronized UpdateManyResult update(
            final Document filter, final Document update, final boolean multi, final boolean upsert) {
        return update(filter, update, multi, upsert, List.of());
    }

    @Override
    public synchronized UpdateManyResult update(
            final Document filter,
            final Document update,
            final boolean multi,
            final boolean upsert,
            final List<Document> arrayFilters) {
        pruneExpiredDocuments();
        final Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);
        final Document effectiveUpdate = update == null ? null : DocumentCopies.copy(update);
        final List<Document> effectiveArrayFilters = copyArrayFilters(arrayFilters);
        final UpdateApplier.ParsedUpdate parsedUpdate = UpdateApplier.parse(effectiveUpdate, effectiveArrayFilters);

        final List<Document> matchedDocuments = new ArrayList<>();
        for (final Document document : documents) {
            if (QueryMatcher.matches(document, effectiveFilter)) {
                matchedDocuments.add(document);
                if (!multi) {
                    break;
                }
            }
        }

        if (matchedDocuments.isEmpty()) {
            if (!upsert) {
                return new UpdateManyResult(0, 0);
            }
            return applyUpsert(effectiveFilter, parsedUpdate);
        }

        for (final Document document : matchedDocuments) {
            UpdateApplier.validateApplicable(document, parsedUpdate);
        }

        final IdentityHashMap<Document, UpdatePreview> previewsByDocument = new IdentityHashMap<>(matchedDocuments.size());
        long modifiedCount = 0;
        for (final Document document : matchedDocuments) {
            final Document previewDocument = DocumentCopies.copy(document);
            final boolean modified = UpdateApplier.apply(previewDocument, parsedUpdate);
            previewsByDocument.put(document, new UpdatePreview(previewDocument, modified));
            if (modified) {
                modifiedCount++;
            }
        }

        if (modifiedCount > 0) {
            final List<Document> candidateDocuments = new ArrayList<>(documents.size());
            for (final Document document : documents) {
                final UpdatePreview preview = previewsByDocument.get(document);
                if (preview == null || !preview.modified()) {
                    candidateDocuments.add(document);
                } else {
                    candidateDocuments.add(preview.updatedDocument());
                }
            }
            validateUniqueConstraints(candidateDocuments, indexesByName.values());
        }

        for (final Document document : matchedDocuments) {
            final UpdatePreview preview = previewsByDocument.get(document);
            if (preview == null || !preview.modified()) {
                continue;
            }
            final Document before = changeLog == null ? null : DocumentCopies.copy(document);
            document.clear();
            document.putAll(preview.updatedDocument());
            recordUpdate(before, document, parsedUpdate.replacementStyle());
        }

        return new UpdateManyResult(matchedDocuments.size(), modifiedCount);
    }

    @Override
    public synchronized UpdateManyResult updatePipeline(
            final Document filter,
            final List<Document> pipeline,
            final boolean multi,
            final boolean upsert) {
        pruneExpiredDocuments();
        final Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);
        final List<Document> effectivePipeline = new ArrayList<>();
        for (final Document stage : Objects.requireNonNull(pipeline, "pipeline")) {
            effectivePipeline.add(DocumentCopies.copy(Objects.requireNonNull(stage, "pipeline stage")));
        }
        if (effectivePipeline.isEmpty()) {
            throw new IllegalArgumentException("update pipeline must not be empty");
        }

        final List<Document> matchedDocuments = new ArrayList<>();
        for (final Document document : documents) {
            if (QueryMatcher.matches(document, effectiveFilter)) {
                matchedDocuments.add(document);
                if (!multi) {
                    break;
                }
            }
        }

        if (matchedDocuments.isEmpty()) {
            if (!upsert) {
                return new UpdateManyResult(0, 0);
            }
            return applyPipelineUpsert(effectiveFilter, effectivePipeline);
        }

        final IdentityHashMap<Document, UpdatePreview> previewsByDocument =
                new IdentityHashMap<>(matchedDocuments.size());
        long modifiedCount = 0L;
        for (final Document document : matchedDocuments) {
            final Document updated = applyPipelineToDocument(document, effectivePipeline);
            final boolean modified = !Objects.deepEquals(document, updated);
            previewsByDocument.put(document, new UpdatePreview(updated, modified));
            if (modified) {
                modifiedCount++;
            }
        }

        if (modifiedCount > 0) {
            final List<Document> candidateDocuments = new ArrayList<>(documents.size());
            for (final Document document : documents) {
                final UpdatePreview preview = previewsByDocument.get(document);
                candidateDocuments.add(preview == null || !preview.modified()
                        ? document
                        : preview.updatedDocument());
            }
            validateUniqueConstraints(candidateDocuments, indexesByName.values());
        }

        for (final Document document : matchedDocuments) {
            final UpdatePreview preview = previewsByDocument.get(document);
            if (preview == null || !preview.modified()) {
                continue;
            }
            final Document before = changeLog == null ? null : DocumentCopies.copy(document);
            document.clear();
            document.putAll(preview.updatedDocument());
            recordUpdate(before, document, false);
        }
        return new UpdateManyResult(matchedDocuments.size(), modifiedCount);
    }

    private static Document applyPipelineToDocument(
            final Document source, final List<Document> pipeline) {
        final List<Document> results = AggregationPipeline.execute(List.of(DocumentCopies.copy(source)), pipeline);
        if (results.size() != 1) {
            throw new IllegalArgumentException("update pipeline must produce exactly one document per input document");
        }
        final Document updated = results.get(0);
        if (source.containsKey("_id")
                && (!updated.containsKey("_id")
                        || !Objects.deepEquals(source.get("_id"), updated.get("_id")))) {
            throw new IllegalArgumentException("update pipeline cannot change immutable field '_id'");
        }
        return DocumentCopies.copy(updated);
    }

    private UpdateManyResult applyPipelineUpsert(
            final Document filter, final List<Document> pipeline) {
        final Document upsertedDocument = applyPipelineToDocument(upsertSeed(filter), pipeline);
        if (!upsertedDocument.containsKey("_id")) {
            upsertedDocument.put("_id", new ObjectId());
        }

        final List<Document> candidateDocuments = new ArrayList<>(documents.size() + 1);
        candidateDocuments.addAll(documents);
        candidateDocuments.add(upsertedDocument);
        validateUniqueConstraints(candidateDocuments, indexesByName.values());
        documents.add(upsertedDocument);
        recordInsert(upsertedDocument);
        return new UpdateManyResult(0, 0, DocumentCopies.copyAny(upsertedDocument.get("_id")));
    }

    private static List<Document> copyArrayFilters(final List<Document> arrayFilters) {
        if (arrayFilters == null || arrayFilters.isEmpty()) {
            return List.of();
        }
        final List<Document> copied = new ArrayList<>(arrayFilters.size());
        for (final Document filter : arrayFilters) {
            copied.add(DocumentCopies.copy(Objects.requireNonNull(filter, "arrayFilters entries must not be null")));
        }
        return List.copyOf(copied);
    }

    @Override
    public synchronized DeleteManyResult deleteMany(Document filter) {
        pruneExpiredDocuments();
        Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);

        long deletedCount = 0;
        Iterator<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            Document document = iterator.next();
            if (QueryMatcher.matches(document, effectiveFilter)) {
                iterator.remove();
                deletedCount++;
                recordDelete(document);
            }
        }

        return new DeleteManyResult(deletedCount, deletedCount);
    }

    private List<Document> copyMatchingDocuments(final Document filter, final CollationSupport.Config collation) {
        List<Document> matches = new ArrayList<>();
        for (Document document : documents) {
            if (QueryMatcher.matches(document, filter, collation)) {
                matches.add(DocumentCopies.copy(document));
            }
        }
        return matches;
    }

    private void pruneExpiredDocuments() {
        final List<TtlRule> ttlRules = ttlRules();
        if (ttlRules.isEmpty() || documents.isEmpty()) {
            return;
        }

        final long nowMillis = clock.millis();
        final Iterator<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            final Document document = iterator.next();
            for (final TtlRule ttlRule : ttlRules) {
                if (ttlRule.isExpired(document, nowMillis)) {
                    iterator.remove();
                    recordDelete(document);
                    break;
                }
            }
        }
    }

    private void recordInsert(final Document document) {
        if (changeLog != null) {
            changeLog.recordInsert(changeNamespace, document);
        }
    }

    private void recordUpdate(final Document before, final Document after, final boolean replacement) {
        if (changeLog != null) {
            changeLog.recordUpdate(changeNamespace, before, after, replacement);
        }
    }

    private void recordDelete(final Document document) {
        if (changeLog != null) {
            changeLog.recordDelete(changeNamespace, document);
        }
    }

    private List<TtlRule> ttlRules() {
        final List<TtlRule> rules = new ArrayList<>();
        for (final IndexMetadata index : indexesByName.values()) {
            if (index.expireAfterSeconds() == null
                    || index.key().size() != 1
                    || index.partialFilterExpression() != null) {
                continue;
            }
            final String fieldPath = index.key().keySet().iterator().next();
            if (fieldPath == null || fieldPath.isBlank()) {
                continue;
            }
            rules.add(new TtlRule(fieldPath, index.expireAfterSeconds() * 1000L));
        }
        return rules;
    }

    private static Long dateLikeMillis(final Object value) {
        if (value instanceof Date dateValue) {
            return dateValue.getTime();
        }
        if (value instanceof Instant instantValue) {
            return instantValue.toEpochMilli();
        }
        return null;
    }

    private UpdateManyResult applyUpsert(final Document filter, final UpdateApplier.ParsedUpdate parsedUpdate) {
        final Document seed = upsertSeed(filter);
        final Document upsertedDocument = DocumentCopies.copy(seed);

        UpdateApplier.validateApplicableForUpsertInsert(upsertedDocument, parsedUpdate);
        UpdateApplier.applyForUpsertInsert(upsertedDocument, parsedUpdate);

        if (!upsertedDocument.containsKey("_id")) {
            upsertedDocument.put("_id", new ObjectId());
        }

        final List<Document> candidateDocuments = new ArrayList<>(documents.size() + 1);
        candidateDocuments.addAll(documents);
        candidateDocuments.add(upsertedDocument);
        validateUniqueConstraints(candidateDocuments, indexesByName.values());

        documents.add(upsertedDocument);
        recordInsert(upsertedDocument);
        return new UpdateManyResult(0, 0, DocumentCopies.copyAny(upsertedDocument.get("_id")));
    }

    private static Document upsertSeed(final Document filter) {
        final Document seed = new Document();
        if (filter == null || filter.isEmpty()) {
            return seed;
        }

        appendUpsertSeed(seed, filter);
        return seed;
    }

    private static void appendUpsertSeed(final Document seed, final Map<?, ?> filter) {
        if (filter == null || filter.isEmpty()) {
            return;
        }

        for (final Map.Entry<?, ?> entry : filter.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                continue;
            }
            if (key == null || key.isEmpty() || key.startsWith("$")) {
                if ("$and".equals(key)) {
                    appendUpsertSeedFromAndClauses(seed, entry.getValue());
                }
                continue;
            }
            if (isOperatorDocument(entry.getValue())) {
                if (seedFromOperatorFilter(seed, key, entry.getValue())) {
                    continue;
                }
                continue;
            }
            setPathValue(seed, key, entry.getValue());
        }
    }

    private static void appendUpsertSeedFromAndClauses(final Document seed, final Object clausesValue) {
        if (!(clausesValue instanceof List<?> clauses)) {
            return;
        }

        for (final Object clause : clauses) {
            if (clause instanceof Map<?, ?> clauseMap) {
                appendUpsertSeed(seed, clauseMap);
            }
        }
    }

    private static boolean isOperatorDocument(final Object value) {
        if (!(value instanceof Map<?, ?> mapValue) || mapValue.isEmpty()) {
            return false;
        }
        for (final Object key : mapValue.keySet()) {
            if (!(key instanceof String fieldName) || !fieldName.startsWith("$")) {
                return false;
            }
        }
        return true;
    }

    private static boolean seedFromOperatorFilter(final Document seed, final String key, final Object value) {
        if (!"_id".equals(key) || !(value instanceof Map<?, ?> operatorMap)) {
            return false;
        }
        final Object typeOperand = operatorMap.get("$type");
        if (isNullType(typeOperand)) {
            setPathValue(seed, key, null);
            return true;
        }

        if (operatorMap.containsKey("$eq") && operatorMap.get("$eq") == null) {
            setPathValue(seed, key, null);
            return true;
        }
        return false;
    }

    private static boolean isNullType(final Object value) {
        if (value instanceof String textValue) {
            return "null".equalsIgnoreCase(textValue);
        }
        if (value instanceof Number numberValue) {
            return numberValue.intValue() == 10;
        }
        if (value instanceof List<?> listValue) {
            for (final Object item : listValue) {
                if (isNullType(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void setPathValue(final Document target, final String path, final Object value) {
        final String[] segments = path.split("\\.");
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) target;

        for (int i = 0; i < segments.length - 1; i++) {
            final String segment = segments[i];
            if (segment.isEmpty()) {
                return;
            }

            final Object existing = current.get(segment);
            if (existing == null) {
                final Document created = new Document();
                current.put(segment, created);
                current = created;
                continue;
            }
            if (!(existing instanceof Map<?, ?> mapExisting)) {
                return;
            }
            current = castStringMap(mapExisting);
        }

        final String leaf = segments[segments.length - 1];
        if (leaf.isEmpty()) {
            return;
        }
        if (current.containsKey(leaf) && !Objects.deepEquals(current.get(leaf), value)) {
            return;
        }
        current.put(leaf, DocumentCopies.copyAny(value));
    }

    private static String requireNonBlankIndexName(final String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("index name must not be blank");
        }
        return name;
    }

    private static Document requireNonEmptyIndexKey(final Document key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("index key must not be empty");
        }
        return DocumentCopies.copy(key);
    }

    private static boolean idIndexRecreation(final String name, final Document key, final IndexDefinition index) {
        return "_id_".equals(name)
                && sameIndexKey(key, new Document("_id", 1))
                && !index.sparse()
                && index.partialFilterExpression() == null
                && index.collation() == null
                && index.expireAfterSeconds() == null;
    }

    private static boolean sameIndexSpec(
            final IndexMetadata existing, final Document key, final IndexDefinition requested) {
        return sameIndexIdentity(existing, key, requested)
                && Objects.equals(existing.expireAfterSeconds(), requested.expireAfterSeconds());
    }

    private static boolean sameIndexIdentity(
            final IndexMetadata existing, final Document key, final IndexDefinition requested) {
        return sameIndexKey(existing.key(), key)
                && existing.unique() == requested.unique()
                && existing.sparse() == requested.sparse()
                && sameOptionalDocument(existing.partialFilterExpression(), requested.partialFilterExpression())
                && sameOptionalDocument(existing.collation(), requested.collation());
    }

    private static boolean sameIndexKey(final Document left, final Document right) {
        if (left.size() != right.size()) {
            return false;
        }
        final Iterator<Map.Entry<String, Object>> leftEntries = left.entrySet().iterator();
        final Iterator<Map.Entry<String, Object>> rightEntries = right.entrySet().iterator();
        while (leftEntries.hasNext()) {
            final Map.Entry<String, Object> leftEntry = leftEntries.next();
            final Map.Entry<String, Object> rightEntry = rightEntries.next();
            if (!leftEntry.getKey().equals(rightEntry.getKey())) {
                return false;
            }
            if (!sameKeyDirection(leftEntry.getValue(), rightEntry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameKeyDirection(final Object left, final Object right) {
        if (left instanceof Number && right instanceof Number) {
            return MongoValueComparator.equals(left, right);
        }
        return Objects.deepEquals(left, right);
    }

    private static boolean sameOptionalDocument(final Document left, final Document right) {
        if (left == null || right == null) {
            return left == right;
        }
        return left.toJson().equals(right.toJson());
    }

    private static List<String> indexFieldPaths(final Document key) {
        final List<String> fieldPaths = new ArrayList<>(key.size());
        for (final String fieldPath : key.keySet()) {
            if (fieldPath == null || fieldPath.isBlank()) {
                continue;
            }
            fieldPaths.add(fieldPath);
        }
        return List.copyOf(fieldPaths);
    }

    private static void validateUniqueConstraints(
            final List<Document> candidateDocuments, final Iterable<IndexMetadata> indexes) {
        for (IndexMetadata index : indexes) {
            if (!index.unique() || index.uniqueFieldPaths().isEmpty()) {
                continue;
            }

            final CollationSupport.Config indexCollation = index.collation() == null
                    ? CollationSupport.Config.simple()
                    : CollationSupport.Config.fromDocument(index.collation());
            final List<UniqueValueKey> seenValues = new ArrayList<>();
            for (Document document : candidateDocuments) {
                if (index.partialFilterExpression() != null
                        && !QueryMatcher.matches(document, index.partialFilterExpression(), indexCollation)) {
                    continue;
                }
                if (index.sparse() && isSparseExcluded(document, index.uniqueFieldPaths())) {
                    continue;
                }

                final Object[] values = resolvePathValues(document, index.uniqueFieldPaths());
                final UniqueValueKey candidateKey = new UniqueValueKey(values, indexCollation);
                if (seenValues.contains(candidateKey)) {
                    throw new DuplicateKeyException(duplicateKeyMessage(index, values));
                }
                seenValues.add(candidateKey);
            }
        }
    }

    private static String duplicateKeyMessage(final IndexMetadata index, final Object[] duplicateValues) {
        final StringBuilder duplicateKeyBuilder = new StringBuilder();
        for (int i = 0; i < index.uniqueFieldPaths().size(); i++) {
            if (i > 0) {
                duplicateKeyBuilder.append(", ");
            }
            duplicateKeyBuilder
                    .append(index.uniqueFieldPaths().get(i))
                    .append(": ")
                    .append(String.valueOf(duplicateValues[i]));
        }
        return "E11000 duplicate key error index: "
                + index.name()
                + " dup key: { "
                + duplicateKeyBuilder
                + " }";
    }

    private static boolean isSparseExcluded(final Document document, final List<String> fieldPaths) {
        for (final String fieldPath : fieldPaths) {
            if (pathExists(document, fieldPath)) {
                return false;
            }
        }
        return true;
    }

    private static boolean pathExists(final Document document, final String path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        if (!path.contains(".")) {
            return document.containsKey(path);
        }

        final String[] segments = path.split("\\.");
        Object current = document;
        for (int i = 0; i < segments.length; i++) {
            if (!(current instanceof Map<?, ?> mapValue)) {
                return false;
            }
            final String segment = segments[i];
            if (!mapValue.containsKey(segment)) {
                return false;
            }
            current = mapValue.get(segment);
            if (i < segments.length - 1 && current == null) {
                return false;
            }
        }
        return true;
    }

    private static Object[] resolvePathValues(final Document document, final List<String> paths) {
        final Object[] values = new Object[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            values[i] = resolvePathValue(document, paths.get(i));
        }
        return values;
    }

    private static Object resolvePathValue(final Document document, final String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        if (!path.contains(".")) {
            return document.get(path);
        }

        String[] segments = path.split("\\.");
        Object current = document;
        for (String segment : segments) {
            if (!(current instanceof Map<?, ?> mapValue)) {
                return null;
            }
            current = mapValue.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castStringMap(final Map<?, ?> source) {
        return (Map<String, Object>) source;
    }

    private static CollectionState emptyState() {
        return new CollectionState(List.of(), List.of());
    }

    private static List<Document> copyDocuments(final List<Document> source) {
        final List<Document> copied = new ArrayList<>(source.size());
        for (final Document document : source) {
            copied.add(DocumentCopies.copy(Objects.requireNonNull(document, "document")));
        }
        return List.copyOf(copied);
    }

    private static List<Document> mergeTransactionDocuments(
            final List<Document> baselineDocuments,
            final List<Document> transactionDocuments,
            final List<Document> currentDocuments) {
        final Map<String, Document> baselineById = documentsById(baselineDocuments);
        final Map<String, Document> transactionById = documentsById(transactionDocuments);
        final Map<String, Document> currentById = documentsById(currentDocuments);
        if (baselineById == null || transactionById == null || currentById == null) {
            return mergeTransactionDocumentsByCanonical(baselineDocuments, transactionDocuments, currentDocuments);
        }

        // A document the transaction inserted, updated, or deleted conflicts when that same
        // _id also differs from the transaction's baseline in the live collection.
        final LinkedHashMap<String, Object> changedIds = new LinkedHashMap<>();
        for (final Map.Entry<String, Document> entry : baselineById.entrySet()) {
            if (!sameCanonical(entry.getValue(), transactionById.get(entry.getKey()))) {
                changedIds.put(entry.getKey(), entry.getValue().get("_id"));
            }
        }
        for (final Map.Entry<String, Document> entry : transactionById.entrySet()) {
            if (!baselineById.containsKey(entry.getKey())) {
                changedIds.put(entry.getKey(), entry.getValue().get("_id"));
            }
        }
        for (final Map.Entry<String, Object> changed : changedIds.entrySet()) {
            if (!sameCanonical(baselineById.get(changed.getKey()), currentById.get(changed.getKey()))) {
                throw new WriteConflictException("commit transaction write conflict on _id=" + changed.getValue());
            }
        }

        final List<Document> merged = new ArrayList<>(copyDocuments(currentDocuments));
        for (final Map.Entry<String, Object> changed : changedIds.entrySet()) {
            removeFirstDocumentById(merged, changed.getValue());
            final Document transactionDocument = transactionById.get(changed.getKey());
            if (transactionDocument != null) {
                merged.add(DocumentCopies.copy(transactionDocument));
            }
        }
        return merged;
    }

    private static List<Document> mergeTransactionDocumentsByCanonical(
            final List<Document> baselineDocuments,
            final List<Document> transactionDocuments,
            final List<Document> currentDocuments) {
        final List<Document> mergedDocuments = new ArrayList<>(copyDocuments(currentDocuments));
        final List<Document> removedDocuments = diffDocuments(baselineDocuments, transactionDocuments);
        for (final Document removedDocument : removedDocuments) {
            final Object removedId = removedDocument.get("_id");
            if (removedId != null && removeFirstDocumentById(mergedDocuments, removedId)) {
                continue;
            }
            removeFirstDocumentByCanonical(mergedDocuments, removedDocument);
        }

        final List<Document> addedDocuments = diffDocuments(transactionDocuments, baselineDocuments);
        for (final Document addedDocument : addedDocuments) {
            final Object addedId = addedDocument.get("_id");
            if (addedId != null) {
                final boolean presentInBaseline = containsDocumentWithId(baselineDocuments, addedId);
                final boolean presentInCurrent = containsDocumentWithId(mergedDocuments, addedId);
                if (presentInCurrent && !presentInBaseline) {
                    throw new WriteConflictException("commit transaction write conflict on _id=" + addedId);
                }
                removeFirstDocumentById(mergedDocuments, addedId);
            }
            mergedDocuments.add(DocumentCopies.copy(addedDocument));
        }
        return mergedDocuments;
    }

    private static Map<String, Document> documentsById(final List<Document> documents) {
        final LinkedHashMap<String, Document> byId = new LinkedHashMap<>();
        for (final Document document : documents) {
            if (document == null || document.get("_id") == null) {
                return null;
            }
            final String key = new Document("id", document.get("_id")).toJson();
            if (byId.containsKey(key)) {
                return null;
            }
            byId.put(key, document);
        }
        return byId;
    }

    private static boolean sameCanonical(final Document left, final Document right) {
        if (left == null || right == null) {
            return left == right;
        }
        return canonicalDocumentKey(left).equals(canonicalDocumentKey(right));
    }

    private static List<Document> diffDocuments(final List<Document> source, final List<Document> subtract) {
        final Map<String, Integer> subtractCounts = new HashMap<>();
        for (final Document document : subtract) {
            subtractCounts.merge(canonicalDocumentKey(document), 1, Integer::sum);
        }

        final List<Document> difference = new ArrayList<>();
        for (final Document document : source) {
            final String key = canonicalDocumentKey(document);
            final Integer remaining = subtractCounts.get(key);
            if (remaining == null || remaining == 0) {
                difference.add(DocumentCopies.copy(document));
                continue;
            }
            if (remaining == 1) {
                subtractCounts.remove(key);
            } else {
                subtractCounts.put(key, remaining - 1);
            }
        }
        return difference;
    }

    private static boolean removeFirstDocumentById(final List<Document> documents, final Object targetId) {
        final Iterator<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            final Document candidate = iterator.next();
            if (Objects.deepEquals(candidate.get("_id"), targetId)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    private static boolean containsDocumentWithId(final List<Document> documents, final Object targetId) {
        for (final Document candidate : documents) {
            if (Objects.deepEquals(candidate.get("_id"), targetId)) {
                return true;
            }
        }
        return false;
    }

    private static boolean removeFirstDocumentByCanonical(final List<Document> documents, final Document targetDocument) {
        final String targetKey = canonicalDocumentKey(targetDocument);
        final Iterator<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            final Document candidate = iterator.next();
            if (targetKey.equals(canonicalDocumentKey(candidate))) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    private static String canonicalDocumentKey(final Document document) {
        return document.toJson();
    }

    private static List<CollectionStore.IndexDefinition> mergeIndexes(
            final List<CollectionStore.IndexDefinition> baselineIndexes,
            final List<CollectionStore.IndexDefinition> transactionIndexes,
            final List<CollectionStore.IndexDefinition> currentIndexes) {
        final Map<String, CollectionStore.IndexDefinition> baselineByName = indexByName(baselineIndexes);
        final Map<String, CollectionStore.IndexDefinition> transactionByName = indexByName(transactionIndexes);
        final LinkedHashMap<String, CollectionStore.IndexDefinition> mergedByName = new LinkedHashMap<>(indexByName(currentIndexes));

        for (final Map.Entry<String, CollectionStore.IndexDefinition> baselineEntry : baselineByName.entrySet()) {
            final String name = baselineEntry.getKey();
            if (transactionByName.containsKey(name)) {
                continue;
            }
            mergedByName.remove(name);
        }

        for (final CollectionStore.IndexDefinition transactionIndex : transactionIndexes) {
            mergedByName.put(transactionIndex.name(), transactionIndex);
        }

        return List.copyOf(mergedByName.values());
    }

    private static Map<String, CollectionStore.IndexDefinition> indexByName(
            final List<CollectionStore.IndexDefinition> indexes) {
        final LinkedHashMap<String, CollectionStore.IndexDefinition> byName = new LinkedHashMap<>();
        for (final CollectionStore.IndexDefinition index : indexes) {
            byName.put(requireNonBlankIndexName(index.name()), index);
        }
        return byName;
    }

    private static Map<String, IndexMetadata> toIndexMetadataMap(final List<CollectionStore.IndexDefinition> definitions) {
        final LinkedHashMap<String, IndexMetadata> byName = new LinkedHashMap<>();
        for (final CollectionStore.IndexDefinition definition : definitions) {
            final String name = requireNonBlankIndexName(definition.name());
            if (byName.containsKey(name)) {
                throw new IllegalArgumentException("duplicate index name: " + name);
            }

            final Document key = requireNonEmptyIndexKey(definition.key());
            final List<String> uniqueFieldPaths = definition.unique() ? indexFieldPaths(key) : List.of();
            final Document partialFilterExpression = definition.partialFilterExpression() == null
                    ? null
                    : DocumentCopies.copy(definition.partialFilterExpression());
            final Document collation =
                    definition.collation() == null ? null : DocumentCopies.copy(definition.collation());
            byName.put(
                    name,
                    new IndexMetadata(
                            name,
                            key,
                            definition.unique(),
                            definition.sparse(),
                            partialFilterExpression,
                            collation,
                            definition.expireAfterSeconds(),
                            uniqueFieldPaths));
        }
        return byName;
    }

    private record UpdatePreview(Document updatedDocument, boolean modified) {}

    private record TtlRule(String fieldPath, long expireAfterMillis) {
        private boolean isExpired(final Document document, final long nowMillis) {
            final Long fieldMillis = dateLikeMillis(resolvePathValue(document, fieldPath));
            return fieldMillis != null && fieldMillis + expireAfterMillis <= nowMillis;
        }
    }

    record CollectionState(List<Document> documents, List<CollectionStore.IndexDefinition> indexes) {
        CollectionState {
            documents = documents == null ? List.of() : copyDocuments(documents);
            indexes = indexes == null ? List.of() : List.copyOf(indexes);
        }
    }

    private record IndexMetadata(
            String name,
            Document key,
            boolean unique,
            boolean sparse,
            Document partialFilterExpression,
            Document collation,
            Long expireAfterSeconds,
            List<String> uniqueFieldPaths) {}

    private static final class UniqueValueKey {
        private final Object[] values;
        private final CollationSupport.Config collation;

        private UniqueValueKey(final Object[] values, final CollationSupport.Config collation) {
            this.values = values == null ? new Object[0] : Arrays.copyOf(values, values.length);
            this.collation = Objects.requireNonNull(collation, "collation");
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof UniqueValueKey that)) {
                return false;
            }
            if (values.length != that.values.length) {
                return false;
            }
            for (int i = 0; i < values.length; i++) {
                if (!uniqueValuesEqual(values[i], that.values[i], collation)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private static boolean uniqueValuesEqual(
            final Object left, final Object right, final CollationSupport.Config collation) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Number && right instanceof Number) {
            return MongoValueComparator.equals(left, right);
        }
        if (left instanceof String && right instanceof String) {
            return collation.valuesEqual(left, right);
        }
        if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
            if (leftMap.size() != rightMap.size()) {
                return false;
            }
            for (final Map.Entry<?, ?> entry : leftMap.entrySet()) {
                if (!rightMap.containsKey(entry.getKey())
                        || !uniqueValuesEqual(entry.getValue(), rightMap.get(entry.getKey()), collation)) {
                    return false;
                }
            }
            return true;
        }
        if (left instanceof List<?> leftList && right instanceof List<?> rightList) {
            if (leftList.size() != rightList.size()) {
                return false;
            }
            for (int index = 0; index < leftList.size(); index++) {
                if (!uniqueValuesEqual(leftList.get(index), rightList.get(index), collation)) {
                    return false;
                }
            }
            return true;
        }
        if (left.getClass().isArray() && right.getClass().isArray()) {
            final int leftLength = java.lang.reflect.Array.getLength(left);
            final int rightLength = java.lang.reflect.Array.getLength(right);
            if (leftLength != rightLength) {
                return false;
            }
            for (int index = 0; index < leftLength; index++) {
                if (!uniqueValuesEqual(
                        java.lang.reflect.Array.get(left, index),
                        java.lang.reflect.Array.get(right, index),
                        collation)) {
                    return false;
                }
            }
            return true;
        }
        return collation.valuesEqual(left, right);
    }

    public static final class IndexConflictException extends RuntimeException {
        private final int code;
        private final String codeName;

        public IndexConflictException(final int code, final String codeName, final String message) {
            super(message);
            this.code = code;
            this.codeName = codeName;
        }

        public int code() {
            return code;
        }

        public String codeName() {
            return codeName;
        }
    }
}
