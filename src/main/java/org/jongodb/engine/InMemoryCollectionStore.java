package org.jongodb.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * Basic synchronized in-memory collection store.
 */
public final class InMemoryCollectionStore implements CollectionStore {
    private final List<Document> documents = new ArrayList<>();
    private final Map<String, IndexMetadata> indexesByName = new LinkedHashMap<>();

    InMemoryCollectionStore() {
        final Document idIndexKey = new Document("_id", 1);
        indexesByName.put(
                "_id_",
                new IndexMetadata("_id_", idIndexKey, true, false, null, null, null, List.of("_id")));
    }

    private InMemoryCollectionStore(final InMemoryCollectionStore source) {
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

    synchronized InMemoryCollectionStore snapshot() {
        return new InMemoryCollectionStore(this);
    }

    synchronized CollectionState snapshotState() {
        return new CollectionState(copyDocuments(documents), listIndexes());
    }

    synchronized void replaceState(final CollectionState state) {
        Objects.requireNonNull(state, "state");

        final List<Document> copiedDocuments = copyDocuments(state.documents());
        final Map<String, IndexMetadata> copiedIndexes = toIndexMetadataMap(state.indexes());
        validateUniqueConstraints(copiedDocuments, copiedIndexes.values());

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

        final List<Document> mergedDocuments = new ArrayList<>(copyDocuments(current.documents()));
        final List<Document> removedDocuments = diffDocuments(baseline.documents(), transaction.documents());
        for (final Document removedDocument : removedDocuments) {
            final Object removedId = removedDocument.get("_id");
            if (removedId != null && removeFirstDocumentById(mergedDocuments, removedId)) {
                continue;
            }
            removeFirstDocumentByCanonical(mergedDocuments, removedDocument);
        }

        final List<Document> addedDocuments = diffDocuments(transaction.documents(), baseline.documents());
        for (final Document addedDocument : addedDocuments) {
            final Object addedId = addedDocument.get("_id");
            if (addedId != null) {
                final boolean presentInBaseline = containsDocumentWithId(baseline.documents(), addedId);
                final boolean presentInCurrent = containsDocumentWithId(mergedDocuments, addedId);
                if (presentInCurrent && !presentInBaseline) {
                    throw new WriteConflictException("commit transaction write conflict on _id=" + addedId);
                }
                removeFirstDocumentById(mergedDocuments, addedId);
            }
            mergedDocuments.add(DocumentCopies.copy(addedDocument));
        }

        final List<CollectionStore.IndexDefinition> mergedIndexes = mergeIndexes(
                baseline.indexes(),
                transaction.indexes(),
                current.indexes());

        return new CollectionState(mergedDocuments, mergedIndexes);
    }

    @Override
    public synchronized void insertMany(List<Document> documents) {
        Objects.requireNonNull(documents, "documents");

        List<Document> copiedDocuments = new ArrayList<>(documents.size());
        for (Document document : documents) {
            if (document == null) {
                throw new IllegalArgumentException("documents must not contain null");
            }
            copiedDocuments.add(DocumentCopies.copy(document));
        }

        List<Document> candidateDocuments = new ArrayList<>(this.documents.size() + copiedDocuments.size());
        candidateDocuments.addAll(this.documents);
        candidateDocuments.addAll(copiedDocuments);
        validateUniqueConstraints(candidateDocuments, indexesByName.values());

        this.documents.addAll(copiedDocuments);
    }

    @Override
    public synchronized CreateIndexesResult createIndexes(List<IndexDefinition> indexes) {
        Objects.requireNonNull(indexes, "indexes");

        final int numIndexesBefore = indexesByName.size();
        final Map<String, IndexMetadata> candidateIndexes = new LinkedHashMap<>(indexesByName);
        for (IndexDefinition index : indexes) {
            if (index == null) {
                throw new IllegalArgumentException("indexes must not contain null");
            }

            final String name = requireNonBlankIndexName(index.name());
            if (candidateIndexes.containsKey(name)) {
                continue;
            }

            final Document key = requireNonEmptyIndexKey(index.key());
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
    public synchronized List<Document> findAll() {
        return copyMatchingDocuments(new Document());
    }

    @Override
    public synchronized List<Document> find(Document filter) {
        Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);
        return copyMatchingDocuments(effectiveFilter);
    }

    @Override
    public synchronized List<Document> aggregate(final List<Document> pipeline) {
        Objects.requireNonNull(pipeline, "pipeline");

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
        final Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);
        final Document effectiveUpdate = update == null ? null : DocumentCopies.copy(update);
        final UpdateApplier.ParsedUpdate parsedUpdate = UpdateApplier.parse(effectiveUpdate);

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
            document.clear();
            document.putAll(preview.updatedDocument());
        }

        return new UpdateManyResult(matchedDocuments.size(), modifiedCount);
    }

    @Override
    public synchronized DeleteManyResult deleteMany(Document filter) {
        Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);

        long deletedCount = 0;
        Iterator<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            Document document = iterator.next();
            if (QueryMatcher.matches(document, effectiveFilter)) {
                iterator.remove();
                deletedCount++;
            }
        }

        return new DeleteManyResult(deletedCount, deletedCount);
    }

    private List<Document> copyMatchingDocuments(Document filter) {
        List<Document> matches = new ArrayList<>();
        for (Document document : documents) {
            if (QueryMatcher.matches(document, filter)) {
                matches.add(DocumentCopies.copy(document));
            }
        }
        return matches;
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
        return new UpdateManyResult(0, 0, DocumentCopies.copyAny(upsertedDocument.get("_id")));
    }

    private static Document upsertSeed(final Document filter) {
        final Document seed = new Document();
        if (filter == null || filter.isEmpty()) {
            return seed;
        }

        for (final Map.Entry<String, Object> entry : filter.entrySet()) {
            final String key = entry.getKey();
            if (key == null || key.isEmpty() || key.startsWith("$")) {
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
        return seed;
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

            Map<UniqueValueKey, Boolean> seenValues = new HashMap<>();
            for (Document document : candidateDocuments) {
                if (index.partialFilterExpression() != null
                        && !QueryMatcher.matches(document, index.partialFilterExpression())) {
                    continue;
                }
                if (index.sparse() && isSparseExcluded(document, index.uniqueFieldPaths())) {
                    continue;
                }

                final Object[] values = resolvePathValues(document, index.uniqueFieldPaths());
                if (seenValues.putIfAbsent(new UniqueValueKey(values), Boolean.TRUE) != null) {
                    throw new DuplicateKeyException(duplicateKeyMessage(index, values));
                }
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
        private final Object value;
        private final int hashCode;

        private UniqueValueKey(final Object value) {
            this.value = value;
            this.hashCode = Arrays.deepHashCode(new Object[] {value});
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof UniqueValueKey that)) {
                return false;
            }
            return Objects.deepEquals(value, that.value);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
