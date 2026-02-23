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

    InMemoryCollectionStore() {}

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
                            index.uniqueFieldPath()));
        }
    }

    synchronized InMemoryCollectionStore snapshot() {
        return new InMemoryCollectionStore(this);
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
            final String uniqueFieldPath = index.unique() ? singleFieldPath(key) : null;
            candidateIndexes.put(name, new IndexMetadata(name, key, index.unique(), uniqueFieldPath));
        }

        validateUniqueConstraints(documents, candidateIndexes.values());

        indexesByName.clear();
        indexesByName.putAll(candidateIndexes);
        return new CreateIndexesResult(numIndexesBefore, indexesByName.size());
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

        UpdateApplier.validateApplicable(upsertedDocument, parsedUpdate);
        UpdateApplier.apply(upsertedDocument, parsedUpdate);

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

    private static String singleFieldPath(final Document key) {
        if (key.size() != 1) {
            return null;
        }
        final String fieldPath = key.keySet().iterator().next();
        if (fieldPath == null || fieldPath.isBlank()) {
            return null;
        }
        return fieldPath;
    }

    private static void validateUniqueConstraints(
            final List<Document> candidateDocuments, final Iterable<IndexMetadata> indexes) {
        for (IndexMetadata index : indexes) {
            if (!index.unique() || index.uniqueFieldPath() == null) {
                continue;
            }

            Map<UniqueValueKey, Boolean> seenValues = new HashMap<>();
            for (Document document : candidateDocuments) {
                Object value = resolvePathValue(document, index.uniqueFieldPath());
                if (seenValues.putIfAbsent(new UniqueValueKey(value), Boolean.TRUE) != null) {
                    throw new DuplicateKeyException(duplicateKeyMessage(index, value));
                }
            }
        }
    }

    private static String duplicateKeyMessage(final IndexMetadata index, final Object duplicateValue) {
        return "E11000 duplicate key error index: "
                + index.name()
                + " dup key: { "
                + index.uniqueFieldPath()
                + ": "
                + String.valueOf(duplicateValue)
                + " }";
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

    private record UpdatePreview(Document updatedDocument, boolean modified) {}

    private record IndexMetadata(String name, Document key, boolean unique, String uniqueFieldPath) {}

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
