package org.jongodb.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

/**
 * Basic synchronized in-memory collection store.
 */
public final class InMemoryCollectionStore implements CollectionStore {
    private final List<Document> documents = new ArrayList<>();

    @Override
    public synchronized void insertMany(List<Document> documents) {
        Objects.requireNonNull(documents, "documents");

        for (Document document : documents) {
            if (document == null) {
                throw new IllegalArgumentException("documents must not contain null");
            }
            this.documents.add(DocumentCopies.copy(document));
        }
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

    private List<Document> copyMatchingDocuments(Document filter) {
        List<Document> matches = new ArrayList<>();
        for (Document document : documents) {
            if (matches(document, filter)) {
                matches.add(DocumentCopies.copy(document));
            }
        }
        return matches;
    }

    private static boolean matches(Document document, Document filter) {
        for (Map.Entry<String, Object> criteria : filter.entrySet()) {
            Object currentValue = document.get(criteria.getKey());
            if (!valueEquals(currentValue, criteria.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static boolean valueEquals(Object left, Object right) {
        if (left instanceof byte[] && right instanceof byte[]) {
            return Arrays.equals((byte[]) left, (byte[]) right);
        }
        return Objects.equals(left, right);
    }
}
