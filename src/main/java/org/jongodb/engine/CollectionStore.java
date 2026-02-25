package org.jongodb.engine;

import java.util.List;
import org.bson.Document;

/**
 * Storage contract for a single namespace.
 */
public interface CollectionStore {
    void insertMany(List<Document> documents);

    CreateIndexesResult createIndexes(List<IndexDefinition> indexes);

    List<IndexDefinition> listIndexes();

    List<Document> findAll();

    List<Document> find(Document filter);

    default List<Document> find(final Document filter, final CollationSupport.Config collation) {
        return find(filter);
    }

    default List<Document> aggregate(final List<Document> pipeline) {
        throw new UnsupportedOperationException("aggregate is not supported");
    }

    UpdateManyResult update(Document filter, Document update, boolean multi, boolean upsert);

    default UpdateManyResult update(
            final Document filter,
            final Document update,
            final boolean multi,
            final boolean upsert,
            final List<Document> arrayFilters) {
        if (arrayFilters != null && !arrayFilters.isEmpty()) {
            throw new IllegalArgumentException("arrayFilters is not supported yet");
        }
        return update(filter, update, multi, upsert);
    }

    default UpdateManyResult updateMany(final Document filter, final Document update) {
        return update(filter, update, true, false);
    }

    DeleteManyResult deleteMany(Document filter);

    record IndexDefinition(
            String name,
            Document key,
            boolean unique,
            boolean sparse,
            Document partialFilterExpression,
            Document collation,
            Long expireAfterSeconds) {
        public IndexDefinition {
            key = key == null ? null : DocumentCopies.copy(key);
            partialFilterExpression =
                    partialFilterExpression == null ? null : DocumentCopies.copy(partialFilterExpression);
            collation = collation == null ? null : DocumentCopies.copy(collation);
        }

        public IndexDefinition(final String name, final Document key, final boolean unique) {
            this(name, key, unique, false, null, null, null);
        }

        public IndexDefinition(
                final String name,
                final Document key,
                final boolean unique,
                final boolean sparse,
                final Document partialFilterExpression,
                final Long expireAfterSeconds) {
            this(name, key, unique, sparse, partialFilterExpression, null, expireAfterSeconds);
        }
    }

    record CreateIndexesResult(int numIndexesBefore, int numIndexesAfter) {}
}
