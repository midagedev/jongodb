package org.jongodb.engine;

import java.util.List;
import org.bson.Document;

/**
 * Storage contract for a single namespace.
 */
public interface CollectionStore {
    void insertMany(List<Document> documents);

    CreateIndexesResult createIndexes(List<IndexDefinition> indexes);

    List<Document> findAll();

    List<Document> find(Document filter);

    UpdateManyResult update(Document filter, Document update, boolean multi, boolean upsert);

    default UpdateManyResult updateMany(final Document filter, final Document update) {
        return update(filter, update, true, false);
    }

    DeleteManyResult deleteMany(Document filter);

    record IndexDefinition(String name, Document key, boolean unique) {}

    record CreateIndexesResult(int numIndexesBefore, int numIndexesAfter) {}
}
