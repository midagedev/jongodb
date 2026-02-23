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

    UpdateManyResult updateMany(Document filter, Document update);

    DeleteManyResult deleteMany(Document filter);

    record IndexDefinition(String name, Document key, boolean unique) {}

    record CreateIndexesResult(int numIndexesBefore, int numIndexesAfter) {}
}
