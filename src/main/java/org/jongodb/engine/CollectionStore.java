package org.jongodb.engine;

import java.util.List;
import org.bson.Document;

/**
 * Storage contract for a single namespace.
 */
public interface CollectionStore {
    void insertMany(List<Document> documents);

    List<Document> findAll();

    List<Document> find(Document filter);
}
