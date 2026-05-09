package org.jongodb.engine;

import java.util.List;

/**
 * Top-level engine store contract for namespace-scoped collections.
 */
public interface EngineStore {
    CollectionStore collection(Namespace namespace);

    default boolean collectionExists(final Namespace namespace) {
        return false;
    }

    default CollectionStore collection(String database, String collection) {
        return collection(Namespace.of(database, collection));
    }

    default boolean collectionExists(final String database, final String collection) {
        return collectionExists(Namespace.of(database, collection));
    }

    default List<String> listCollectionNames(final String database) {
        return List.of();
    }

    default boolean dropCollection(final String database, final String collection) {
        return false;
    }

    default int dropDatabase(final String database) {
        return 0;
    }
}
