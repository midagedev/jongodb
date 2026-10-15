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

    /**
     * Read-path lookup that must not materialize a missing collection; returns {@code null} when it does not exist.
     * Stores that cannot tell fall back to {@link #collection(Namespace)}.
     */
    default CollectionStore existingCollection(final Namespace namespace) {
        return collection(namespace);
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

    default List<String> listDatabaseNames() {
        return List.of();
    }

    /** Creates an empty collection; returns false when it already exists. */
    default boolean createCollection(final String database, final String collection) {
        if (collectionExists(database, collection)) {
            return false;
        }
        collection(database, collection);
        return true;
    }

    default void renameCollection(
            final String fromDatabase,
            final String fromCollection,
            final String toDatabase,
            final String toCollection,
            final boolean dropTarget) {
        throw new UnsupportedOperationException("renameCollection is not supported");
    }
}
