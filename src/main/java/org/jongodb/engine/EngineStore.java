package org.jongodb.engine;

/**
 * Top-level engine store contract for namespace-scoped collections.
 */
public interface EngineStore {
    CollectionStore collection(Namespace namespace);

    default CollectionStore collection(String database, String collection) {
        return collection(Namespace.of(database, collection));
    }
}
