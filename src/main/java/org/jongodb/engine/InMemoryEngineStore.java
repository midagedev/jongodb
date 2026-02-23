package org.jongodb.engine;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of the engine store.
 */
public final class InMemoryEngineStore implements EngineStore {
    private final ConcurrentMap<Namespace, CollectionStore> collections = new ConcurrentHashMap<>();

    @Override
    public CollectionStore collection(Namespace namespace) {
        Objects.requireNonNull(namespace, "namespace");
        return collections.computeIfAbsent(namespace, key -> new InMemoryCollectionStore());
    }
}
