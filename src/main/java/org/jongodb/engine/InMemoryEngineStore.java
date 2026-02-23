package org.jongodb.engine;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of the engine store.
 */
public final class InMemoryEngineStore implements EngineStore {
    private final ConcurrentMap<Namespace, InMemoryCollectionStore> collections = new ConcurrentHashMap<>();

    @Override
    public CollectionStore collection(Namespace namespace) {
        Objects.requireNonNull(namespace, "namespace");
        return collections.computeIfAbsent(namespace, key -> new InMemoryCollectionStore());
    }

    public synchronized InMemoryEngineStore snapshot() {
        final InMemoryEngineStore snapshot = new InMemoryEngineStore();
        for (final var entry : collections.entrySet()) {
            snapshot.collections.put(entry.getKey(), entry.getValue().snapshot());
        }
        return snapshot;
    }

    public synchronized void replaceWith(final InMemoryEngineStore source) {
        Objects.requireNonNull(source, "source");
        collections.clear();
        for (final var entry : source.collections.entrySet()) {
            collections.put(entry.getKey(), entry.getValue().snapshot());
        }
    }
}
