package org.jongodb.engine;

import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of the engine store.
 */
public final class InMemoryEngineStore implements EngineStore {
    private final ConcurrentMap<Namespace, InMemoryCollectionStore> collections = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryEngineStore() {
        this(Clock.systemUTC());
    }

    public InMemoryEngineStore(final Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CollectionStore collection(Namespace namespace) {
        Objects.requireNonNull(namespace, "namespace");
        return collections.computeIfAbsent(namespace, key -> new InMemoryCollectionStore(clock));
    }

    @Override
    public boolean collectionExists(final Namespace namespace) {
        Objects.requireNonNull(namespace, "namespace");
        return collections.containsKey(namespace);
    }

    @Override
    public synchronized List<String> listCollectionNames(final String database) {
        Objects.requireNonNull(database, "database");
        return collections.keySet().stream()
                .filter(namespace -> namespace.database().equals(database))
                .map(Namespace::collection)
                .sorted()
                .toList();
    }

    @Override
    public synchronized boolean dropCollection(final String database, final String collection) {
        return collections.remove(Namespace.of(database, collection)) != null;
    }

    @Override
    public synchronized int dropDatabase(final String database) {
        Objects.requireNonNull(database, "database");
        int dropped = 0;
        for (final Namespace namespace : List.copyOf(collections.keySet())) {
            if (namespace.database().equals(database) && collections.remove(namespace) != null) {
                dropped++;
            }
        }
        return dropped;
    }

    public synchronized InMemoryEngineStore snapshot() {
        final InMemoryEngineStore snapshot = new InMemoryEngineStore(clock);
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

    public void mergeTransactionSnapshot(
            final InMemoryEngineStore baselineSnapshot, final InMemoryEngineStore transactionSnapshot) {
        Objects.requireNonNull(baselineSnapshot, "baselineSnapshot");
        Objects.requireNonNull(transactionSnapshot, "transactionSnapshot");

        final Map<Namespace, InMemoryCollectionStore.CollectionState> baselineStates =
                baselineSnapshot.collectionStatesSnapshot();
        final Map<Namespace, InMemoryCollectionStore.CollectionState> transactionStates =
                transactionSnapshot.collectionStatesSnapshot();
        final Map<Namespace, InMemoryCollectionStore.CollectionState> currentStates = collectionStatesSnapshot();

        final Set<Namespace> touchedNamespaces = new HashSet<>();
        touchedNamespaces.addAll(baselineStates.keySet());
        touchedNamespaces.addAll(transactionStates.keySet());

        synchronized (this) {
            for (final Namespace namespace : touchedNamespaces) {
                final InMemoryCollectionStore.CollectionState baselineState = baselineStates.get(namespace);
                final InMemoryCollectionStore.CollectionState transactionState = transactionStates.get(namespace);
                if (InMemoryCollectionStore.statesEqual(baselineState, transactionState)) {
                    continue;
                }

                final InMemoryCollectionStore.CollectionState currentState = currentStates.get(namespace);
                final InMemoryCollectionStore.CollectionState mergedState =
                        InMemoryCollectionStore.mergeTransactionState(baselineState, transactionState, currentState);
                collections.computeIfAbsent(namespace, key -> new InMemoryCollectionStore(clock))
                        .replaceState(mergedState);
            }
        }
    }

    private synchronized Map<Namespace, InMemoryCollectionStore.CollectionState> collectionStatesSnapshot() {
        final Map<Namespace, InMemoryCollectionStore.CollectionState> states = new LinkedHashMap<>();
        for (final var entry : collections.entrySet()) {
            states.put(entry.getKey(), entry.getValue().snapshotState());
        }
        return states;
    }
}
