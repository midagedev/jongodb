package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.Document;
import org.jongodb.engine.CollectionStore;
import org.jongodb.engine.DeleteManyResult;
import org.jongodb.engine.EngineStore;
import org.jongodb.engine.Namespace;
import org.jongodb.engine.UpdateManyResult;
import org.junit.jupiter.api.Test;

class EngineBackedCommandStoreAggregateStreamingTest {
    @Test
    void aggregateUsesScanAllForPrimaryCollection() {
        final TrackingCollectionStore primary = new TrackingCollectionStore(List.of(
                new Document("_id", 1).append("category", "active"),
                new Document("_id", 2).append("category", "archived")));
        final EngineStore store = namespace -> primary;
        final EngineBackedCommandStore commandStore = new EngineBackedCommandStore(store);

        final List<BsonDocument> result = commandStore.aggregate(
                "app",
                "users",
                List.of(BsonDocument.parse("{\"$match\":{\"category\":\"active\"}}")));

        assertEquals(1, result.size());
        assertEquals(1, primary.scanAllCalls());
        assertFalse(primary.findAllCalled());
    }

    @Test
    void aggregateUsesScanAllForForeignResolverCollections() {
        final TrackingCollectionStore primary = new TrackingCollectionStore(List.of(new Document("_id", 1).append("v", "a")));
        final TrackingCollectionStore archive = new TrackingCollectionStore(List.of(new Document("_id", 10).append("v", "b")));
        final Map<Namespace, CollectionStore> stores = new LinkedHashMap<>();
        stores.put(Namespace.of("app", "users"), primary);
        stores.put(Namespace.of("app", "archive"), archive);

        final EngineStore store = namespace -> stores.getOrDefault(namespace, new TrackingCollectionStore(List.of()));
        final EngineBackedCommandStore commandStore = new EngineBackedCommandStore(store);

        final List<BsonDocument> result = commandStore.aggregate(
                "app",
                "users",
                List.of(BsonDocument.parse("{\"$unionWith\":\"archive\"}")));

        assertEquals(2, result.size());
        assertEquals(1, primary.scanAllCalls());
        assertEquals(1, archive.scanAllCalls());
        assertFalse(primary.findAllCalled());
        assertFalse(archive.findAllCalled());
    }

    private static final class TrackingCollectionStore implements CollectionStore {
        private final List<Document> documents;
        private int scanAllCalls;
        private boolean findAllCalled;

        private TrackingCollectionStore(final List<Document> documents) {
            this.documents = documents.stream()
                    .map(TrackingCollectionStore::copyDocument)
                    .toList();
        }

        private int scanAllCalls() {
            return scanAllCalls;
        }

        private boolean findAllCalled() {
            return findAllCalled;
        }

        @Override
        public Iterable<Document> scanAll() {
            scanAllCalls++;
            final List<Document> snapshot = documents;
            return new Iterable<>() {
                @Override
                public Iterator<Document> iterator() {
                    final Iterator<Document> iterator = snapshot.iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public Document next() {
                            return copyDocument(iterator.next());
                        }
                    };
                }
            };
        }

        @Override
        public List<Document> findAll() {
            findAllCalled = true;
            throw new AssertionError("findAll should not be used by aggregate path");
        }

        @Override
        public void insertMany(final List<Document> documents) {
            throw new UnsupportedOperationException("not required for aggregate path test");
        }

        @Override
        public CreateIndexesResult createIndexes(final List<IndexDefinition> indexes) {
            throw new UnsupportedOperationException("not required for aggregate path test");
        }

        @Override
        public List<IndexDefinition> listIndexes() {
            return List.of();
        }

        @Override
        public List<Document> find(final Document filter) {
            throw new UnsupportedOperationException("not required for aggregate path test");
        }

        @Override
        public UpdateManyResult update(final Document filter, final Document update, final boolean multi, final boolean upsert) {
            throw new UnsupportedOperationException("not required for aggregate path test");
        }

        @Override
        public DeleteManyResult deleteMany(final Document filter) {
            throw new UnsupportedOperationException("not required for aggregate path test");
        }

        private static Document copyDocument(final Document source) {
            return Document.parse(source.toJson());
        }
    }
}

