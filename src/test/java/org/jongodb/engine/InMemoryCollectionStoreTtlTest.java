package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class InMemoryCollectionStoreTtlTest {
    @Test
    void findAllPrunesExpiredDateDocumentsAtExpiryBoundary() {
        final MutableClock clock = new MutableClock(Instant.parse("2026-05-09T00:00:10Z"));
        final InMemoryCollectionStore store = new InMemoryCollectionStore(clock);
        store.createIndexes(List.of(ttlIndex("expiresAt_1", new Document("expiresAt", 1), 10L)));
        store.insertMany(List.of(
                new Document("_id", 1).append("expiresAt", Date.from(Instant.parse("2026-05-09T00:00:00Z"))),
                new Document("_id", 2).append("expiresAt", Date.from(Instant.parse("2026-05-09T00:00:01Z")))));

        final List<Document> remaining = store.findAll();

        assertEquals(1, remaining.size());
        assertEquals(2, remaining.get(0).getInteger("_id"));
    }

    @Test
    void pruneSupportsInstantValues() {
        final MutableClock clock = new MutableClock(Instant.parse("2026-05-09T00:00:10Z"));
        final InMemoryCollectionStore store = new InMemoryCollectionStore(clock);
        store.createIndexes(List.of(ttlIndex("expiresAt_1", new Document("expiresAt", 1), 10L)));
        store.insertMany(List.of(
                new Document("_id", 1).append("expiresAt", Instant.parse("2026-05-09T00:00:00Z")),
                new Document("_id", 2).append("expiresAt", Instant.parse("2026-05-09T00:00:01Z"))));

        final List<Document> remaining = store.findAll();

        assertEquals(1, remaining.size());
        assertEquals(2, remaining.get(0).getInteger("_id"));
    }

    @Test
    void pruneIgnoresTierZeroUnsupportedValuesAndIndexes() {
        final MutableClock clock = new MutableClock(Instant.parse("2026-05-09T00:00:10Z"));
        final InMemoryCollectionStore store = new InMemoryCollectionStore(clock);
        store.createIndexes(List.of(
                ttlIndex("expiresAt_1", new Document("expiresAt", 1), 10L),
                ttlIndex("compound_1", new Document("compoundAt", 1).append("tenantId", 1), 10L),
                new CollectionStore.IndexDefinition(
                        "partialAt_1",
                        new Document("partialAt", 1),
                        false,
                        false,
                        new Document("active", true),
                        null,
                        10L)));
        store.insertMany(List.of(
                new Document("_id", 1),
                new Document("_id", 2).append("expiresAt", null),
                new Document("_id", 3).append("expiresAt", "2026-05-09T00:00:00Z"),
                new Document("_id", 4).append("expiresAt", List.of(Date.from(Instant.parse("2026-05-09T00:00:00Z")))),
                new Document("_id", 5).append("compoundAt", Date.from(Instant.parse("2026-05-09T00:00:00Z"))),
                new Document("_id", 6).append("partialAt", Date.from(Instant.parse("2026-05-09T00:00:00Z"))),
                new Document("_id", 7).append("expiresAt", Date.from(Instant.parse("2026-05-09T00:00:00Z")))));

        final List<Document> remaining = store.findAll();

        assertEquals(List.of(1, 2, 3, 4, 5, 6), ids(remaining));
    }

    @Test
    void insertManyPrunesExpiredDocumentsBeforeUniqueValidation() {
        final MutableClock clock = new MutableClock(Instant.parse("2026-05-09T00:00:10Z"));
        final InMemoryCollectionStore store = new InMemoryCollectionStore(clock);
        store.createIndexes(List.of(
                ttlIndex("expiresAt_1", new Document("expiresAt", 1), 10L),
                new CollectionStore.IndexDefinition("email_1", new Document("email", 1), true)));
        store.insertMany(List.of(new Document("_id", 1)
                .append("email", "ada@example.com")
                .append("expiresAt", Date.from(Instant.parse("2026-05-09T00:00:00Z")))));

        store.insertMany(List.of(new Document("_id", 2).append("email", "ada@example.com")));

        final List<Document> remaining = store.findAll();
        assertEquals(1, remaining.size());
        assertEquals(2, remaining.get(0).getInteger("_id"));
    }

    @Test
    void engineSnapshotsCarryInjectedClock() {
        final MutableClock clock = new MutableClock(Instant.parse("2026-05-09T00:00:09Z"));
        final InMemoryEngineStore engine = new InMemoryEngineStore(clock);
        final CollectionStore collection = engine.collection("app", "sessions");
        collection.createIndexes(List.of(ttlIndex("expiresAt_1", new Document("expiresAt", 1), 10L)));
        collection.insertMany(List.of(new Document("_id", 1)
                .append("expiresAt", Date.from(Instant.parse("2026-05-09T00:00:00Z")))));

        final InMemoryEngineStore snapshot = engine.snapshot();
        clock.setInstant(Instant.parse("2026-05-09T00:00:10Z"));

        assertEquals(0, snapshot.collection("app", "sessions").findAll().size());
    }

    private static CollectionStore.IndexDefinition ttlIndex(
            final String name, final Document key, final Long expireAfterSeconds) {
        return new CollectionStore.IndexDefinition(name, key, false, false, null, expireAfterSeconds);
    }

    private static List<Integer> ids(final List<Document> documents) {
        return documents.stream().map(document -> document.getInteger("_id")).toList();
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(final Instant instant) {
            this.instant = instant;
        }

        private void setInstant(final Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
