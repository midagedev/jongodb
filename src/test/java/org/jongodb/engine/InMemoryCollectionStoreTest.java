package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class InMemoryCollectionStoreTest {
    @Test
    void insertManyAndFindAllReturnsInsertedDocuments() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("name", "Ada"),
                        new Document("_id", 2).append("name", "Linus")));

        List<Document> found = store.findAll();
        assertEquals(2, found.size());
        assertEquals("Ada", found.get(0).getString("name"));
        assertEquals("Linus", found.get(1).getString("name"));
    }

    @Test
    void findSupportsSimpleEqualityFilter() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("role", "user").append("active", true),
                        new Document("_id", 2).append("role", "admin").append("active", true),
                        new Document("_id", 3).append("role", "user").append("active", false)));

        List<Document> filtered = store.find(new Document("role", "user").append("active", true));
        assertEquals(1, filtered.size());
        assertEquals(1, filtered.get(0).getInteger("_id"));
    }

    @Test
    void findSupportsDotPathLookup() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("profile", new Document("city", "Seoul")),
                        new Document("_id", 2).append("profile", new Document("city", "Busan"))));

        List<Document> filtered = store.find(new Document("profile.city", "Seoul"));
        assertEquals(1, filtered.size());
        assertEquals(1, filtered.get(0).getInteger("_id"));
    }

    @Test
    void findSupportsArrayContainmentForScalarEquality() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("tags", Arrays.asList("java", "db")),
                        new Document("_id", 2).append("tags", Arrays.asList("ops", "infra"))));

        List<Document> filtered = store.find(new Document("tags", "db"));
        assertEquals(1, filtered.size());
        assertEquals(1, filtered.get(0).getInteger("_id"));
    }

    @Test
    void findSupportsOperatorFiltersForNestedArrayDocuments() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1)
                                .append("tags", Arrays.asList("java", "db"))
                                .append(
                                        "items",
                                        List.of(
                                                new Document("sku", "A").append("qty", 2),
                                                new Document("sku", "B").append("qty", 5))),
                        new Document("_id", 2)
                                .append("tags", Arrays.asList("ops"))
                                .append(
                                        "items",
                                        List.of(new Document("sku", "A").append("qty", 1)))));

        Document filter =
                new Document(
                        "$and",
                        List.of(
                                new Document("tags", new Document("$in", List.of("db"))),
                                new Document("items.sku", "A"),
                                new Document("items.qty", new Document("$gte", 2))));

        List<Document> filtered = store.find(filter);
        assertEquals(1, filtered.size());
        assertEquals(1, filtered.get(0).getInteger("_id"));
    }

    @Test
    void insertManyAppendsToExistingCollectionState() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(Arrays.asList(new Document("_id", 1)));
        store.insertMany(Arrays.asList(new Document("_id", 2), new Document("_id", 3)));

        List<Document> found = store.findAll();
        assertEquals(3, found.size());
        assertEquals(1, found.get(0).getInteger("_id"));
        assertEquals(2, found.get(1).getInteger("_id"));
        assertEquals(3, found.get(2).getInteger("_id"));
    }

    @Test
    void insertManyStoresCopiesOfInputDocuments() {
        CollectionStore store = new InMemoryCollectionStore();

        Document original =
                new Document("_id", 1)
                        .append("name", "before")
                        .append("nested", new Document("city", "Seoul"));

        store.insertMany(Arrays.asList(original));

        original.put("name", "after");
        original.get("nested", Document.class).put("city", "Busan");

        Document stored = store.findAll().get(0);
        assertEquals("before", stored.getString("name"));
        assertEquals("Seoul", stored.get("nested", Document.class).getString("city"));
    }

    @Test
    void findReturnsCopiesOfStoredDocuments() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1)
                                .append("name", "immutable")
                                .append("nested", new Document("city", "Seoul"))));

        List<Document> firstRead = store.findAll();
        firstRead.get(0).put("name", "changed");
        firstRead.get(0).get("nested", Document.class).put("city", "Busan");

        List<Document> secondRead = store.findAll();
        assertEquals("immutable", secondRead.get(0).getString("name"));
        assertEquals("Seoul", secondRead.get(0).get("nested", Document.class).getString("city"));
        assertNotSame(firstRead.get(0), secondRead.get(0));
    }

    @Test
    void namespaceIsolationIsHandledByEngineStore() {
        EngineStore engineStore = new InMemoryEngineStore();
        CollectionStore users = engineStore.collection("appdb", "users");
        CollectionStore orders = engineStore.collection("appdb", "orders");

        users.insertMany(Arrays.asList(new Document("_id", 1).append("kind", "user")));
        orders.insertMany(Arrays.asList(new Document("_id", 2).append("kind", "order")));

        assertEquals(1, users.findAll().size());
        assertEquals(1, orders.findAll().size());
        assertEquals("user", users.findAll().get(0).getString("kind"));
        assertEquals("order", orders.findAll().get(0).getString("kind"));

        CollectionStore usersAgain = engineStore.collection(Namespace.of("appdb", "users"));
        assertEquals(1, usersAgain.findAll().size());
        assertTrue(usersAgain.findAll().get(0).containsKey("_id"));
    }

    @Test
    void updateManyReturnsMatchedAndModifiedCounts() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("role", "user").append("active", true),
                        new Document("_id", 2).append("role", "user").append("active", false),
                        new Document("_id", 3).append("role", "admin").append("active", true)));

        UpdateManyResult result =
                store.updateMany(
                        new Document("role", "user"), new Document("$set", new Document("active", false)));

        assertEquals(2, result.matchedCount());
        assertEquals(1, result.modifiedCount());

        assertFalse(byId(store.findAll(), 1).getBoolean("active"));
        assertFalse(byId(store.findAll(), 2).getBoolean("active"));
        assertTrue(byId(store.findAll(), 3).getBoolean("active"));
    }

    @Test
    void updateManySupportsIncAndUnsetOperators() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("count", 1).append("stale", true),
                        new Document("_id", 2).append("stale", true)));

        UpdateManyResult result =
                store.updateMany(
                        new Document(),
                        new Document("$inc", new Document("count", 2))
                                .append("$unset", new Document("stale", true)));

        assertEquals(2, result.matchedCount());
        assertEquals(2, result.modifiedCount());
        assertEquals(3, byId(store.findAll(), 1).getInteger("count"));
        assertEquals(2, byId(store.findAll(), 2).getInteger("count"));
        assertFalse(byId(store.findAll(), 1).containsKey("stale"));
        assertFalse(byId(store.findAll(), 2).containsKey("stale"));
    }

    @Test
    void updateManyRejectsIncForNonNumericTargetsWithoutPartialUpdates() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("count", 1),
                        new Document("_id", 2).append("count", "oops")));

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                store.updateMany(
                                        new Document(),
                                        new Document("$inc", new Document("count", 1))));
        assertTrue(error.getMessage().contains("$inc target"));

        assertEquals(1, byId(store.findAll(), 1).getInteger("count"));
        assertEquals("oops", byId(store.findAll(), 2).getString("count"));
    }

    @Test
    void updateManySetCopiesValuesPerDocument() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("group", "users"),
                        new Document("_id", 2).append("group", "users")));

        store.updateMany(
                new Document("group", "users"),
                new Document("$set", new Document("profile", new Document("city", "Seoul"))));
        store.updateMany(
                new Document("_id", 1),
                new Document("$set", new Document("profile.city", "Busan")));

        assertEquals("Busan", byId(store.findAll(), 1).get("profile", Document.class).getString("city"));
        assertEquals("Seoul", byId(store.findAll(), 2).get("profile", Document.class).getString("city"));
    }

    @Test
    void updateWithMultiFalseUpdatesOnlyOneMatchedDocument() {
        CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("role", "user").append("active", true),
                        new Document("_id", 2).append("role", "user").append("active", true)));

        UpdateManyResult result =
                store.update(
                        new Document("role", "user"),
                        new Document("$set", new Document("active", false)),
                        false,
                        false);

        assertEquals(1, result.matchedCount());
        assertEquals(1, result.modifiedCount());
        assertFalse(byId(store.findAll(), 1).getBoolean("active"));
        assertTrue(byId(store.findAll(), 2).getBoolean("active"));
    }

    @Test
    void replaceOneReplacesMatchedDocumentAndPreservesId() {
        CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("name", "before").append("extra", true),
                        new Document("_id", 2).append("name", "other")));

        UpdateManyResult result =
                store.update(new Document("_id", 1), new Document("name", "after"), false, false);

        assertEquals(1, result.matchedCount());
        assertEquals(1, result.modifiedCount());
        Document replaced = byId(store.findAll(), 1);
        assertEquals(1, replaced.getInteger("_id"));
        assertEquals("after", replaced.getString("name"));
        assertFalse(replaced.containsKey("extra"));
    }

    @Test
    void updateWithUpsertInsertsOperatorDocumentAndReturnsUpsertedId() {
        CollectionStore store = new InMemoryCollectionStore();

        UpdateManyResult result =
                store.update(
                        new Document("email", "ada@example.com"),
                        new Document("$set", new Document("name", "Ada")),
                        false,
                        true);

        assertEquals(0, result.matchedCount());
        assertEquals(0, result.modifiedCount());
        assertNotNull(result.upsertedId());

        List<Document> all = store.findAll();
        assertEquals(1, all.size());
        assertEquals("ada@example.com", all.get(0).getString("email"));
        assertEquals("Ada", all.get(0).getString("name"));
        assertEquals(result.upsertedId(), all.get(0).get("_id"));
    }

    @Test
    void replaceOneWithUpsertInsertsReplacementAndUsesFilterIdSeed() {
        CollectionStore store = new InMemoryCollectionStore();

        UpdateManyResult result =
                store.update(new Document("_id", 77), new Document("name", "inserted"), false, true);

        assertEquals(0, result.matchedCount());
        assertEquals(0, result.modifiedCount());
        assertEquals(77, result.upsertedId());

        List<Document> all = store.findAll();
        assertEquals(1, all.size());
        assertEquals(77, all.get(0).getInteger("_id"));
        assertEquals("inserted", all.get(0).getString("name"));
    }

    @Test
    void upsertRespectsUniqueIndexesAndRemainsAtomic() {
        CollectionStore store = new InMemoryCollectionStore();
        store.createIndexes(
                List.of(new CollectionStore.IndexDefinition("email_1", new Document("email", 1), true)));
        store.insertMany(List.of(new Document("_id", 1).append("email", "ada@example.com")));

        assertThrows(
                DuplicateKeyException.class,
                () ->
                        store.update(
                                new Document("_id", 2),
                                new Document("$set", new Document("email", "ada@example.com")),
                                false,
                                true));

        List<Document> all = store.findAll();
        assertEquals(1, all.size());
        assertEquals(1, all.get(0).getInteger("_id"));
        assertEquals("ada@example.com", all.get(0).getString("email"));
    }

    @Test
    void deleteManyRemovesMatchingDocumentsAndReturnsCounts() {
        CollectionStore store = new InMemoryCollectionStore();

        store.insertMany(
                Arrays.asList(
                        new Document("_id", 1).append("tags", Arrays.asList("java", "db")),
                        new Document("_id", 2).append("tags", Arrays.asList("ops")),
                        new Document("_id", 3).append("tags", Arrays.asList("db", "search"))));

        DeleteManyResult result = store.deleteMany(new Document("tags", "db"));
        assertEquals(2, result.matchedCount());
        assertEquals(2, result.deletedCount());
        assertEquals(1, store.findAll().size());
        assertEquals(2, store.findAll().get(0).getInteger("_id"));
    }

    private static Document byId(List<Document> documents, int id) {
        for (Document document : documents) {
            if (document.getInteger("_id") == id) {
                return document;
            }
        }
        throw new AssertionError("missing document for _id=" + id);
    }
}
