package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
}
