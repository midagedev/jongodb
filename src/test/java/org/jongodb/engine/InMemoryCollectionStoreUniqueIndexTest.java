package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

class InMemoryCollectionStoreUniqueIndexTest {
    @Test
    void numericKeysCollideAcrossNumericTypes() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("n", null);
        store.insertMany(List.of(new Document("_id", "int").append("n", 1)));

        for (final Object equalValue : List.of(1L, 1.0d, 1.0f, new Decimal128(BigDecimal.ONE), new BigDecimal("1.000"))) {
            assertThrows(
                    DuplicateKeyException.class,
                    () -> store.insertMany(List.of(new Document("_id", "other").append("n", equalValue))),
                    () -> "expected duplicate for " + equalValue.getClass().getSimpleName());
        }
        assertDoesNotThrow(() -> store.insertMany(List.of(new Document("_id", "two").append("n", 2.0d))));
        assertEquals(2, store.findAll().size());
    }

    @Test
    void zeroKeysCollideIncludingNegativeZeros() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("n", null);
        store.insertMany(List.of(new Document("_id", 1).append("n", 0)));
        assertThrows(
                DuplicateKeyException.class,
                () -> store.insertMany(List.of(new Document("_id", 2).append("n", -0.0d))));

        final InMemoryCollectionStore decimalStore = storeWithUniqueIndex("n", null);
        decimalStore.insertMany(List.of(new Document("_id", 1).append("n", Decimal128.NEGATIVE_ZERO)));
        assertThrows(
                DuplicateKeyException.class,
                () -> decimalStore.insertMany(List.of(new Document("_id", 2).append("n", -0.0d))));
    }

    @Test
    void stringKeysFollowIndexCollationStrength() {
        final InMemoryCollectionStore caseInsensitive =
                storeWithUniqueIndex("email", new Document("locale", "en").append("strength", 2));
        caseInsensitive.insertMany(List.of(new Document("_id", 1).append("email", "Ada@Example.com")));
        assertThrows(
                DuplicateKeyException.class,
                () -> caseInsensitive.insertMany(List.of(new Document("_id", 2).append("email", "ada@example.COM"))));
        assertDoesNotThrow(
                () -> caseInsensitive.insertMany(List.of(new Document("_id", 3).append("email", "adá@example.com"))));

        final InMemoryCollectionStore accentInsensitive =
                storeWithUniqueIndex("name", new Document("locale", "en").append("strength", 1));
        accentInsensitive.insertMany(List.of(new Document("_id", 1).append("name", "cafe")));
        assertThrows(
                DuplicateKeyException.class,
                () -> accentInsensitive.insertMany(List.of(new Document("_id", 2).append("name", "Café"))));

        final InMemoryCollectionStore caseLevel = storeWithUniqueIndex(
                "name", new Document("locale", "en").append("strength", 1).append("caseLevel", true));
        caseLevel.insertMany(List.of(new Document("_id", 1).append("name", "cafe")));
        assertDoesNotThrow(() -> caseLevel.insertMany(List.of(new Document("_id", 2).append("name", "Cafe"))));
    }

    @Test
    void duplicatesWithinOneInsertBatchAreRejectedAtomically() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("email", null);
        store.insertMany(List.of(new Document("_id", 1).append("email", "a@example.com")));

        final DuplicateKeyException error = assertThrows(
                DuplicateKeyException.class,
                () -> store.insertMany(List.of(
                        new Document("_id", 2).append("email", "b@example.com"),
                        new Document("_id", 3).append("email", "c@example.com"),
                        new Document("_id", 4).append("email", "b@example.com"))));

        assertTrue(error.getMessage().contains("email_1"), error.getMessage());
        assertTrue(error.getMessage().contains("b@example.com"), error.getMessage());
        assertEquals(1, store.findAll().size());
    }

    @Test
    void insertBatchReportsEarliestCollidingDocument() {
        final InMemoryCollectionStore store =
                storeWithUniqueIndex("email", new Document("locale", "en").append("strength", 2));
        store.insertMany(List.of(new Document("_id", 1).append("email", "Stored@example.com")));

        final DuplicateKeyException error = assertThrows(
                DuplicateKeyException.class,
                () -> store.insertMany(List.of(
                        new Document("_id", 2).append("email", "Batch@example.com"),
                        new Document("_id", 3).append("email", "STORED@example.com"),
                        new Document("_id", 4).append("email", "batch@example.com"))));

        assertTrue(error.getMessage().contains("STORED@example.com"), error.getMessage());
    }

    @Test
    void updateThatCreatesDuplicateIsRejectedAndLeavesDocumentsUnchanged() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("email", null);
        store.insertMany(List.of(
                new Document("_id", 1).append("email", "a@example.com"),
                new Document("_id", 2).append("email", "b@example.com")));

        assertThrows(
                DuplicateKeyException.class,
                () -> store.update(
                        new Document("_id", 2),
                        new Document("$set", new Document("email", "a@example.com")),
                        false,
                        false));

        assertEquals("b@example.com", store.find(new Document("_id", 2)).get(0).getString("email"));
    }

    @Test
    void updateReportsLaterDocumentInCollectionOrder() {
        final InMemoryCollectionStore store =
                storeWithUniqueIndex("email", new Document("locale", "en").append("strength", 2));
        store.insertMany(List.of(
                new Document("_id", 1).append("email", "first@example.com"),
                new Document("_id", 2).append("email", "Later@example.com")));

        final DuplicateKeyException modifiedFirst = assertThrows(
                DuplicateKeyException.class,
                () -> store.update(
                        new Document("_id", 1),
                        new Document("$set", new Document("email", "LATER@example.com")),
                        false,
                        false));
        assertTrue(modifiedFirst.getMessage().contains("Later@example.com"), modifiedFirst.getMessage());

        final DuplicateKeyException modifiedLast = assertThrows(
                DuplicateKeyException.class,
                () -> store.update(
                        new Document("_id", 2),
                        new Document("$set", new Document("email", "FIRST@example.com")),
                        false,
                        false));
        assertTrue(modifiedLast.getMessage().contains("FIRST@example.com"), modifiedLast.getMessage());
    }

    @Test
    void multiUpdateRejectsDocumentsMovedOntoTheSameKey() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("slot", null);
        store.insertMany(List.of(
                new Document("_id", 1).append("slot", 1).append("group", "g"),
                new Document("_id", 2).append("slot", 2).append("group", "g"),
                new Document("_id", 3).append("slot", 3).append("group", "other")));

        assertThrows(
                DuplicateKeyException.class,
                () -> store.update(
                        new Document("group", "g"),
                        new Document("$set", new Document("slot", 9)),
                        true,
                        false));

        assertEquals(List.of(1, 2, 3), slots(store));
    }

    @Test
    void updateKeepingItsOwnKeyOrMovingToFreeKeyIsAccepted() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("email", null);
        store.insertMany(List.of(
                new Document("_id", 1).append("email", "a@example.com").append("visits", 0),
                new Document("_id", 2).append("email", "b@example.com").append("visits", 0)));

        assertDoesNotThrow(() -> store.update(
                new Document(),
                new Document("$inc", new Document("visits", 1)),
                true,
                false));
        assertDoesNotThrow(() -> store.update(
                new Document("_id", 1),
                new Document("$set", new Document("email", "a@example.com").append("visits", 5)),
                false,
                false));
        assertDoesNotThrow(() -> store.update(
                new Document("_id", 2),
                new Document("$set", new Document("email", "c@example.com")),
                false,
                false));

        assertEquals("c@example.com", store.find(new Document("_id", 2)).get(0).getString("email"));
    }

    @Test
    void pipelineUpdateThatCreatesDuplicateIsRejected() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("email", null);
        store.insertMany(List.of(
                new Document("_id", 1).append("email", "a@example.com"),
                new Document("_id", 2).append("email", "b@example.com")));

        assertThrows(
                DuplicateKeyException.class,
                () -> store.updatePipeline(
                        new Document("_id", 2),
                        List.of(new Document("$set", new Document("email", "a@example.com"))),
                        false,
                        false));
        assertEquals("b@example.com", store.find(new Document("_id", 2)).get(0).getString("email"));
    }

    @Test
    void partialAndSparseIndexesOnlyConstrainCoveredDocuments() {
        final InMemoryCollectionStore partial = new InMemoryCollectionStore();
        partial.createIndexes(List.of(new CollectionStore.IndexDefinition(
                "email_active", new Document("email", 1), true, false, new Document("active", true), null)));
        partial.insertMany(List.of(
                new Document("_id", 1).append("email", "a@example.com").append("active", false),
                new Document("_id", 2).append("email", "a@example.com").append("active", false),
                new Document("_id", 3).append("email", "a@example.com").append("active", true)));
        assertThrows(
                DuplicateKeyException.class,
                () -> partial.insertMany(
                        List.of(new Document("_id", 4).append("email", "a@example.com").append("active", true))));

        final InMemoryCollectionStore sparse = new InMemoryCollectionStore();
        sparse.createIndexes(List.of(new CollectionStore.IndexDefinition(
                "nick_1", new Document("nick", 1), true, true, null, null)));
        sparse.insertMany(List.of(new Document("_id", 1), new Document("_id", 2)));
        sparse.insertMany(List.of(new Document("_id", 3).append("nick", null)));
        assertThrows(
                DuplicateKeyException.class,
                () -> sparse.insertMany(List.of(new Document("_id", 4).append("nick", null))));
        assertEquals(3, sparse.findAll().size());
    }

    @Test
    void embeddedDocumentAndArrayKeysCompareByValue() {
        final InMemoryCollectionStore documents = storeWithUniqueIndex("profile", null);
        documents.insertMany(List.of(new Document("_id", 1).append("profile", new Document("a", 1).append("b", 2))));
        assertThrows(
                DuplicateKeyException.class,
                () -> documents.insertMany(List.of(
                        new Document("_id", 2).append("profile", new Document("b", 2L).append("a", 1.0d)))));

        final InMemoryCollectionStore arrays = storeWithUniqueIndex("tags", null);
        arrays.insertMany(List.of(new Document("_id", 1).append("tags", List.of(1, 2))));
        assertThrows(
                DuplicateKeyException.class,
                () -> arrays.insertMany(List.of(new Document("_id", 2).append("tags", List.of(1L, 2.0d)))));
        assertDoesNotThrow(() -> arrays.insertMany(List.of(new Document("_id", 3).append("tags", List.of(2, 1)))));
    }

    @Test
    void upsertAgainstLargeCollectionStillDetectsDuplicate() {
        final InMemoryCollectionStore store = storeWithUniqueIndex("email", null);
        final List<Document> seed = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            seed.add(new Document("_id", i).append("email", "user" + i + "@example.com"));
        }
        store.insertMany(seed);

        assertThrows(
                DuplicateKeyException.class,
                () -> store.update(
                        new Document("_id", "new"),
                        new Document("$set", new Document("email", "user4999@example.com")),
                        false,
                        true));
        assertEquals(5_000, store.findAll().size());
    }

    private static InMemoryCollectionStore storeWithUniqueIndex(final String field, final Document collation) {
        final InMemoryCollectionStore store = new InMemoryCollectionStore();
        store.createIndexes(List.of(new CollectionStore.IndexDefinition(
                field + "_1", new Document(field, 1), true, false, null, collation, null)));
        return store;
    }

    private static List<Integer> slots(final InMemoryCollectionStore store) {
        final List<Integer> slots = new ArrayList<>();
        for (final Document document : store.findAll()) {
            slots.add(document.getInteger("slot"));
        }
        return slots;
    }
}
