package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class InMemoryCollectionStoreAggregateTest {
    @Test
    void aggregateSupportsMatchProjectSortSkipAndLimit() {
        final CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(Arrays.asList(
                new Document("_id", 1)
                        .append("status", "active")
                        .append("name", "Zara")
                        .append("profile", new Document("city", "Seoul")),
                new Document("_id", 2)
                        .append("status", "active")
                        .append("name", "Adam")
                        .append("profile", new Document("city", "Busan")),
                new Document("_id", 3)
                        .append("status", "inactive")
                        .append("name", "Mike")
                        .append("profile", new Document("city", "Seoul")),
                new Document("_id", 4)
                        .append("status", "active")
                        .append("name", "Bella")
                        .append("profile", new Document("city", "Seoul"))));

        final List<Document> aggregated = store.aggregate(List.of(
                new Document("$match", new Document("status", "active")),
                new Document("$project", new Document("_id", 0).append("name", 1).append("city", "$profile.city")),
                new Document("$sort", new Document("name", 1)),
                new Document("$skip", 1),
                new Document("$limit", 1)));

        assertEquals(1, aggregated.size());
        assertFalse(aggregated.get(0).containsKey("_id"));
        assertEquals("Bella", aggregated.get(0).getString("name"));
        assertEquals("Seoul", aggregated.get(0).getString("city"));
    }

    @Test
    void aggregateSupportsUnwindGroupAndSort() {
        final CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(Arrays.asList(
                new Document("_id", 1).append("qty", 2).append("tags", Arrays.asList("a", "b")),
                new Document("_id", 2).append("qty", 3).append("tags", Arrays.asList("a")),
                new Document("_id", 3).append("qty", 5).append("tags", Arrays.asList())));

        final List<Document> aggregated = store.aggregate(List.of(
                new Document("$unwind", "$tags"),
                new Document(
                        "$group",
                        new Document("_id", "$tags")
                                .append("totalQty", new Document("$sum", "$qty"))
                                .append("count", new Document("$sum", 1))),
                new Document("$sort", new Document("_id", 1))));

        assertEquals(2, aggregated.size());

        assertEquals("a", aggregated.get(0).getString("_id"));
        assertEquals(5L, asLong(aggregated.get(0).get("totalQty")));
        assertEquals(2L, asLong(aggregated.get(0).get("count")));

        assertEquals("b", aggregated.get(1).getString("_id"));
        assertEquals(2L, asLong(aggregated.get(1).get("totalQty")));
        assertEquals(1L, asLong(aggregated.get(1).get("count")));
    }

    @Test
    void aggregateUnwindSupportsPreserveNullAndEmptyArrays() {
        final CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(Arrays.asList(
                new Document("_id", 1).append("tags", Arrays.asList("x", "y")),
                new Document("_id", 2).append("tags", Arrays.asList()),
                new Document("_id", 3),
                new Document("_id", 4).append("tags", null)));

        final List<Document> aggregated = store.aggregate(List.of(new Document(
                "$unwind",
                new Document("path", "$tags").append("preserveNullAndEmptyArrays", true))));

        assertEquals(5, aggregated.size());
        assertEquals(2, countById(aggregated, 1));
        assertEquals(1, countById(aggregated, 2));
        assertEquals(1, countById(aggregated, 3));
        assertEquals(1, countById(aggregated, 4));
    }

    @Test
    void aggregateCountReturnsSingleDocumentOrEmptyResult() {
        final CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(Arrays.asList(
                new Document("_id", 1).append("active", true),
                new Document("_id", 2).append("active", true),
                new Document("_id", 3).append("active", false)));

        final List<Document> counted = store.aggregate(List.of(
                new Document("$match", new Document("active", true)),
                new Document("$count", "total")));

        assertEquals(1, counted.size());
        assertEquals(2, counted.get(0).getInteger("total"));

        final List<Document> emptyCount = store.aggregate(List.of(
                new Document("$match", new Document("active", "missing")),
                new Document("$count", "total")));
        assertTrue(emptyCount.isEmpty());
    }

    @Test
    void aggregateSupportsAddFieldsAndSortByCount() {
        final CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(Arrays.asList(
                new Document("_id", 1).append("profile", new Document("city", "Seoul")),
                new Document("_id", 2).append("profile", new Document("city", "Busan")),
                new Document("_id", 3).append("profile", new Document("city", "Seoul"))));

        final List<Document> aggregated = store.aggregate(List.of(
                new Document("$addFields", new Document("city", "$profile.city")),
                new Document("$sortByCount", "$city")));

        assertEquals(2, aggregated.size());
        assertEquals("Seoul", aggregated.get(0).getString("_id"));
        assertEquals(2L, asLong(aggregated.get(0).get("count")));
        assertEquals("Busan", aggregated.get(1).getString("_id"));
        assertEquals(1L, asLong(aggregated.get(1).get("count")));
    }

    @Test
    void aggregateSupportsFacetWithIndependentPipelines() {
        final CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(Arrays.asList(
                new Document("_id", 1)
                        .append("status", "active")
                        .append("profile", new Document("city", "Seoul")),
                new Document("_id", 2)
                        .append("status", "active")
                        .append("profile", new Document("city", "Busan")),
                new Document("_id", 3)
                        .append("status", "inactive")
                        .append("profile", new Document("city", "Seoul"))));

        final List<Document> aggregated = store.aggregate(List.of(new Document(
                "$facet",
                new Document(
                        "activeOnly",
                        List.of(
                                new Document("$match", new Document("status", "active")),
                                new Document("$count", "total")))
                        .append(
                                "byCity",
                                List.of(
                                        new Document(
                                                "$group",
                                                new Document("_id", "$profile.city")
                                                        .append("total", new Document("$sum", 1))),
                                        new Document("$sort", new Document("_id", 1)))))));

        assertEquals(1, aggregated.size());
        final Document facetResult = aggregated.get(0);

        @SuppressWarnings("unchecked")
        final List<Document> activeOnly = (List<Document>) facetResult.get("activeOnly");
        assertEquals(1, activeOnly.size());
        assertEquals(2, activeOnly.get(0).getInteger("total"));

        @SuppressWarnings("unchecked")
        final List<Document> byCity = (List<Document>) facetResult.get("byCity");
        assertEquals(2, byCity.size());
        assertEquals("Busan", byCity.get(0).getString("_id"));
        assertEquals(1L, asLong(byCity.get(0).get("total")));
        assertEquals("Seoul", byCity.get(1).getString("_id"));
        assertEquals(2L, asLong(byCity.get(1).get("total")));
    }

    @Test
    void aggregateGroupSupportsAddToSetAccumulator() {
        final CollectionStore store = new InMemoryCollectionStore();
        store.insertMany(Arrays.asList(
                new Document("_id", 1).append("category", "a").append("color", "red"),
                new Document("_id", 2).append("category", "a").append("color", "blue"),
                new Document("_id", 3).append("category", "a").append("color", "red"),
                new Document("_id", 4).append("category", "b").append("color", "green")));

        final List<Document> aggregated = store.aggregate(List.of(
                new Document(
                        "$group",
                        new Document("_id", "$category")
                                .append("colors", new Document("$addToSet", "$color"))),
                new Document("$sort", new Document("_id", 1))));

        assertEquals(2, aggregated.size());
        assertEquals("a", aggregated.get(0).getString("_id"));
        assertEquals(List.of("red", "blue"), aggregated.get(0).getList("colors", Object.class));
        assertEquals("b", aggregated.get(1).getString("_id"));
        assertEquals(List.of("green"), aggregated.get(1).getList("colors", Object.class));
    }

    private static long asLong(final Object value) {
        if (!(value instanceof Number number)) {
            throw new AssertionError("expected numeric value but got: " + value);
        }
        return number.longValue();
    }

    private static int countById(final List<Document> documents, final int id) {
        int count = 0;
        for (final Document document : documents) {
            if (id == document.getInteger("_id")) {
                count++;
            }
        }
        return count;
    }
}
