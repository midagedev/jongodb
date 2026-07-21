package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.time.Instant;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class AggregationPipelineTest {
    @Test
    void executeDoesNotMutateSourceDocumentsForSetAndUnset() {
        final Document sourceDocument = new Document("_id", 1)
                .append("name", "alpha")
                .append("legacy", true)
                .append("profile", new Document("city", "Seoul"));
        final List<Document> source = List.of(sourceDocument);

        final List<Document> result = AggregationPipeline.execute(
                source,
                List.of(
                        new Document("$set", new Document("profile.city", "Busan")),
                        new Document("$unset", "legacy")));

        assertEquals("Seoul", sourceDocument.get("profile", Document.class).getString("city"));
        assertTrue(sourceDocument.getBoolean("legacy"));
        assertEquals(1, result.size());
        assertNotSame(sourceDocument, result.get(0));
        assertEquals("Busan", result.get(0).get("profile", Document.class).getString("city"));
        assertFalse(result.get(0).containsKey("legacy"));
    }

    @Test
    void executeDoesNotMutateSourceDocumentsForUnwind() {
        final Document sourceDocument = new Document("_id", 1).append("tags", List.of("a", "b"));
        final List<Document> source = List.of(sourceDocument);

        final List<Document> result =
                AggregationPipeline.execute(source, List.of(new Document("$unwind", "$tags")));

        assertEquals(List.of("a", "b"), sourceDocument.getList("tags", String.class));
        assertEquals(2, result.size());
        final List<Object> unwindValues = new ArrayList<>();
        for (final Document document : result) {
            unwindValues.add(document.get("tags"));
        }
        assertEquals(List.of("a", "b"), unwindValues);
    }

    @Test
    void unwindSupportsIncludeArrayIndex() {
        final List<Document> source = List.of(
                new Document("_id", 1).append("tags", List.of("a", "b")),
                new Document("_id", 2).append("tags", "scalar"),
                new Document("_id", 3).append("tags", List.of()),
                new Document("_id", 4),
                new Document("_id", 5).append("tags", null));

        final List<Document> result = AggregationPipeline.execute(
                source,
                List.of(new Document(
                        "$unwind",
                        new Document("path", "$tags")
                                .append("includeArrayIndex", "tagIndex")
                                .append("preserveNullAndEmptyArrays", true))));

        assertEquals(6, result.size());
        assertEquals("a", result.get(0).getString("tags"));
        assertEquals(0L, result.get(0).get("tagIndex"));
        assertEquals("b", result.get(1).getString("tags"));
        assertEquals(1L, result.get(1).get("tagIndex"));
        assertEquals("scalar", result.get(2).getString("tags"));
        assertTrue(result.get(2).containsKey("tagIndex"));
        assertNull(result.get(2).get("tagIndex"));
        assertFalse(result.get(3).containsKey("tags"));
        assertTrue(result.get(3).containsKey("tagIndex"));
        assertNull(result.get(3).get("tagIndex"));
        assertFalse(result.get(4).containsKey("tags"));
        assertTrue(result.get(4).containsKey("tagIndex"));
        assertNull(result.get(4).get("tagIndex"));
        assertTrue(result.get(5).containsKey("tags"));
        assertNull(result.get(5).get("tags"));
        assertTrue(result.get(5).containsKey("tagIndex"));
        assertNull(result.get(5).get("tagIndex"));
    }

    @Test
    void unwindRejectsInvalidIncludeArrayIndexName() {
        final List<Document> source = List.of(new Document("_id", 1).append("tags", List.of("a")));

        assertThrows(
                IllegalArgumentException.class,
                () -> AggregationPipeline.execute(
                        source,
                        List.of(new Document(
                                "$unwind",
                                new Document("path", "$tags").append("includeArrayIndex", "$index")))));
    }

    @Test
    void groupSupportsCompoundIdsCoreAccumulatorsAndMergeObjects() {
        final List<Document> source = List.of(
                new Document("event", "run").append("tenant", "crown").append("duration", 10).append("labels", new Document("a", 1)),
                new Document("event", "run").append("tenant", "crown").append("duration", 30.0d).append("labels", new Document("b", 2)),
                new Document("event", "run").append("tenant", "crown").append("labels", new Document("a", 3)));
        final Document group = new Document("_id", new Document("event", "$event").append("tenant", "$tenant"))
                .append("count", new Document("$sum", 1))
                .append("total", new Document("$sum", "$duration"))
                .append("average", new Document("$avg", "$duration"))
                .append("minimum", new Document("$min", "$duration"))
                .append("maximum", new Document("$max", "$duration"))
                .append("first", new Document("$first", "$duration"))
                .append("last", new Document("$last", "$duration"))
                .append("all", new Document("$push", "$duration"))
                .append("unique", new Document("$addToSet", "$duration"))
                .append("labels", new Document("$mergeObjects", "$labels"));

        final Document result = AggregationPipeline.execute(source, List.of(new Document("$group", group))).get(0);

        assertEquals(new Document("event", "run").append("tenant", "crown"), result.get("_id"));
        assertEquals(3L, result.get("count"));
        assertEquals(40L, result.get("total"));
        assertEquals(20.0d, result.get("average"));
        assertEquals(10, result.get("minimum"));
        assertEquals(30.0d, result.get("maximum"));
        assertEquals(10, result.get("first"));
        assertNull(result.get("last"));
        assertEquals(Arrays.asList(10, 30.0d, null), result.get("all"));
        assertEquals(new Document("a", 3).append("b", 2), result.get("labels"));
    }

    @Test
    void dateTruncSupportsUtcCalendarAndFixedUnits() {
        final Date timestamp = Date.from(Instant.parse("2026-07-21T09:37:42.987Z"));
        final Document projection = new Document("hour", new Document("$dateTrunc", new Document("date", "$ts").append("unit", "hour")))
                .append("day", new Document("$dateTrunc", new Document("date", "$ts").append("unit", "day").append("timezone", "UTC")))
                .append("week", new Document("$dateTrunc", new Document("date", "$ts").append("unit", "week").append("startOfWeek", "mon")))
                .append("month", new Document("$dateTrunc", new Document("date", "$ts").append("unit", "month")));

        final Document result = AggregationPipeline.execute(
                        List.of(new Document("ts", timestamp)), List.of(new Document("$project", projection)))
                .get(0);

        assertEquals(Date.from(Instant.parse("2026-07-21T09:00:00Z")), result.getDate("hour"));
        assertEquals(Date.from(Instant.parse("2026-07-21T00:00:00Z")), result.getDate("day"));
        assertEquals(Date.from(Instant.parse("2026-07-20T00:00:00Z")), result.getDate("week"));
        assertEquals(Date.from(Instant.parse("2026-07-01T00:00:00Z")), result.getDate("month"));
    }

    @Test
    void groupSupportsPercentileMedianAndNAccumulators() {
        final List<Document> source = List.of(
                new Document("name", "A").append("score", 10),
                new Document("name", "B").append("score", 30),
                new Document("name", "C").append("score", 20),
                new Document("name", "D").append("score", 40));
        final Document group = new Document("_id", null)
                .append("p", new Document("$percentile", new Document("input", "$score").append("p", List.of(0.5d, 0.95d)).append("method", "approximate")))
                .append("med", new Document("$median", new Document("input", "$score").append("method", "approximate")))
                .append("first", new Document("$firstN", new Document("input", "$name").append("n", 2)))
                .append("last", new Document("$lastN", new Document("input", "$name").append("n", 2)))
                .append("min", new Document("$minN", new Document("input", "$score").append("n", 2)))
                .append("max", new Document("$maxN", new Document("input", "$score").append("n", 2)))
                .append("top", new Document("$topN", new Document("output", "$name").append("sortBy", new Document("score", -1)).append("n", 2)))
                .append("bottom", new Document("$bottomN", new Document("output", "$name").append("sortBy", new Document("score", -1)).append("n", 2)));

        final Document result = AggregationPipeline.execute(source, List.of(new Document("$group", group))).get(0);

        assertEquals(List.of(25.0d, 38.5d), result.get("p"));
        assertEquals(25.0d, result.get("med"));
        assertEquals(List.of("A", "B"), result.get("first"));
        assertEquals(List.of("C", "D"), result.get("last"));
        assertEquals(List.of(10, 20), result.get("min"));
        assertEquals(List.of(40, 30), result.get("max"));
        assertEquals(List.of("D", "B"), result.get("top"));
        assertEquals(List.of("C", "A"), result.get("bottom"));
    }

    @Test
    void setWindowFieldsSupportsShiftDocumentNumberAndRanks() {
        final List<Document> source = List.of(
                new Document("account", "a").append("at", 20).append("event", "second"),
                new Document("account", "a").append("at", 10).append("event", "first"),
                new Document("account", "b").append("at", 5).append("event", "only"));
        final Document outputs = new Document("previous", new Document("$shift", new Document("output", "$event").append("by", -1).append("default", "none")))
                .append("number", new Document("$documentNumber", new Document()))
                .append("rank", new Document("$rank", new Document()))
                .append("denseRank", new Document("$denseRank", new Document()));
        final Document stage = new Document("partitionBy", "$account")
                .append("sortBy", new Document("at", 1))
                .append("output", outputs);

        final List<Document> result = AggregationPipeline.execute(
                source, List.of(new Document("$setWindowFields", stage)));

        assertEquals("first", result.get(0).getString("event"));
        assertEquals("none", result.get(0).getString("previous"));
        assertEquals(1L, result.get(0).get("number"));
        assertEquals("second", result.get(1).getString("event"));
        assertEquals("first", result.get(1).getString("previous"));
        assertEquals(2L, result.get(1).get("rank"));
        assertEquals("only", result.get(2).getString("event"));
        assertEquals(1L, result.get(2).get("denseRank"));
    }
}
