package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
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
}
