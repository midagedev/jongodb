package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
}
