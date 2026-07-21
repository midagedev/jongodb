package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Date;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class UpdateApplierTest {
    @Test
    void applySupportsSetIncAndUnsetOperators() {
        Document target =
                new Document("_id", 1)
                        .append("stats", new Document("views", 3))
                        .append("status", "stale");

        Document update =
                new Document("$set", new Document("profile.city", "Seoul"))
                        .append("$inc", new Document("stats.views", 2))
                        .append("$unset", new Document("status", true));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        UpdateApplier.validateApplicable(target, parsed);

        assertTrue(UpdateApplier.apply(target, parsed));
        assertEquals("Seoul", target.get("profile", Document.class).getString("city"));
        assertEquals(5, target.get("stats", Document.class).getInteger("views"));
        assertFalse(target.containsKey("status"));
    }

    @Test
    void applyReturnsFalseWhenUpdateDoesNotChangeDocument() {
        Document target = new Document("count", 5).append("offset", 2);
        Document update =
                new Document("$set", new Document("count", 5))
                        .append("$inc", new Document("offset", 0))
                        .append("$unset", new Document("missing", true));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        UpdateApplier.validateApplicable(target, parsed);

        assertFalse(UpdateApplier.apply(target, parsed));
        assertEquals(5, target.getInteger("count"));
    }

    @Test
    void applySupportsMinMaxForMissingMixedNumbersDatesAndDottedPaths() {
        final Date older = new Date(1_000L);
        final Date newer = new Date(2_000L);
        final Document target = new Document("duration", 3_000)
                .append("lastEventAt", older)
                .append("stats", new Document("minimum", 8.0d));
        final Document update = new Document("$inc", new Document("count", 3L))
                .append("$max", new Document("duration", 4_000.0d).append("lastEventAt", newer))
                .append("$min", new Document("stats.minimum", 5).append("firstSeenAt", older));

        final UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        UpdateApplier.validateApplicable(target, parsed);

        assertTrue(UpdateApplier.apply(target, parsed));
        assertEquals(4_000.0d, target.getDouble("duration"));
        assertEquals(newer, target.getDate("lastEventAt"));
        assertEquals(5, target.get("stats", Document.class).getInteger("minimum"));
        assertEquals(older, target.getDate("firstSeenAt"));
        assertEquals(3L, target.getLong("count"));

        final Document noOp = new Document("$max", new Document("duration", 2_000.0d).append("lastEventAt", older))
                .append("$min", new Document("stats.minimum", 9.0d));
        assertFalse(UpdateApplier.apply(target, UpdateApplier.parse(noOp)));
    }

    @Test
    void applyForUpsertInsertCreatesMinAndMaxFields() {
        final Document target = new Document("_id", "rollup");
        final Document update = new Document("$max", new Document("durationMaxMs", 4_000.0d))
                .append("$min", new Document("durationMinMs", 40.0d));

        assertTrue(UpdateApplier.applyForUpsertInsert(target, UpdateApplier.parse(update)));
        assertEquals(4_000.0d, target.getDouble("durationMaxMs"));
        assertEquals(40.0d, target.getDouble("durationMinMs"));
    }

    @Test
    void parseRejectsConflictingUpdatePathsAcrossOperators() {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$set", new Document("stats", 1))
                        .append("$max", new Document("stats.maximum", 2))));

        assertTrue(error.getMessage().contains("would create a conflict"));
    }

    @Test
    void validateApplicableRejectsNonNumericIncTarget() {
        Document target = new Document("count", "oops");
        Document update = new Document("$inc", new Document("count", 1));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> UpdateApplier.validateApplicable(target, parsed));
        assertTrue(error.getMessage().contains("$inc target"));
    }

    @Test
    void parseRejectsUnsupportedOperators() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> UpdateApplier.parse(new Document("$push", new Document("a", 1))));
        assertTrue(error.getMessage().contains("unsupported update operator"));
    }

    @Test
    void validateApplicableRejectsWritesThroughScalarPathSegment() {
        Document target = new Document("profile", "string");
        Document update = new Document("$set", new Document("profile.city", "Seoul"));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> UpdateApplier.validateApplicable(target, parsed));
        assertTrue(error.getMessage().contains("non-document segment"));
    }

    @Test
    void replacementUpdateReplacesDocumentAndPreservesIdWhenMissingInReplacement() {
        Document target =
                new Document("_id", 1)
                        .append("name", "old")
                        .append("profile", new Document("city", "Seoul"));
        Document replacement = new Document("name", "new");

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(replacement);
        UpdateApplier.validateApplicable(target, parsed);

        assertTrue(UpdateApplier.apply(target, parsed));
        assertEquals(1, target.getInteger("_id"));
        assertEquals("new", target.getString("name"));
        assertFalse(target.containsKey("profile"));
    }

    @Test
    void replacementUpdateRejectsChangingId() {
        Document target = new Document("_id", 1).append("name", "old");
        Document replacement = new Document("_id", 2).append("name", "new");

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(replacement);

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> UpdateApplier.validateApplicable(target, parsed));
        assertTrue(error.getMessage().contains("immutable field '_id'"));
    }

    @Test
    void parseRejectsPositionalPathUpdates() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> UpdateApplier.parse(new Document("$set", new Document("items.$.qty", 1))));
        assertTrue(error.getMessage().contains("positional and array filter updates are not supported"));
    }

    @Test
    void applySupportsArrayFilterSetAndUnsetSubset() {
        Document target =
                new Document("_id", 1)
                        .append(
                                "items",
                                List.of(
                                        new Document("sku", "A").append("qty", 1).append("legacy", true),
                                        new Document("sku", "B").append("qty", 4).append("legacy", true)));
        Document update =
                new Document("$set", new Document("items.$[hot].qty", 9))
                        .append("$unset", new Document("items.$[hot].legacy", true));
        List<Document> arrayFilters = List.of(new Document("hot.qty", new Document("$gte", 3)));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update, arrayFilters);
        UpdateApplier.validateApplicable(target, parsed);

        assertTrue(UpdateApplier.apply(target, parsed));
        assertEquals(1, target.getList("items", Document.class).get(0).getInteger("qty"));
        assertEquals(9, target.getList("items", Document.class).get(1).getInteger("qty"));
        assertTrue(!target.getList("items", Document.class).get(1).containsKey("legacy"));
    }

    @Test
    void parseRejectsArrayFilterPathWithoutBindings() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$set", new Document("items.$[e].qty", 1)), List.of()));
        assertTrue(error.getMessage().contains("arrayFilters must be specified"));
    }

    @Test
    void parseRejectsArrayFilterPathOnUnsupportedOperator() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> UpdateApplier.parse(
                        new Document("$inc", new Document("items.$[e].qty", 1)),
                        List.of(new Document("e.qty", new Document("$gte", 2)))));
        assertTrue(error.getMessage().contains("arrayFilters currently support only $set/$unset"));
    }

    @Test
    void applyIgnoresSetOnInsertForMatchedDocuments() {
        Document target = new Document("_id", 1).append("name", "before");
        Document update =
                new Document("$set", new Document("name", "after"))
                        .append("$setOnInsert", new Document("createdAt", "insert-only"));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        UpdateApplier.validateApplicable(target, parsed);

        assertTrue(UpdateApplier.apply(target, parsed));
        assertEquals("after", target.getString("name"));
        assertFalse(target.containsKey("createdAt"));
    }

    @Test
    void applyForUpsertInsertAppliesSetOnInsert() {
        Document target = new Document("email", "ada@example.com");
        Document update =
                new Document("$set", new Document("name", "Ada"))
                        .append("$setOnInsert", new Document("createdAt", "inserted"));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        UpdateApplier.validateApplicableForUpsertInsert(target, parsed);

        assertTrue(UpdateApplier.applyForUpsertInsert(target, parsed));
        assertEquals("Ada", target.getString("name"));
        assertEquals("inserted", target.getString("createdAt"));
    }
}
