package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.List;
import org.bson.BsonTimestamp;
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
                        () -> UpdateApplier.parse(new Document("$bogus", new Document("a", 1))));
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

    private static boolean run(Document target, Document update) {
        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        UpdateApplier.validateApplicable(target, parsed);
        return UpdateApplier.apply(target, parsed);
    }

    private static void assertRejected(Document target, Document update, String message) {
        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> UpdateApplier.validateApplicable(target, parsed));
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    @Test
    void pushAppendsSingleValueAndCreatesMissingArray() {
        Document target = new Document("tags", new java.util.ArrayList<>(List.of("a")));
        assertTrue(run(target, new Document("$push", new Document("tags", "b").append("nested.list", 1))));
        assertEquals(List.of("a", "b"), target.get("tags"));
        assertEquals(List.of(1), target.get("nested", Document.class).get("list"));
    }

    @Test
    void pushRejectsNonArrayTarget() {
        assertRejected(new Document("tags", "x"), new Document("$push", new Document("tags", 1)), "$push target");
    }

    @Test
    void pushEachWithPositionInsertsAtFrontMiddleAndFromEnd() {
        Document target = new Document("a", new java.util.ArrayList<>(List.of(1, 2, 3)));
        run(target, new Document("$push", new Document("a",
                new Document("$each", List.of(8, 9)).append("$position", 1))));
        assertEquals(List.of(1, 8, 9, 2, 3), target.get("a"));
        run(target, new Document("$push", new Document("a",
                new Document("$each", List.of(0)).append("$position", -1))));
        assertEquals(List.of(1, 8, 9, 2, 0, 3), target.get("a"));
        run(target, new Document("$push", new Document("a",
                new Document("$each", List.of(7)).append("$position", -100))));
        assertEquals(List.of(7, 1, 8, 9, 2, 0, 3), target.get("a"));
    }

    @Test
    void pushEachSortThenSliceFollowsMongoOrder() {
        Document target = new Document("scores", new java.util.ArrayList<>(List.of(5, 1)));
        run(target, new Document("$push", new Document("scores",
                new Document("$each", List.of(9, 3)).append("$sort", -1).append("$slice", 3))));
        assertEquals(List.of(9, 5, 3), target.get("scores"));

        run(target, new Document("$push", new Document("scores",
                new Document("$each", List.of(2)).append("$sort", 1).append("$slice", -2))));
        assertEquals(List.of(5, 9), target.get("scores"));

        run(target, new Document("$push", new Document("scores",
                new Document("$each", List.of()).append("$slice", 0))));
        assertEquals(List.of(), target.get("scores"));
    }

    @Test
    void pushEachSortsDocumentsByField() {
        Document target = new Document("q", new java.util.ArrayList<>(List.of(new Document("n", 2))));
        run(target, new Document("$push", new Document("q",
                new Document("$each", List.of(new Document("n", 3), new Document("n", 1)))
                        .append("$sort", new Document("n", 1)))));
        assertEquals(
                List.of(new Document("n", 1), new Document("n", 2), new Document("n", 3)), target.get("q"));
    }

    @Test
    void pushWithEmptyEachDoesNotModifyExistingArray() {
        Document target = new Document("a", new java.util.ArrayList<>(List.of(1)));
        assertFalse(run(target, new Document("$push", new Document("a", new Document("$each", List.of())))));
    }

    @Test
    void pushRejectsModifiersWithoutEach() {
        assertThrows(IllegalArgumentException.class, () -> UpdateApplier.parse(
                new Document("$push", new Document("a", new Document("$slice", 1)))));
    }

    @Test
    void pullRemovesEqualValuesOperatorMatchesAndDocumentConditions() {
        Document target = new Document("n", new java.util.ArrayList<>(List.of(1, 2, 2.0d, 7, 9)))
                .append("docs", new java.util.ArrayList<>(List.of(
                        new Document("score", 8).append("item", "A"),
                        new Document("score", 8).append("item", "B"),
                        new Document("score", 1).append("item", "B"))));
        assertTrue(run(target, new Document("$pull", new Document("n", 2))));
        assertEquals(List.of(1, 7, 9), target.get("n"));
        assertTrue(run(target, new Document("$pull", new Document("n", new Document("$gte", 7)))));
        assertEquals(List.of(1), target.get("n"));
        assertTrue(run(target, new Document("$pull", new Document("docs", new Document("score", 8).append("item", "B")))));
        assertEquals(2, target.getList("docs", Document.class).size());
        assertTrue(run(target, new Document("$pull", new Document("n", new Document("$in", List.of(1, 5))))));
        assertEquals(List.of(), target.get("n"));
    }

    @Test
    void pullIsNoOpForMissingFieldAndRejectsNonArray() {
        assertFalse(run(new Document(), new Document("$pull", new Document("a", 1))));
        assertRejected(new Document("a", 1), new Document("$pull", new Document("a", 1)), "$pull target");
    }

    @Test
    void pullAllRemovesEveryListedValue() {
        Document target = new Document("a", new java.util.ArrayList<>(List.of(1, 2, 3, 1, 4)));
        assertTrue(run(target, new Document("$pullAll", new Document("a", List.of(1, 4)))));
        assertEquals(List.of(2, 3), target.get("a"));
        assertFalse(run(target, new Document("$pullAll", new Document("a", List.of(9)))));
        assertFalse(run(new Document(), new Document("$pullAll", new Document("a", List.of(1)))));
        assertThrows(IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$pullAll", new Document("a", 1))));
        assertRejected(new Document("a", "x"), new Document("$pullAll", new Document("a", List.of(1))), "$pullAll target");
    }

    @Test
    void popRemovesFirstOrLastAndRejectsOtherArguments() {
        Document target = new Document("a", new java.util.ArrayList<>(List.of(1, 2, 3)));
        assertTrue(run(target, new Document("$pop", new Document("a", 1))));
        assertEquals(List.of(1, 2), target.get("a"));
        assertTrue(run(target, new Document("$pop", new Document("a", -1))));
        assertEquals(List.of(2), target.get("a"));
        assertFalse(run(new Document("a", new java.util.ArrayList<>()), new Document("$pop", new Document("a", 1))));
        assertFalse(run(new Document(), new Document("$pop", new Document("a", 1))));
        assertThrows(IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$pop", new Document("a", 2))));
        assertRejected(new Document("a", 1), new Document("$pop", new Document("a", 1)), "$pop target");
    }

    @Test
    void mulMultipliesPromotesAndCreatesZeroOfSameType() {
        Document target = new Document("i", 3).append("big", Integer.MAX_VALUE).append("d", 1.5d);
        assertTrue(run(target, new Document("$mul",
                new Document("i", 4).append("big", 2).append("d", 2).append("missingInt", 5)
                        .append("missingLong", 5L).append("missingDouble", 2.5d))));
        assertEquals(12, target.get("i"));
        assertEquals(Integer.MAX_VALUE * 2L, target.get("big"));
        assertEquals(3.0d, target.get("d"));
        assertEquals(0, target.get("missingInt"));
        assertEquals(0L, target.get("missingLong"));
        assertEquals(0.0d, target.get("missingDouble"));
        assertFalse(run(target, new Document("$mul", new Document("i", 1))));
        assertRejected(new Document("a", "x"), new Document("$mul", new Document("a", 2)), "$mul target");
        assertThrows(IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$mul", new Document("a", "2"))));
    }

    @Test
    void renameMovesFieldsIncludingDottedPaths() {
        Document target = new Document("old", 1).append("a", new Document("b", 2));
        assertTrue(run(target, new Document("$rename", new Document("old", "fresh").append("a.b", "c.d"))));
        assertFalse(target.containsKey("old"));
        assertEquals(1, target.get("fresh"));
        assertEquals(2, target.get("c", Document.class).get("d"));
        assertFalse(target.get("a", Document.class).containsKey("b"));
        assertFalse(run(target, new Document("$rename", new Document("missing", "x"))));
        assertThrows(IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$rename", new Document("a", "a.b"))));
        assertThrows(IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$rename", new Document("a.b", "a"))));
        assertThrows(IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$rename", new Document("a", 1))));
    }

    @Test
    void currentDateSetsDateOrTimestamp() {
        Document target = new Document();
        assertTrue(run(target, new Document("$currentDate", new Document("d", true)
                .append("d2", new Document("$type", "date"))
                .append("t", new Document("$type", "timestamp")))));
        assertTrue(target.get("d") instanceof Date);
        assertTrue(target.get("d2") instanceof Date);
        assertTrue(target.get("t") instanceof BsonTimestamp);
        assertThrows(IllegalArgumentException.class,
                () -> UpdateApplier.parse(new Document("$currentDate", new Document("a", new Document("$type", "x")))));
    }

    @Test
    void newOperatorsRejectArrayFilterPaths() {
        assertThrows(IllegalArgumentException.class, () -> UpdateApplier.parse(
                new Document("$push", new Document("a.$[x].b", 1)),
                List.of(new Document("x.k", 1))));
    }
}
