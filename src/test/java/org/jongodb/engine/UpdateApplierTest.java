package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        Document target = new Document("count", 5);
        Document update =
                new Document("$set", new Document("count", 5))
                        .append("$inc", new Document("count", 0))
                        .append("$unset", new Document("missing", true));

        UpdateApplier.ParsedUpdate parsed = UpdateApplier.parse(update);
        UpdateApplier.validateApplicable(target, parsed);

        assertFalse(UpdateApplier.apply(target, parsed));
        assertEquals(5, target.getInteger("count"));
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
}
