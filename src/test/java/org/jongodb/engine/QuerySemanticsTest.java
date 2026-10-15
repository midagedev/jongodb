package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

class QuerySemanticsTest {
    @Test
    void crossTypeNumericEqualityMatchesForEqInNinNeElemMatchAndArrays() {
        final Document document = new Document("l", 1L)
                .append("d", 1.0d)
                .append("dec", Decimal128.parse("1"))
                .append("arr", List.of(1L, 2L))
                .append("nested", new Document("n", 1L))
                .append("items", List.of(new Document("n", 1L), new Document("n", 2)))
                .append("label", "1");

        assertTrue(QueryMatcher.matches(document, new Document("l", 1)));
        assertTrue(QueryMatcher.matches(document, new Document("d", 1)));
        assertTrue(QueryMatcher.matches(document, new Document("dec", 1)));
        assertFalse(QueryMatcher.matches(document, new Document("l", 2)));
        assertFalse(QueryMatcher.matches(document, new Document("l", "1")));

        assertTrue(QueryMatcher.matches(document, new Document("l", new Document("$eq", 1))));
        assertTrue(QueryMatcher.matches(document, new Document("l", new Document("$in", List.of(2, 1)))));
        assertFalse(QueryMatcher.matches(document, new Document("l", new Document("$nin", List.of(1, 3)))));
        assertTrue(QueryMatcher.matches(document, new Document("l", new Document("$ne", 2))));
        assertFalse(QueryMatcher.matches(document, new Document("l", new Document("$ne", 1))));
        assertTrue(QueryMatcher.matches(document, new Document("l", new Document("$gt", 0))));

        assertTrue(QueryMatcher.matches(document, new Document("arr", 1)));
        assertTrue(QueryMatcher.matches(document, new Document("arr", List.of(1, 2))));
        assertFalse(QueryMatcher.matches(document, new Document("arr", List.of(2, 1))));
        assertTrue(QueryMatcher.matches(document, new Document("nested", new Document("n", 1))));
        assertTrue(QueryMatcher.matches(
                document, new Document("items", new Document("$elemMatch", new Document("n", 1)))));
        assertFalse(QueryMatcher.matches(
                document, new Document("items", new Document("$elemMatch", new Document("n", 3)))));
    }

    @Test
    void ninContainingNullExcludesMissingFields() {
        final Document present = new Document("a", 1);
        final Document explicitNull = new Document("a", null);
        final Document arrayWithNull = new Document("a", Arrays.asList(null, "x"));
        final List<Object> onlyNull = Arrays.asList((Object) null);
        final List<Object> nullAndOther = Arrays.asList(null, "x");

        assertFalse(QueryMatcher.matches(present, new Document("missing", new Document("$nin", onlyNull))));
        assertFalse(QueryMatcher.matches(present, new Document("missing", new Document("$nin", nullAndOther))));
        assertTrue(QueryMatcher.matches(present, new Document("missing", new Document("$nin", List.of("x")))));
        assertFalse(QueryMatcher.matches(explicitNull, new Document("a", new Document("$nin", onlyNull))));
        assertTrue(QueryMatcher.matches(present, new Document("a", new Document("$nin", onlyNull))));
        assertFalse(QueryMatcher.matches(arrayWithNull, new Document("a", new Document("$nin", onlyNull))));
        assertTrue(QueryMatcher.matches(present, new Document("missing", new Document("$in", onlyNull))));
    }

    @Test
    void elemMatchWithAndOrNorMatchesOnlyWhenOneElementSatisfiesTheWholeCondition() {
        final Document split = new Document(
                "arr",
                List.of(new Document("a", 1).append("b", 0), new Document("a", 0).append("b", 2)));
        final Document together = new Document("arr", List.of(new Document("a", 1).append("b", 2)));
        final Document neither = new Document("arr", List.of(new Document("a", 0).append("b", 0)));
        final Document orHit = new Document(
                "arr", List.of(new Document("a", 0).append("b", 0), new Document("a", 1).append("b", 9)));
        final Document scores = new Document("scores", List.of(1, 5, 9));

        final Document or = new Document(
                "$elemMatch", new Document("$or", List.of(new Document("a", 1), new Document("b", 2))));
        final Document and = new Document(
                "$elemMatch", new Document("$and", List.of(new Document("a", 1), new Document("b", 2))));
        final Document nor = new Document(
                "$elemMatch", new Document("$nor", List.of(new Document("a", 1), new Document("b", 2))));

        assertTrue(QueryMatcher.matches(orHit, new Document("arr", or)));
        assertFalse(QueryMatcher.matches(neither, new Document("arr", or)));
        assertFalse(QueryMatcher.matches(split, new Document("arr", and)));
        assertTrue(QueryMatcher.matches(together, new Document("arr", and)));
        assertTrue(QueryMatcher.matches(neither, new Document("arr", nor)));
        assertFalse(QueryMatcher.matches(together, new Document("arr", nor)));
        assertFalse(QueryMatcher.matches(split, new Document("arr", nor)));
        assertTrue(QueryMatcher.matches(
                new Document("arr", List.of(new Document("a", 1).append("b", 2), new Document("a", 0).append("b", 0))),
                new Document("arr", nor)));

        assertThrows(
                IllegalArgumentException.class,
                () -> QueryMatcher.matches(
                        scores,
                        new Document(
                                "scores",
                                new Document(
                                        "$elemMatch",
                                        new Document("$and", List.of(new Document("$gte", 5), new Document("$lt", 9)))))));
        assertTrue(QueryMatcher.matches(
                scores, new Document("scores", new Document("$elemMatch", new Document("$gte", 5).append("$lt", 9)))));
    }

    @Test
    void jsonSchemaEvaluatesRequiredPropertiesBsonTypeEnumBoundsAndAdditionalProperties() {
        final Document schema = new Document(
                "$jsonSchema",
                new Document("bsonType", "object")
                        .append("required", List.of("name", "n"))
                        .append(
                                "properties",
                                new Document("name", new Document("bsonType", "string"))
                                        .append(
                                                "n",
                                                new Document("bsonType", List.of("int", "long"))
                                                        .append("minimum", 1)
                                                        .append("maximum", 10)
                                                        .append("enum", List.of(1, 2, 3)))
                                        .append(
                                                "profile",
                                                new Document("bsonType", "object")
                                                        .append("required", List.of("city"))
                                                        .append(
                                                                "properties",
                                                                new Document("city", new Document("bsonType", "string")))
                                                        .append("additionalProperties", false)))
                        .append("additionalProperties", false));

        assertTrue(QueryMatcher.matches(
                new Document("name", "ada").append("n", 2).append("profile", new Document("city", "seoul")),
                schema));
        assertTrue(QueryMatcher.matches(
                new Document("n", 1L),
                new Document(
                        "$jsonSchema",
                        new Document(
                                "properties",
                                new Document("n", new Document("bsonType", List.of("int", "long")))))));

        final Document valid = new Document("name", "ada").append("n", 2);
        assertTrue(QueryMatcher.matches(valid, schema));
        assertFalse(QueryMatcher.matches(new Document("name", "ada"), schema));
        assertFalse(QueryMatcher.matches(new Document("name", 1).append("n", 2), schema));
        assertFalse(QueryMatcher.matches(new Document("name", "ada").append("n", 4), schema));
        assertFalse(QueryMatcher.matches(new Document("name", "ada").append("n", 0), schema));
        assertFalse(QueryMatcher.matches(new Document("name", "ada").append("n", 11), schema));
        assertTrue(QueryMatcher.matches(new Document("name", "ada").append("n", 2L), schema));
        assertTrue(QueryMatcher.matches(
                new Document("n", new Document("v", 1L)),
                new Document(
                        "$jsonSchema",
                        new Document(
                                "properties",
                                new Document("n", new Document("enum", List.of(new Document("v", 1))))))));
        assertFalse(QueryMatcher.matches(
                new Document("n", "2"),
                new Document("$jsonSchema", new Document("properties", new Document("n", new Document("enum", List.of(2)))))));
        assertFalse(QueryMatcher.matches(new Document("name", "ada").append("n", 2).append("extra", true), schema));
        assertFalse(QueryMatcher.matches(
                new Document("name", "ada").append("n", 2).append("profile", new Document("city", "seoul").append("zip", "1")),
                schema));
        assertFalse(QueryMatcher.matches(
                new Document("name", "ada").append("n", 2).append("profile", new Document("zip", "1")), schema));
        assertTrue(QueryMatcher.matches(new Document("name", "ada").append("n", "2"), new Document("$jsonSchema", new Document("minimum", 1))));
        assertFalse(QueryMatcher.matches(new Document(), new Document("$jsonSchema", new Document("bsonType", "string"))));
        assertThrows(
                IllegalArgumentException.class,
                () -> QueryMatcher.matches(new Document(), new Document("$jsonSchema", new Document("bsonType", "nope"))));
    }
}
