package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

class QueryMatcherTest {
    @Test
    void matchesSimpleEqualityByKey() {
        Document document = new Document("_id", 1).append("role", "user").append("active", true);

        assertTrue(QueryMatcher.matches(document, new Document("role", "user")));
        assertFalse(QueryMatcher.matches(document, new Document("role", "admin")));
    }

    @Test
    void matchesDotPathLookup() {
        Document document =
                new Document("_id", 1)
                        .append(
                                "profile",
                                new Document("address", new Document("city", "Seoul")));

        assertTrue(QueryMatcher.matches(document, new Document("profile.address.city", "Seoul")));
        assertFalse(QueryMatcher.matches(document, new Document("profile.address.city", "Busan")));
    }

    @Test
    void matchesArrayContainmentForScalarEquality() {
        Document document = new Document("_id", 1).append("tags", Arrays.asList("java", "db"));

        assertTrue(QueryMatcher.matches(document, new Document("tags", "db")));
        assertFalse(QueryMatcher.matches(document, new Document("tags", "infra")));
    }

    @Test
    void doesNotUseContainmentForNonScalarFilterValues() {
        Document document = new Document("_id", 1).append("tags", Arrays.asList("java", "db"));

        assertTrue(QueryMatcher.matches(document, new Document("tags", Arrays.asList("java", "db"))));
        assertFalse(QueryMatcher.matches(document, new Document("tags", Arrays.asList("db"))));
    }

    @Test
    void supportsEqAndNeOperators() {
        Document document =
                new Document("score", 10)
                        .append("tags", Arrays.asList("java", "db"))
                        .append("nullable", null);

        assertTrue(QueryMatcher.matches(document, new Document("score", new Document("$eq", 10))));
        assertFalse(QueryMatcher.matches(document, new Document("score", new Document("$eq", 11))));
        assertTrue(QueryMatcher.matches(document, new Document("score", new Document("$ne", 11))));
        assertFalse(QueryMatcher.matches(document, new Document("score", new Document("$ne", 10))));
        assertTrue(QueryMatcher.matches(document, new Document("tags", new Document("$eq", "db"))));
        assertTrue(QueryMatcher.matches(document, new Document("missing", new Document("$ne", 10))));
    }

    @Test
    void supportsComparisonOperators() {
        Document document = new Document("score", 7).append("samples", Arrays.asList(2, 5, 8));

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document("score", new Document("$gt", 5).append("$lte", 7))));
        assertTrue(QueryMatcher.matches(document, new Document("samples", new Document("$gte", 8))));
        assertFalse(QueryMatcher.matches(document, new Document("samples", new Document("$lt", 2))));
        assertFalse(QueryMatcher.matches(document, new Document("score", new Document("$gt", 10))));
    }

    @Test
    void supportsInAndNinOperators() {
        Document document = new Document("role", "user").append("tags", Arrays.asList("java", "db"));

        assertTrue(QueryMatcher.matches(document, new Document("role", new Document("$in", List.of("admin", "user")))));
        assertTrue(QueryMatcher.matches(document, new Document("tags", new Document("$in", List.of("ops", "db")))));
        assertFalse(QueryMatcher.matches(document, new Document("role", new Document("$in", List.of("admin")))));

        assertTrue(QueryMatcher.matches(document, new Document("role", new Document("$nin", List.of("admin")))));
        assertFalse(QueryMatcher.matches(document, new Document("tags", new Document("$nin", List.of("db", "ops")))));
        assertTrue(QueryMatcher.matches(document, new Document("missing", new Document("$nin", List.of("x")))));
        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "missing",
                                new Document("$in", Arrays.asList((Object) null)))));
    }

    @Test
    void supportsLogicalAndOrNotNorOperators() {
        Document document = new Document("role", "user").append("active", true).append("score", 7);

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "$and",
                                List.of(new Document("role", "user"), new Document("active", true)))));
        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "$or",
                                List.of(new Document("role", "admin"), new Document("score", 7)))));
        assertTrue(QueryMatcher.matches(document, new Document("$not", new Document("role", "admin"))));
        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "$nor",
                                List.of(
                                        new Document("role", "admin"),
                                        new Document("score", new Document("$lt", 0))))));

        assertFalse(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "$and",
                                List.of(new Document("role", "user"), new Document("active", false)))));
        assertFalse(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "$or",
                                List.of(new Document("role", "admin"), new Document("score", 100)))));
        assertFalse(QueryMatcher.matches(document, new Document("$not", new Document("role", "user"))));
        assertFalse(QueryMatcher.matches(document, new Document("$nor", List.of(new Document("score", 7)))));
    }

    @Test
    void supportsFieldLevelNot() {
        Document document = new Document("score", 7).append("name", "Alice");

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document("score", new Document("$not", new Document("$gt", 10)))));
        assertFalse(
                QueryMatcher.matches(
                        document,
                        new Document("score", new Document("$not", new Document("$gte", 7)))));
        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document("name", new Document("$not", Pattern.compile("^B")))));
    }

    @Test
    void supportsExistsOperator() {
        Document document = new Document("nullable", null).append("value", 1);

        assertTrue(QueryMatcher.matches(document, new Document("nullable", new Document("$exists", true))));
        assertTrue(QueryMatcher.matches(document, new Document("missing", new Document("$exists", false))));
        assertFalse(QueryMatcher.matches(document, new Document("missing", new Document("$exists", true))));
        assertTrue(QueryMatcher.matches(document, new Document("missing", null)));
    }

    @Test
    void supportsTypeOperator() {
        Document document =
                new Document("doubleValue", 1.25d)
                        .append("stringValue", "hello")
                        .append("objectValue", new Document("k", 1))
                        .append("arrayValue", Arrays.asList(1, "x"))
                        .append("boolValue", true)
                        .append("dateValue", new Date(0L))
                        .append("nullValue", null)
                        .append("regexValue", Pattern.compile("abc"))
                        .append("intValue", 3)
                        .append("longValue", 3L)
                        .append("decimalValue", Decimal128.parse("3.14"))
                        .append("objectIdValue", new ObjectId("507f1f77bcf86cd799439011"));

        assertTrue(QueryMatcher.matches(document, new Document("doubleValue", new Document("$type", "double"))));
        assertTrue(QueryMatcher.matches(document, new Document("stringValue", new Document("$type", "string"))));
        assertTrue(QueryMatcher.matches(document, new Document("objectValue", new Document("$type", "object"))));
        assertTrue(QueryMatcher.matches(document, new Document("arrayValue", new Document("$type", "array"))));
        assertTrue(QueryMatcher.matches(document, new Document("arrayValue", new Document("$type", "string"))));
        assertTrue(QueryMatcher.matches(document, new Document("boolValue", new Document("$type", "bool"))));
        assertTrue(QueryMatcher.matches(document, new Document("dateValue", new Document("$type", "date"))));
        assertTrue(QueryMatcher.matches(document, new Document("nullValue", new Document("$type", "null"))));
        assertTrue(QueryMatcher.matches(document, new Document("regexValue", new Document("$type", "regex"))));
        assertTrue(QueryMatcher.matches(document, new Document("intValue", new Document("$type", "int"))));
        assertTrue(QueryMatcher.matches(document, new Document("longValue", new Document("$type", 18))));
        assertTrue(QueryMatcher.matches(document, new Document("decimalValue", new Document("$type", 19))));
        assertTrue(QueryMatcher.matches(document, new Document("objectIdValue", new Document("$type", "objectId"))));
        assertFalse(QueryMatcher.matches(document, new Document("missing", new Document("$type", "string"))));
    }

    @Test
    void supportsSizeOperator() {
        Document document = new Document("tags", Arrays.asList("java", "db"));

        assertTrue(QueryMatcher.matches(document, new Document("tags", new Document("$size", 2))));
        assertFalse(QueryMatcher.matches(document, new Document("tags", new Document("$size", 1))));
        assertFalse(QueryMatcher.matches(document, new Document("missing", new Document("$size", 0))));
    }

    @Test
    void supportsElemMatchForScalarArrays() {
        Document document = new Document("scores", Arrays.asList(2, 5, 9));

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "scores",
                                new Document("$elemMatch", new Document("$gte", 5).append("$lt", 9)))));
        assertFalse(
                QueryMatcher.matches(
                        document,
                        new Document("scores", new Document("$elemMatch", new Document("$gt", 10)))));
    }

    @Test
    void supportsElemMatchForArrayOfDocuments() {
        Document document =
                new Document(
                        "items",
                        List.of(
                                new Document("sku", "A").append("qty", 3),
                                new Document("sku", "B").append("qty", 1)));

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "items",
                                new Document(
                                        "$elemMatch",
                                        new Document("sku", "A")
                                                .append("qty", new Document("$gte", 3))))));
        assertFalse(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "items",
                                new Document(
                                        "$elemMatch",
                                        new Document("sku", "A")
                                                .append("qty", new Document("$lt", 3))))));
    }

    @Test
    void supportsAllOperator() {
        Document document =
                new Document("tags", Arrays.asList("java", "db", "engine"))
                        .append(
                                "items",
                                List.of(
                                        new Document("sku", "A").append("qty", 2),
                                        new Document("sku", "B").append("qty", 4)));

        assertTrue(QueryMatcher.matches(document, new Document("tags", new Document("$all", List.of("java", "db")))));
        assertFalse(QueryMatcher.matches(document, new Document("tags", new Document("$all", List.of("java", "ops")))));

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "items",
                                new Document(
                                        "$all",
                                        List.of(
                                                new Document(
                                                        "$elemMatch",
                                                        new Document("sku", "A")
                                                                .append(
                                                                        "qty",
                                                                        new Document("$gte", 2))))))));
    }

    @Test
    void supportsRegexOperatorAndLiteralPattern() {
        Document document = new Document("name", "Alice").append("tags", Arrays.asList("core-db", "ops"));

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document("name", new Document("$regex", "^ali").append("$options", "i"))));
        assertFalse(QueryMatcher.matches(document, new Document("name", new Document("$regex", "^ali"))));
        assertTrue(QueryMatcher.matches(document, new Document("name", Pattern.compile("lic"))));
        assertTrue(QueryMatcher.matches(document, new Document("tags", new Document("$regex", "db$"))));
    }

    @Test
    void supportsNestedPathMatchingAcrossArrayOfDocuments() {
        Document document =
                new Document(
                        "orders",
                        List.of(
                                new Document("id", 1)
                                        .append(
                                                "lines",
                                                List.of(
                                                        new Document("sku", "A").append("qty", 2),
                                                        new Document("sku", "B").append("qty", 1))),
                                new Document("id", 2)
                                        .append(
                                                "lines",
                                                List.of(
                                                        new Document("sku", "C")
                                                                .append("qty", 5)))));

        assertTrue(QueryMatcher.matches(document, new Document("orders.lines.sku", "C")));
        assertTrue(QueryMatcher.matches(document, new Document("orders.lines.qty", new Document("$gte", 5))));
        assertFalse(QueryMatcher.matches(document, new Document("orders.lines.qty", new Document("$lt", 0))));
    }

    @Test
    void supportsArrayIndexPathMatching() {
        final Document document =
                new Document(
                        "routes",
                        List.of(
                                new Document("region", "apac").append("weight", 5),
                                new Document("region", "emea").append("weight", 2)));

        assertTrue(QueryMatcher.matches(document, new Document("routes.0.region", "apac")));
        assertTrue(QueryMatcher.matches(document, new Document("routes.1.weight", new Document("$lt", 3))));
        assertFalse(QueryMatcher.matches(document, new Document("routes.2.region", "apac")));
    }

    @Test
    void supportsExprTopLevelOperator() {
        final Document document =
                new Document("price", 120)
                        .append("cost", 80)
                        .append("qty", 3)
                        .append("active", true);

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document("$expr", new Document("$gt", List.of("$price", "$cost")))));
        assertFalse(
                QueryMatcher.matches(
                        document,
                        new Document("$expr", new Document("$lt", List.of("$price", "$cost")))));
        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document(
                                "$expr",
                                new Document(
                                        "$and",
                                        List.of(
                                                new Document("$eq", List.of("$qty", 3)),
                                                new Document("$eq", List.of("$active", true)))))));
    }

    @Test
    void supportsExprPathResolutionWithArrayIndexes() {
        final Document document =
                new Document("metrics", List.of(5, 2, 1))
                        .append("series", List.of(new Document("value", 9), new Document("value", 4)));

        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document("$expr", new Document("$eq", List.of("$metrics.0", 5)))));
        assertTrue(
                QueryMatcher.matches(
                        document,
                        new Document("$expr", new Document("$gt", List.of("$series.0.value", "$series.1.value")))));
        assertFalse(
                QueryMatcher.matches(
                        document,
                        new Document("$expr", new Document("$eq", List.of("$metrics.3", 9)))));
    }

    @Test
    void supportsCollationSubsetForStringEqualityAndComparison() {
        final Document document = new Document("name", "Alpha").append("city", "ä");
        final Document uppercaseDocument = new Document("name", "ALPHA");
        final CollationSupport.Config collation =
                CollationSupport.Config.fromDocument(new Document("locale", "en").append("strength", 1));

        assertTrue(QueryMatcher.matches(document, new Document("name", "alpha"), collation));
        assertTrue(QueryMatcher.matches(document, new Document("name", new Document("$gte", "a")), collation));
        assertTrue(QueryMatcher.matches(uppercaseDocument, new Document("name", new Document("$gte", "a")), collation));
        assertTrue(QueryMatcher.matches(document, new Document("city", new Document("$lt", "z")), collation));
        assertFalse(QueryMatcher.matches(document, new Document("name", "beta"), collation));
    }

    @Test
    void rejectsUnsupportedOperatorsOrInvalidOperands() {
        Document document = new Document("score", 10);

        assertThrows(
                IllegalArgumentException.class,
                () -> QueryMatcher.matches(document, new Document("score", new Document("$foo", 1))));
        assertThrows(
                IllegalArgumentException.class,
                () -> QueryMatcher.matches(document, new Document("score", new Document("$in", 1))));
        assertThrows(
                IllegalArgumentException.class,
                () -> QueryMatcher.matches(document, new Document("$and", new Document("role", "user"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        QueryMatcher.matches(
                                document,
                                new Document("name", new Document("$regex", "a").append("$options", "q"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        QueryMatcher.matches(
                                document,
                                new Document("$expr", new Document("$eq", List.of("$score")))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        QueryMatcher.matches(
                                document,
                                new Document("$expr", new Document("$unknown", List.of(1, 1)))));
    }
}
