package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.bson.Document;
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
}
