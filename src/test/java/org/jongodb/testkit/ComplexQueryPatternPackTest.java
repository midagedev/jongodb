package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ComplexQueryPatternPackTest {
    @Test
    void packProvidesCanonicalPatternCountAndUniqueStableIds() {
        final List<ComplexQueryPatternPack.PatternCase> patterns = ComplexQueryPatternPack.patterns();

        assertTrue(patterns.size() >= 20, "expected at least 20 patterns");
        assertTrue(patterns.size() <= 30, "expected at most 30 patterns");

        final Set<String> ids = new HashSet<>();
        int explicitlyUnsupportedCount = 0;
        for (final ComplexQueryPatternPack.PatternCase pattern : patterns) {
            assertFalse(pattern.id().isBlank());
            assertTrue(ids.add(pattern.id()), "duplicate pattern id: " + pattern.id());
            if (pattern.supportClass() == ComplexQueryPatternPack.SupportClass.EXPLICITLY_UNSUPPORTED) {
                explicitlyUnsupportedCount++;
                assertEquals(
                        ComplexQueryPatternPack.ExpectedOutcome.UNSUPPORTED_POLICY,
                        pattern.expectedOutcome(),
                        "explicitly unsupported pattern should map to unsupported-policy expectation");
            }
        }

        assertTrue(explicitlyUnsupportedCount >= 3, "expected explicit unsupported coverage in pattern pack");
    }

    @Test
    void exprArrayIndexComparisonPatternUsesMongodCompatibleExprPathSemantics() {
        final ComplexQueryPatternPack.PatternCase pattern = ComplexQueryPatternPack.patterns().stream()
                .filter(candidate -> "cq.expr.array-index-comparison".equals(candidate.id()))
                .findFirst()
                .orElseThrow();

        final ScenarioOutcome outcome = new WireCommandIngressBackend("wire").execute(pattern.scenario());
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected successful scenario execution"));
        assertEquals(2, outcome.commandResults().size());

        final Map<String, Object> findResult = asMap(outcome.commandResults().get(1), "find result");
        final Map<String, Object> cursor = asMap(findResult.get("cursor"), "find cursor");
        final List<?> firstBatch = asList(cursor.get("firstBatch"), "find firstBatch");
        assertEquals(0, firstBatch.size(), "expected no matches for $expr path '$metrics.0' against array values");
    }

    private static Map<String, Object> asMap(final Object value, final String fieldName) {
        assertNotNull(value, fieldName + " must not be null");
        assertTrue(value instanceof Map<?, ?>, fieldName + " must be an object");
        final Map<?, ?> raw = (Map<?, ?>) value;
        final java.util.LinkedHashMap<String, Object> normalized = new java.util.LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : raw.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return normalized;
    }

    private static List<?> asList(final Object value, final String fieldName) {
        assertNotNull(value, fieldName + " must not be null");
        assertTrue(value instanceof List<?>, fieldName + " must be an array");
        return (List<?>) value;
    }
}
