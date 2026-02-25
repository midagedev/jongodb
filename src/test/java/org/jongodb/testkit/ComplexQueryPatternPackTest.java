package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
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
}
