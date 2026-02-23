package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DifferentialHarnessReportGenerationSmokeTest {
    @Test
    void generatesMismatchReportWithStableDiffPathAndStatus() {
        Scenario scenario = new Scenario(
            "find-mismatch",
            "find should report count mismatch",
            List.of(new ScenarioCommand("find", Map.of("filter", Map.of("k", 1))))
        );

        DifferentialBackend left = new StaticBackend(
            "mongod",
            ScenarioOutcome.success(List.of(Map.of("ok", 1, "count", 2)))
        );
        DifferentialBackend right = new StaticBackend(
            "jongodb",
            ScenarioOutcome.success(List.of(Map.of("ok", 1, "count", 1)))
        );

        DifferentialHarness harness = new DifferentialHarness(
            left,
            right,
            Clock.fixed(Instant.parse("2026-02-23T10:10:00Z"), ZoneOffset.UTC)
        );

        DifferentialReport report = harness.run(List.of(scenario));
        assertEquals(1, report.totalScenarios());
        assertEquals(1, report.mismatchCount());

        DiffResult result = report.results().get(0);
        assertEquals(DiffStatus.MISMATCH, result.status());
        assertEquals("find-mismatch", result.scenarioId());
        assertEquals("mongod", result.leftBackend());
        assertEquals("jongodb", result.rightBackend());
        assertFalse(result.entries().isEmpty());

        DiffEntry entry = result.entries().get(0);
        assertEquals("$.commandResults[0].count", entry.path());
        assertEquals(2, entry.leftValue());
        assertEquals(1, entry.rightValue());
        assertEquals("value mismatch", entry.note());

        DiffSummaryGenerator generator = new DiffSummaryGenerator();
        String markdown = generator.toMarkdown(report);
        String json = generator.toJson(report);

        assertTrue(markdown.contains("find-mismatch"));
        assertTrue(markdown.contains("MISMATCH"));
        assertTrue(markdown.contains("$.commandResults[0].count"));

        assertTrue(json.contains("\"status\":\"MISMATCH\""));
        assertTrue(json.contains("\"scenarioId\":\"find-mismatch\""));
        assertTrue(json.contains("\"path\":\"$.commandResults[0].count\""));
    }

    private static final class StaticBackend implements DifferentialBackend {
        private final String name;
        private final ScenarioOutcome outcome;

        private StaticBackend(String name, ScenarioOutcome outcome) {
            this.name = name;
            this.outcome = outcome;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ScenarioOutcome execute(Scenario scenario) {
            return outcome;
        }
    }
}
