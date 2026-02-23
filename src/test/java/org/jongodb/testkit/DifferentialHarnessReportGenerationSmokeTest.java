package org.jongodb.testkit;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public final class DifferentialHarnessReportGenerationSmokeTest {
    public static void main(String[] args) {
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
        expect(report.totalScenarios() == 1, "expected one scenario");
        expect(report.mismatchCount() == 1, "expected one mismatch");

        DiffResult result = report.results().get(0);
        expect(result.status() == DiffStatus.MISMATCH, "expected mismatch status");
        expect(!result.entries().isEmpty(), "expected diff entries");

        DiffSummaryGenerator generator = new DiffSummaryGenerator();
        String markdown = generator.toMarkdown(report);
        String json = generator.toJson(report);

        expect(markdown.contains("find-mismatch"), "markdown missing scenario id");
        expect(markdown.contains("MISMATCH"), "markdown missing mismatch status");
        expect(markdown.contains("$.commandResults[0].count"), "markdown missing diff path");

        expect(json.contains("\"status\":\"MISMATCH\""), "json missing mismatch status");
        expect(json.contains("\"scenarioId\":\"find-mismatch\""), "json missing scenario id");
        expect(json.contains("\"path\":\"$.commandResults[0].count\""), "json missing diff path");
    }

    private static void expect(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
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
