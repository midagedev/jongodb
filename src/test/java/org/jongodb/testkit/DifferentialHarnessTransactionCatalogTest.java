package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class DifferentialHarnessTransactionCatalogTest {
    @Test
    void transactionCatalogMaintainsTargetPassRateAndGeneratesReports() {
        List<Scenario> scenarios = TransactionScenarioCatalog.scenarios();

        DifferentialBackend left = new WireCommandIngressBackend("wire-left");
        DifferentialBackend right = new WireCommandIngressBackend("wire-right");
        DifferentialHarness harness = new DifferentialHarness(
            left,
            right,
            Clock.fixed(Instant.parse("2026-02-23T10:30:00Z"), ZoneOffset.UTC)
        );

        DifferentialReport report = harness.run(scenarios);
        PassRate passRate = PassRate.from(report);
        assertEquals(scenarios.size(), report.totalScenarios());
        assertTrue(passRate.ratio() >= 0.90d, "pass rate below threshold: " + passRate.formatted());

        DiffSummaryGenerator summaryGenerator = new DiffSummaryGenerator();
        String markdown = summaryGenerator.toMarkdown(report);
        String json = summaryGenerator.toJson(report);

        assertTrue(markdown.contains("# Differential Report"));
        assertTrue(markdown.contains("wire-left vs wire-right"));
        assertTrue(markdown.contains("txn.start-commit-path"));
        assertTrue(markdown.contains("txn.start-abort-path"));
        assertTrue(markdown.contains("txn.no-such-transaction-path"));
        assertTrue(markdown.contains("txn.lifecycle-transition-path"));
        assertTrue(markdown.contains("- total: " + scenarios.size()));

        assertTrue(json.contains("\"leftBackend\":\"wire-left\""));
        assertTrue(json.contains("\"rightBackend\":\"wire-right\""));
        assertTrue(json.contains("\"scenarioId\":\"txn.start-commit-path\""));
        assertTrue(json.contains("\"scenarioId\":\"txn.start-abort-path\""));
        assertTrue(json.contains("\"scenarioId\":\"txn.no-such-transaction-path\""));
        assertTrue(json.contains("\"scenarioId\":\"txn.lifecycle-transition-path\""));
    }
}
