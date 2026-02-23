package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class DifferentialHarnessCrudCatalogTest {
    @Test
    void crudCatalogMaintainsTargetPassRateAndGeneratesReports() {
        List<Scenario> scenarios = CrudScenarioCatalog.scenarios();

        DifferentialBackend left = new WireCommandIngressBackend("wire-left");
        DifferentialBackend right = new WireCommandIngressBackend("wire-right");
        DifferentialHarness harness = new DifferentialHarness(
            left,
            right,
            Clock.fixed(Instant.parse("2026-02-23T10:20:00Z"), ZoneOffset.UTC)
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
        assertTrue(markdown.contains("crud.insert-find"));
        assertTrue(markdown.contains("crud.create-indexes-duplicate-key"));
        assertTrue(markdown.contains("- total: " + scenarios.size()));

        assertTrue(json.contains("\"leftBackend\":\"wire-left\""));
        assertTrue(json.contains("\"rightBackend\":\"wire-right\""));
        assertTrue(json.contains("\"scenarioId\":\"crud.insert-find\""));
        assertTrue(json.contains("\"scenarioId\":\"crud.create-indexes-duplicate-key\""));
    }
}
