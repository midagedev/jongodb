package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class M3GateAutomationTest {
    @Test
    void computesFlakeRateFromBaselineFingerprints() {
        DifferentialReport baseline = new DifferentialReport(
            Instant.parse("2026-02-23T12:00:00Z"),
            "wire-left",
            "wire-right",
            List.of(DiffResult.match("scenario-1", "wire-left", "wire-right"))
        );

        DifferentialReport rerunStable = new DifferentialReport(
            Instant.parse("2026-02-23T12:01:00Z"),
            "wire-left",
            "wire-right",
            List.of(DiffResult.match("scenario-1", "wire-left", "wire-right"))
        );

        DifferentialReport rerunFlaky = new DifferentialReport(
            Instant.parse("2026-02-23T12:02:00Z"),
            "wire-left",
            "wire-right",
            List.of(
                DiffResult.mismatch(
                    "scenario-1",
                    "wire-left",
                    "wire-right",
                    List.of(new DiffEntry("$.commandResults[0].ok", 1, 0, "value mismatch"))
                )
            )
        );

        M3GateAutomation.FlakeSummary summary = M3GateAutomation.computeFlakeSummary(
            baseline,
            List.of(rerunStable, rerunFlaky)
        );

        assertEquals(2, summary.runs());
        assertEquals(2, summary.observations());
        assertEquals(1, summary.flakyObservations());
        assertEquals(0.5d, summary.rate());
    }

    @Test
    void runAndWriteProducesReleaseReadinessArtifacts() throws IOException {
        Path outputDir = Files.createTempDirectory("m3-gate-artifacts");
        M3GateAutomation automation = new M3GateAutomation(
            Clock.fixed(Instant.parse("2026-02-23T12:10:00Z"), ZoneOffset.UTC)
        );
        M3GateAutomation.EvidenceConfig config = new M3GateAutomation.EvidenceConfig(
            outputDir,
            2,
            3,
            false
        );

        M3GateAutomation.EvidenceResult result = automation.runAndWrite(config);
        M3GateAutomation.ArtifactPaths artifactPaths = M3GateAutomation.artifactPaths(outputDir);

        assertTrue(Files.exists(artifactPaths.releaseReadinessJson()));
        assertTrue(Files.exists(artifactPaths.releaseReadinessMarkdown()));
        assertTrue(Files.exists(artifactPaths.compatibilityJson()));
        assertTrue(Files.exists(artifactPaths.compatibilityMarkdown()));

        String releaseJson = Files.readString(artifactPaths.releaseReadinessJson());
        String releaseMarkdown = Files.readString(artifactPaths.releaseReadinessMarkdown());
        String compatibilityMarkdown = Files.readString(artifactPaths.compatibilityMarkdown());

        assertTrue(releaseJson.contains("\"compatibilityPassRate\""));
        assertTrue(releaseJson.contains("\"flakeRate\""));
        assertTrue(releaseJson.contains("\"reproTimeP50Minutes\""));
        assertTrue(releaseMarkdown.contains("compatibilityPassRate"));
        assertTrue(releaseMarkdown.contains("flakeRate"));
        assertTrue(releaseMarkdown.contains("reproTimeP50Minutes"));
        assertTrue(compatibilityMarkdown.contains("# Differential Report"));

        assertEquals(2, result.flakeSummary().runs());
        assertEquals(3, result.reproSummary().sampleCount());
        assertEquals(
            CrudScenarioCatalog.scenarios().size() + TransactionScenarioCatalog.scenarios().size(),
            result.compatibilityReport().totalScenarios()
        );
    }

    @Test
    void percentileUsesNearestRankRule() {
        double p50 = M3GateAutomation.percentile(List.of(4.0d, 1.0d, 7.0d, 2.0d, 3.0d), 0.50d);
        assertEquals(3.0d, p50);
    }
}
