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

class R1PerformanceStabilityGateAutomationTest {
    @Test
    void rendersDeterministicMarkdownAndJsonArtifacts() {
        R1BenchmarkRunner.BenchmarkResult benchmarkResult = new R1BenchmarkRunner.BenchmarkResult(
            List.of(100.0d, 120.0d, 140.0d),
            List.of(11.0d, 12.0d, 13.0d),
            List.of(1.0d, 2.0d, 3.0d, 4.0d, 5.0d),
            10,
            5,
            120.0d,
            12.0d,
            5.0d,
            800.0d
        );

        DifferentialReport baselineReport = new DifferentialReport(
            Instant.parse("2026-02-23T14:00:00Z"),
            "wire-left",
            "wire-right",
            List.of(
                DiffResult.match("scenario-a", "wire-left", "wire-right"),
                DiffResult.mismatch(
                    "scenario-b",
                    "wire-left",
                    "wire-right",
                    List.of(new DiffEntry("$.ok", 1, 0, "value mismatch"))
                )
            )
        );

        R1FlakeRateEvaluator.FlakeEvaluation flakeEvaluation = new R1FlakeRateEvaluator.FlakeEvaluation(
            baselineReport,
            List.of(),
            new R1FlakeRateEvaluator.FlakeSummary(4, 8, 1, 0.125d)
        );

        R1PerformanceStabilityGateAutomation.EvidenceResult result =
            new R1PerformanceStabilityGateAutomation.EvidenceResult(
                Instant.parse("2026-02-23T15:00:00Z"),
                321L,
                benchmarkResult,
                flakeEvaluation,
                List.of(
                    new R1PerformanceStabilityGateAutomation.GateCheck(
                        "cold-start",
                        "coldStartMillis",
                        QualityGateOperator.LESS_OR_EQUAL,
                        120.0d,
                        150.0d,
                        QualityGateStatus.PASS
                    ),
                    new R1PerformanceStabilityGateAutomation.GateCheck(
                        "reset",
                        "resetMillis",
                        QualityGateOperator.LESS_OR_EQUAL,
                        12.0d,
                        10.0d,
                        QualityGateStatus.FAIL
                    ),
                    new R1PerformanceStabilityGateAutomation.GateCheck(
                        "crud-p95-latency",
                        "crudP95LatencyMillis",
                        QualityGateOperator.LESS_OR_EQUAL,
                        5.0d,
                        5.0d,
                        QualityGateStatus.PASS
                    ),
                    new R1PerformanceStabilityGateAutomation.GateCheck(
                        "flake-rate",
                        "flakeRate",
                        QualityGateOperator.LESS_OR_EQUAL,
                        0.125d,
                        0.002d,
                        QualityGateStatus.FAIL
                    )
                )
            );

        R1PerformanceStabilityGateAutomation automation = new R1PerformanceStabilityGateAutomation(
            Clock.fixed(Instant.parse("2026-02-23T15:00:00Z"), ZoneOffset.UTC),
            new R1BenchmarkRunner(),
            new R1FlakeRateEvaluator()
        );

        String markdown = automation.renderGateMarkdown(result);
        String json = automation.renderGateJson(result);

        String expectedMarkdown = """
            # R1 Performance and Stability Gate

            - generatedAt: 2026-02-23T15:00:00Z
            - overall: FAIL
            - pass: 2
            - fail: 2
            - durationMillis: 321

            ## Metrics
            - coldStartMillis: 120.00ms
            - resetMillis: 12.00ms
            - crudP95LatencyMillis: 5.00ms
            - throughputOpsPerSecond: 800.00ops/s
            - flakeRate: 12.50%

            ## Benchmarks
            - warmupOperations: 10
            - measuredOperations: 5
            - coldStartSamplesMillis: [100.0000, 120.0000, 140.0000]
            - resetSamplesMillis: [11.0000, 12.0000, 13.0000]
            - crudLatencySamplesMillis: [1.0000, 2.0000, 3.0000, 4.0000, 5.0000]

            ## Flake Evidence
            - runs: 4
            - observations: 8
            - flakyObservations: 1
            - flakeRate: 12.50%

            ## Differential Baseline
            - total: 2
            - match: 1
            - mismatch: 1
            - error: 0

            ## Gates
            - cold-start: PASS (coldStartMillis 120.00ms <= 150.00ms)
            - reset: FAIL (resetMillis 12.00ms <= 10.00ms)
            - crud-p95-latency: PASS (crudP95LatencyMillis 5.00ms <= 5.00ms)
            - flake-rate: FAIL (flakeRate 12.50% <= 0.20%)
            """;

        String expectedJson =
            "{\"generatedAt\":\"2026-02-23T15:00:00Z\",\"overallStatus\":\"FAIL\",\"summary\":"
                + "{\"pass\":2,\"fail\":2},\"buildInfo\":{\"durationMillis\":321},\"metrics\":"
                + "{\"coldStartMillis\":120.0,\"resetMillis\":12.0,\"crudP95LatencyMillis\":5.0,"
                + "\"throughputOpsPerSecond\":800.0,\"flakeRate\":0.125},\"benchmark\":"
                + "{\"warmupOperations\":10,\"measuredOperations\":5,\"coldStartSamplesMillis\":"
                + "[100.0,120.0,140.0],\"resetSamplesMillis\":[11.0,12.0,13.0],"
                + "\"crudLatencySamplesMillis\":[1.0,2.0,3.0,4.0,5.0]},\"flake\":"
                + "{\"runs\":4,\"observations\":8,\"flakyObservations\":1,\"rate\":0.125},"
                + "\"differentialBaseline\":{\"leftBackend\":\"wire-left\",\"rightBackend\":"
                + "\"wire-right\",\"total\":2,\"match\":1,\"mismatch\":1,\"error\":0},"
                + "\"gates\":[{\"gateId\":\"cold-start\",\"metricKey\":\"coldStartMillis\","
                + "\"measuredValue\":120.0,\"operator\":\"<=\",\"thresholdValue\":150.0,"
                + "\"status\":\"PASS\"},{\"gateId\":\"reset\",\"metricKey\":\"resetMillis\","
                + "\"measuredValue\":12.0,\"operator\":\"<=\",\"thresholdValue\":10.0,"
                + "\"status\":\"FAIL\"},{\"gateId\":\"crud-p95-latency\","
                + "\"metricKey\":\"crudP95LatencyMillis\",\"measuredValue\":5.0,"
                + "\"operator\":\"<=\",\"thresholdValue\":5.0,\"status\":\"PASS\"},"
                + "{\"gateId\":\"flake-rate\",\"metricKey\":\"flakeRate\",\"measuredValue\":0.125,"
                + "\"operator\":\"<=\",\"thresholdValue\":0.002,\"status\":\"FAIL\"}]}";

        assertEquals(expectedMarkdown, markdown);
        assertEquals(expectedJson, json);
    }

    @Test
    void runAndWriteProducesR1GateArtifacts() throws IOException {
        Path outputDir = Files.createTempDirectory("r1-performance-stability");
        R1PerformanceStabilityGateAutomation automation = new R1PerformanceStabilityGateAutomation(
            Clock.fixed(Instant.parse("2026-02-23T15:10:00Z"), ZoneOffset.UTC),
            new R1BenchmarkRunner(),
            new R1FlakeRateEvaluator()
        );
        R1PerformanceStabilityGateAutomation.EvidenceConfig config =
            new R1PerformanceStabilityGateAutomation.EvidenceConfig(
                outputDir,
                1,
                new R1BenchmarkRunner.BenchmarkConfig(1, 1, 2, 10),
                false
            );

        R1PerformanceStabilityGateAutomation.EvidenceResult result = automation.runAndWrite(config);
        R1PerformanceStabilityGateAutomation.ArtifactPaths paths =
            R1PerformanceStabilityGateAutomation.artifactPaths(outputDir);

        assertTrue(Files.exists(paths.gateJson()));
        assertTrue(Files.exists(paths.gateMarkdown()));
        assertTrue(Files.exists(paths.flakeBaselineJson()));
        assertTrue(Files.exists(paths.flakeBaselineMarkdown()));
        assertTrue(Files.readString(paths.gateJson()).contains("\"metrics\""));
        assertTrue(Files.readString(paths.gateMarkdown()).contains("# R1 Performance and Stability Gate"));
        assertEquals(1, result.flakeEvaluation().summary().runs());
    }
}
