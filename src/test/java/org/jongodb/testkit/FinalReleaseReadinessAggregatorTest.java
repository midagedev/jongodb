package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class FinalReleaseReadinessAggregatorTest {
    @Test
    void runAndWriteMarksMissingSpringEvidenceWithoutCrashing() throws IOException {
        Path root = Files.createTempDirectory("final-readiness");
        Path m3Dir = root.resolve("m3");
        Path realDir = root.resolve("real");
        Path r1Dir = root.resolve("r1");
        Path outputDir = root.resolve("final");
        Path springMatrixJson = root.resolve("spring/spring-compatibility-matrix.json");

        writeJson(
            M3GateAutomation.artifactPaths(m3Dir).releaseReadinessJson(),
            "{\"generatedAt\":\"2026-02-23T10:00:00Z\",\"overallStatus\":\"PASS\",\"metrics\":"
                + "{\"compatibilityPassRate\":0.99,\"flakeRate\":0.001,\"reproTimeP50Minutes\":2.0}}"
        );
        writeJson(
            RealMongodCorpusRunner.artifactPaths(realDir).jsonArtifact(),
            "{\"generatedAt\":\"2026-02-23T10:01:00Z\",\"summary\":"
                + "{\"total\":20,\"match\":20,\"mismatch\":0,\"error\":0,\"passRate\":1.0}}"
        );
        writeJson(
            R1PerformanceStabilityGateAutomation.artifactPaths(r1Dir).gateJson(),
            "{\"generatedAt\":\"2026-02-23T10:02:00Z\",\"overallStatus\":\"PASS\",\"metrics\":"
                + "{\"coldStartMillis\":120.0,\"resetMillis\":8.0,\"crudP95LatencyMillis\":4.0,"
                + "\"throughputOpsPerSecond\":700.0,\"flakeRate\":0.001}}"
        );

        FinalReleaseReadinessAggregator aggregator = new FinalReleaseReadinessAggregator(
            Clock.fixed(Instant.parse("2026-02-23T10:30:00Z"), ZoneOffset.UTC),
            new NoopEvidenceGenerator()
        );
        FinalReleaseReadinessAggregator.RunConfig config = new FinalReleaseReadinessAggregator.RunConfig(
            outputDir,
            m3Dir,
            realDir,
            r1Dir,
            springMatrixJson,
            false,
            null,
            false
        );

        FinalReleaseReadinessAggregator.RunResult result = aggregator.runAndWrite(config);
        FinalReleaseReadinessAggregator.ArtifactPaths reportPaths = FinalReleaseReadinessAggregator.artifactPaths(outputDir);

        assertTrue(Files.exists(reportPaths.jsonArtifact()));
        assertTrue(Files.exists(reportPaths.markdownArtifact()));
        assertFalse(result.overallPassed());
        assertEquals(3, result.passCount());
        assertEquals(0, result.failCount());
        assertEquals(1, result.missingCount());

        FinalReleaseReadinessAggregator.GateResult springGate = gate(result, "spring-compatibility-matrix");
        assertEquals(FinalReleaseReadinessAggregator.GateStatus.MISSING, springGate.status());
        assertTrue(springGate.diagnostics().get(0).contains("missing artifact"));

        String json = Files.readString(reportPaths.jsonArtifact());
        String markdown = Files.readString(reportPaths.markdownArtifact());
        assertTrue(json.contains("\"overallStatus\":\"FAIL\""));
        assertTrue(json.contains("\"missing\":1"));
        assertTrue(markdown.contains("spring-compatibility-matrix: MISSING"));
        assertTrue(markdown.contains("Missing Evidence Diagnostics"));
    }

    @Test
    void realMongodGateFailsWhenMismatchExists() throws IOException {
        Path root = Files.createTempDirectory("final-readiness-real-fail");
        Path m3Dir = root.resolve("m3");
        Path realDir = root.resolve("real");
        Path r1Dir = root.resolve("r1");
        Path springMatrixJson = root.resolve("spring/spring-compatibility-matrix.json");

        writeJson(
            M3GateAutomation.artifactPaths(m3Dir).releaseReadinessJson(),
            "{\"generatedAt\":\"2026-02-23T11:00:00Z\",\"overallStatus\":\"PASS\",\"metrics\":"
                + "{\"compatibilityPassRate\":0.99,\"flakeRate\":0.001,\"reproTimeP50Minutes\":2.0}}"
        );
        writeJson(
            RealMongodCorpusRunner.artifactPaths(realDir).jsonArtifact(),
            "{\"generatedAt\":\"2026-02-23T11:01:00Z\",\"summary\":"
                + "{\"total\":20,\"match\":19,\"mismatch\":1,\"error\":0,\"passRate\":0.95}}"
        );
        writeJson(
            R1PerformanceStabilityGateAutomation.artifactPaths(r1Dir).gateJson(),
            "{\"generatedAt\":\"2026-02-23T11:02:00Z\",\"overallStatus\":\"PASS\",\"metrics\":"
                + "{\"coldStartMillis\":110.0,\"resetMillis\":7.0,\"crudP95LatencyMillis\":3.5,"
                + "\"throughputOpsPerSecond\":650.0,\"flakeRate\":0.001}}"
        );
        writeJson(
            springMatrixJson,
            "{\"generatedAt\":\"2026-02-23T11:03:00Z\",\"summary\":{\"pass\":98,\"fail\":2}}"
        );

        FinalReleaseReadinessAggregator aggregator = new FinalReleaseReadinessAggregator(
            Clock.fixed(Instant.parse("2026-02-23T11:30:00Z"), ZoneOffset.UTC),
            new NoopEvidenceGenerator()
        );
        FinalReleaseReadinessAggregator.RunResult result = aggregator.run(
            new FinalReleaseReadinessAggregator.RunConfig(
                root.resolve("final"),
                m3Dir,
                realDir,
                r1Dir,
                springMatrixJson,
                false,
                null,
                false
            )
        );

        assertFalse(result.overallPassed());
        assertEquals(3, result.passCount());
        assertEquals(1, result.failCount());
        assertEquals(0, result.missingCount());

        FinalReleaseReadinessAggregator.GateResult realGate = gate(result, "real-mongod-differential-baseline");
        assertEquals(FinalReleaseReadinessAggregator.GateStatus.FAIL, realGate.status());
        assertTrue(realGate.diagnostics().contains("expected mismatch=0 but was 1"));
    }

    @Test
    void generateMissingEvidenceUsesGeneratorForAvailableSources() throws IOException {
        Path root = Files.createTempDirectory("final-readiness-generate");
        Path m3Dir = root.resolve("m3");
        Path realDir = root.resolve("real");
        Path r1Dir = root.resolve("r1");
        Path springMatrixJson = root.resolve("spring/spring-compatibility-matrix.json");

        RecordingEvidenceGenerator generator = new RecordingEvidenceGenerator();
        FinalReleaseReadinessAggregator aggregator = new FinalReleaseReadinessAggregator(
            Clock.fixed(Instant.parse("2026-02-23T12:00:00Z"), ZoneOffset.UTC),
            generator
        );
        FinalReleaseReadinessAggregator.RunResult result = aggregator.run(
            new FinalReleaseReadinessAggregator.RunConfig(
                root.resolve("final"),
                m3Dir,
                realDir,
                r1Dir,
                springMatrixJson,
                true,
                "mongodb://localhost:27017",
                false
            )
        );

        assertEquals(1, generator.m3Calls);
        assertEquals(1, generator.realCalls);
        assertEquals(1, generator.r1Calls);

        FinalReleaseReadinessAggregator.GateResult m3Gate = gate(result, "m3-release-readiness");
        FinalReleaseReadinessAggregator.GateResult realGate = gate(result, "real-mongod-differential-baseline");
        FinalReleaseReadinessAggregator.GateResult r1Gate = gate(result, "r1-performance-stability");
        FinalReleaseReadinessAggregator.GateResult springGate = gate(result, "spring-compatibility-matrix");

        assertEquals(FinalReleaseReadinessAggregator.GateStatus.PASS, m3Gate.status());
        assertEquals(FinalReleaseReadinessAggregator.GateStatus.PASS, realGate.status());
        assertEquals(FinalReleaseReadinessAggregator.GateStatus.PASS, r1Gate.status());
        assertEquals(FinalReleaseReadinessAggregator.GateStatus.MISSING, springGate.status());
        assertTrue(m3Gate.evidenceGenerated());
        assertTrue(realGate.evidenceGenerated());
        assertTrue(r1Gate.evidenceGenerated());
        assertFalse(springGate.evidenceGenerated());
        assertEquals(3, result.passCount());
        assertEquals(0, result.failCount());
        assertEquals(1, result.missingCount());
    }

    private static FinalReleaseReadinessAggregator.GateResult gate(
        FinalReleaseReadinessAggregator.RunResult result,
        String gateId
    ) {
        for (FinalReleaseReadinessAggregator.GateResult gateResult : result.gateResults()) {
            if (gateId.equals(gateResult.gateId())) {
                return gateResult;
            }
        }
        fail("gate not found: " + gateId);
        throw new IllegalStateException("unreachable");
    }

    private static void writeJson(Path path, String content) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8);
    }

    private static final class NoopEvidenceGenerator implements FinalReleaseReadinessAggregator.EvidenceGenerator {
        @Override
        public void generateM3Evidence(Path outputDir) {
            throw new IllegalStateException("should not be called");
        }

        @Override
        public void generateRealMongodEvidence(Path outputDir, String mongoUri) {
            throw new IllegalStateException("should not be called");
        }

        @Override
        public void generateR1PerformanceEvidence(Path outputDir) {
            throw new IllegalStateException("should not be called");
        }
    }

    private static final class RecordingEvidenceGenerator implements FinalReleaseReadinessAggregator.EvidenceGenerator {
        private int m3Calls;
        private int realCalls;
        private int r1Calls;

        @Override
        public void generateM3Evidence(Path outputDir) throws IOException {
            m3Calls++;
            writeJson(
                M3GateAutomation.artifactPaths(outputDir).releaseReadinessJson(),
                "{\"generatedAt\":\"2026-02-23T12:01:00Z\",\"overallStatus\":\"PASS\",\"metrics\":"
                    + "{\"compatibilityPassRate\":0.99,\"flakeRate\":0.001,\"reproTimeP50Minutes\":2.0}}"
            );
        }

        @Override
        public void generateRealMongodEvidence(Path outputDir, String mongoUri) throws IOException {
            realCalls++;
            writeJson(
                RealMongodCorpusRunner.artifactPaths(outputDir).jsonArtifact(),
                "{\"generatedAt\":\"2026-02-23T12:02:00Z\",\"summary\":"
                    + "{\"total\":10,\"match\":10,\"mismatch\":0,\"error\":0,\"passRate\":1.0}}"
            );
        }

        @Override
        public void generateR1PerformanceEvidence(Path outputDir) throws IOException {
            r1Calls++;
            writeJson(
                R1PerformanceStabilityGateAutomation.artifactPaths(outputDir).gateJson(),
                "{\"generatedAt\":\"2026-02-23T12:03:00Z\",\"overallStatus\":\"PASS\",\"metrics\":"
                    + "{\"coldStartMillis\":100.0,\"resetMillis\":5.0,\"crudP95LatencyMillis\":3.0,"
                    + "\"throughputOpsPerSecond\":700.0,\"flakeRate\":0.001}}"
            );
        }
    }
}
