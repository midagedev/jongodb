package org.jongodb.testkit.springsuite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jongodb.testkit.Scenario;
import org.jongodb.testkit.ScenarioOutcome;
import org.junit.jupiter.api.Test;

class SpringCompatibilityMatrixRunnerTest {
    private static final String TARGET_BOOT_27 = "boot-2.7-data-3.4";
    private static final String TARGET_BOOT_32 = "boot-3.2-data-4.2";

    @Test
    void accountsScenarioResultsAcrossTargetAndSurfaceMatrix() {
        SpringCompatibilityMatrixRunner runner = new SpringCompatibilityMatrixRunner(
            Clock.fixed(Instant.parse("2026-02-23T15:30:00Z"), ZoneOffset.UTC),
            target -> new StubEndpoint(target.id(), true),
            SpringCompatibilityMatrixRunner.defaultTargets(),
            SpringCompatibilityMatrixRunner.defaultScenarios()
        );
        SpringCompatibilityMatrixRunner.EvidenceConfig config = new SpringCompatibilityMatrixRunner.EvidenceConfig(
            Path.of("build/reports/spring-suite-test"),
            List.of(),
            false
        );

        SpringCompatibilityMatrixRunner.MatrixReport report = runner.run(config);

        assertEquals(6, report.totalCells());
        assertEquals(5, report.passCount());
        assertEquals(1, report.failCount());
        assertEquals(5.0d / 6.0d, report.passRate(), 1e-9);

        Map<String, SpringCompatibilityMatrixRunner.TargetSummary> targetSummary =
            toMap(report.targetSummaries(), SpringCompatibilityMatrixRunner.TargetSummary::targetId);
        assertEquals(3, targetSummary.get(TARGET_BOOT_27).passCount());
        assertEquals(0, targetSummary.get(TARGET_BOOT_27).failCount());
        assertEquals(2, targetSummary.get(TARGET_BOOT_32).passCount());
        assertEquals(1, targetSummary.get(TARGET_BOOT_32).failCount());

        Map<SpringCompatibilityMatrixRunner.SpringSurface, SpringCompatibilityMatrixRunner.SurfaceSummary> surfaceSummary =
            toMap(report.surfaceSummaries(), SpringCompatibilityMatrixRunner.SurfaceSummary::surface);
        SpringCompatibilityMatrixRunner.SurfaceSummary transactionSummary =
            surfaceSummary.get(SpringCompatibilityMatrixRunner.SpringSurface.TRANSACTION_TEMPLATE);
        assertEquals(1, transactionSummary.passCount());
        assertEquals(1, transactionSummary.failCount());

        List<SpringCompatibilityMatrixRunner.FailureSample> failures = report.failureMinimization(3);
        assertEquals(1, failures.size());
        assertEquals(TARGET_BOOT_32, failures.get(0).targetId());
        assertEquals("spring.transaction-template.commit", failures.get(0).scenarioId());
        assertTrue(failures.get(0).errorMessage().contains("NoSuchTransaction"));
    }

    @Test
    void runAndWriteGeneratesJsonAndMarkdownArtifacts() throws IOException {
        Path outputDir = Files.createTempDirectory("spring-compatibility-matrix");
        SpringCompatibilityMatrixRunner runner = new SpringCompatibilityMatrixRunner(
            Clock.fixed(Instant.parse("2026-02-23T15:45:00Z"), ZoneOffset.UTC),
            target -> new StubEndpoint(target.id(), false),
            SpringCompatibilityMatrixRunner.defaultTargets(),
            SpringCompatibilityMatrixRunner.defaultScenarios()
        );
        SpringCompatibilityMatrixRunner.EvidenceConfig config = new SpringCompatibilityMatrixRunner.EvidenceConfig(
            outputDir,
            List.of(TARGET_BOOT_27, TARGET_BOOT_32),
            false
        );

        SpringCompatibilityMatrixRunner.MatrixReport report = runner.runAndWrite(config);
        SpringCompatibilityMatrixRunner.ArtifactPaths paths = SpringCompatibilityMatrixRunner.artifactPaths(outputDir);

        assertTrue(Files.exists(paths.matrixJson()));
        assertTrue(Files.exists(paths.matrixMarkdown()));

        String json = Files.readString(paths.matrixJson());
        String markdown = Files.readString(paths.matrixMarkdown());

        assertTrue(json.contains("\"matrixDimensions\""));
        assertTrue(json.contains("\"targetSummary\""));
        assertTrue(json.contains("\"surfaceSummary\""));
        assertTrue(json.contains("\"scenarioId\":\"spring.mongo-template.basic-crud\""));
        assertTrue(json.contains("\"failureMinimization\":[]"));

        assertTrue(markdown.contains("# Spring Data Mongo Compatibility Matrix"));
        assertTrue(markdown.contains("## Matrix"));
        assertTrue(markdown.contains("## Failure Minimization"));
        assertTrue(markdown.contains("| spring.transaction-template.commit | TransactionTemplate | PASS | PASS |"));

        assertEquals(0, report.failCount());
        assertEquals(6, report.passCount());
    }

    private static <K, V> Map<K, V> toMap(List<V> values, Function<V, K> keyMapper) {
        return values.stream().collect(Collectors.toMap(keyMapper, Function.identity()));
    }

    private static final class StubEndpoint implements SpringCompatibilityMatrixRunner.JongodbEndpoint {
        private final String targetId;
        private final boolean failTransactionOnBoot32;

        private StubEndpoint(String targetId, boolean failTransactionOnBoot32) {
            this.targetId = targetId;
            this.failTransactionOnBoot32 = failTransactionOnBoot32;
        }

        @Override
        public String id() {
            return "stub-endpoint";
        }

        @Override
        public ScenarioOutcome execute(Scenario scenario) {
            if (
                failTransactionOnBoot32
                    && TARGET_BOOT_32.equals(targetId)
                    && "spring.transaction-template.commit".equals(scenario.id())
            ) {
                return ScenarioOutcome.failure("NoSuchTransaction: simulated transaction mismatch for matrix target");
            }
            return ScenarioOutcome.success(
                List.of(
                    Map.of(
                        "ok",
                        1,
                        "scenarioId",
                        scenario.id(),
                        "targetId",
                        targetId
                    )
                )
            );
        }
    }
}
