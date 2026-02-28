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
    private static final String TARGET_BOOT_27 = "petclinic-boot-2.7-data-3.4";
    private static final String TARGET_BOOT_32 = "petclinic-boot-3.2-data-4.2";

    @Test
    void defaultCatalogIncludesMultipleProjectTargetsAndScenarioCoverage() {
        assertTrue(SpringCompatibilityMatrixRunner.defaultTargets().size() >= 5);
        assertTrue(SpringCompatibilityMatrixRunner.defaultScenarios().size() >= 15);
        assertTrue(
            SpringCompatibilityMatrixRunner.defaultScenarios().stream()
                .anyMatch(SpringCompatibilityMatrixRunner.SpringScenario::isComplexQueryScenario),
            "expected at least one complex query scenario"
        );
        assertTrue(
            SpringCompatibilityMatrixRunner.defaultScenarios().stream()
                .filter(scenario -> scenario.certificationPatternId() != null)
                .count() >= 8,
            "expected expanded certification pattern mapping coverage"
        );
    }

    @Test
    void accountsScenarioResultsAcrossTargetAndSurfaceMatrix() {
        final int scenarioCount = SpringCompatibilityMatrixRunner.defaultScenarios().size();
        final int transactionScenarioCount = (int) SpringCompatibilityMatrixRunner.defaultScenarios().stream()
            .filter(scenario -> scenario.surface() == SpringCompatibilityMatrixRunner.SpringSurface.TRANSACTION_TEMPLATE)
            .count();

        SpringCompatibilityMatrixRunner runner = new SpringCompatibilityMatrixRunner(
            Clock.fixed(Instant.parse("2026-02-23T15:30:00Z"), ZoneOffset.UTC),
            target -> new StubEndpoint(target.id(), true),
            SpringCompatibilityMatrixRunner.defaultTargets(),
            SpringCompatibilityMatrixRunner.defaultScenarios()
        );
        SpringCompatibilityMatrixRunner.EvidenceConfig config = new SpringCompatibilityMatrixRunner.EvidenceConfig(
            Path.of("build/reports/spring-suite-test"),
            List.of(TARGET_BOOT_27, TARGET_BOOT_32),
            false
        );

        SpringCompatibilityMatrixRunner.MatrixReport report = runner.run(config);

        assertEquals(scenarioCount * 2, report.totalCells());
        assertEquals((scenarioCount * 2) - 1, report.passCount());
        assertEquals(1, report.failCount());
        assertEquals(((scenarioCount * 2) - 1.0d) / (scenarioCount * 2.0d), report.passRate(), 1e-9);

        Map<String, SpringCompatibilityMatrixRunner.TargetSummary> targetSummary =
            toMap(report.targetSummaries(), SpringCompatibilityMatrixRunner.TargetSummary::targetId);
        assertEquals(scenarioCount, targetSummary.get(TARGET_BOOT_27).passCount());
        assertEquals(0, targetSummary.get(TARGET_BOOT_27).failCount());
        assertEquals(scenarioCount - 1, targetSummary.get(TARGET_BOOT_32).passCount());
        assertEquals(1, targetSummary.get(TARGET_BOOT_32).failCount());

        Map<SpringCompatibilityMatrixRunner.SpringSurface, SpringCompatibilityMatrixRunner.SurfaceSummary> surfaceSummary =
            toMap(report.surfaceSummaries(), SpringCompatibilityMatrixRunner.SurfaceSummary::surface);
        SpringCompatibilityMatrixRunner.SurfaceSummary transactionSummary =
            surfaceSummary.get(SpringCompatibilityMatrixRunner.SpringSurface.TRANSACTION_TEMPLATE);
        assertEquals((transactionScenarioCount * 2) - 1, transactionSummary.passCount());
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
        assertTrue(json.contains("\"complexQuerySummary\""));
        assertTrue(json.contains("\"complexQueryScenarios\""));
        assertTrue(json.contains("\"certificationPatternMapping\""));
        assertTrue(json.contains("\"certificationPatternId\""));
        assertTrue(json.contains("\"scenarioId\":\"spring.mongo-template.basic-crud\""));
        assertTrue(json.contains("\"scenarioId\":\"spring.transaction-template.multi-template.same-manager\""));
        assertTrue(json.contains("\"scenarioId\":\"spring.complex.aggregation.lookup-unwind-group\""));
        assertTrue(json.contains("\"scenarioId\":\"spring.complex.query.array-index-comparison\""));
        assertTrue(json.contains("\"scenarioId\":\"spring.complex.query.deep-array-document\""));
        assertTrue(json.contains("\"failureMinimization\":[]"));

        assertTrue(markdown.contains("# Spring Data Mongo Compatibility Matrix"));
        assertTrue(markdown.contains("## Matrix"));
        assertTrue(markdown.contains("## Complex Query Matrix"));
        assertTrue(markdown.contains("## Certification Pattern Mapping"));
        assertTrue(markdown.contains("## Failure Minimization"));
        assertTrue(markdown.contains("spring.transaction-template.out-of-scope.same-namespace-interleaving"));
        assertTrue(markdown.contains("spring.complex.query.nested-criteria"));
        assertTrue(markdown.contains("spring.complex.query.expr-array-index-comparison"));
        assertTrue(markdown.contains("| spring.transaction-template.commit | TransactionTemplate | PASS | PASS |"));

        assertEquals(0, report.failCount());
        assertEquals(SpringCompatibilityMatrixRunner.defaultScenarios().size() * 2, report.passCount());
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
