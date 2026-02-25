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
import java.util.Map;
import java.util.function.Function;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ComplexQueryCertificationRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void runAndWriteProducesGateAndReplayArtifacts() throws IOException {
        final List<ComplexQueryPatternPack.PatternCase> pack = List.of(
                pattern(
                        "cq.test.match",
                        ComplexQueryPatternPack.SupportClass.SUPPORTED,
                        ComplexQueryPatternPack.ExpectedOutcome.MATCH,
                        scenario("cq.test.match")),
                pattern(
                        "cq.test.mismatch",
                        ComplexQueryPatternPack.SupportClass.SUPPORTED,
                        ComplexQueryPatternPack.ExpectedOutcome.MATCH,
                        scenario("cq.test.mismatch")),
                pattern(
                        "cq.test.unsupported",
                        ComplexQueryPatternPack.SupportClass.EXPLICITLY_UNSUPPORTED,
                        ComplexQueryPatternPack.ExpectedOutcome.UNSUPPORTED_POLICY,
                        scenario("cq.test.unsupported")),
                pattern(
                        "cq.test.error",
                        ComplexQueryPatternPack.SupportClass.PARTIAL,
                        ComplexQueryPatternPack.ExpectedOutcome.MATCH,
                        scenario("cq.test.error")));

        final Clock fixedClock = Clock.fixed(Instant.parse("2026-02-25T03:10:00Z"), ZoneOffset.UTC);
        final ComplexQueryCertificationRunner runner = new ComplexQueryCertificationRunner(
                fixedClock,
                () -> new StubBackend("left", scenario -> {
                    if ("cq.test.unsupported".equals(scenario.id())) {
                        return ScenarioOutcome.failure(
                                "command 'find' failed at index 0: unsupported feature (code=238, codeName=NotImplemented)");
                    }
                    return successOutcome("left-" + scenario.id());
                }),
                mongoUri -> new StubBackend("right", scenario -> {
                    if ("cq.test.mismatch".equals(scenario.id())) {
                        return successOutcome("right-different");
                    }
                    if ("cq.test.error".equals(scenario.id())) {
                        throw new IllegalStateException("forced-error");
                    }
                    return successOutcome("left-" + scenario.id());
                }),
                pack);

        final Path outputDir = tempDir.resolve("out");
        final ComplexQueryCertificationRunner.RunConfig config = new ComplexQueryCertificationRunner.RunConfig(
                outputDir,
                "seed-a",
                null,
                "mongodb://example",
                true);

        final ComplexQueryCertificationRunner.RunResult result = runner.runAndWrite(config);
        assertEquals(4, result.patternResults().size());
        assertEquals(1, result.summary().mismatchCount());
        assertEquals(1, result.summary().errorCount());
        assertEquals(1, result.summary().unsupportedByPolicyCount());
        assertEquals(0, result.summary().unsupportedDeltaCount());
        assertEquals(ComplexQueryCertificationRunner.GateStatus.FAIL, result.gateSummary().status());

        final Map<String, ComplexQueryCertificationRunner.PatternStatus> statusById = result.patternResults().stream()
                .collect(java.util.stream.Collectors.toMap(
                        ComplexQueryCertificationRunner.PatternResult::patternId,
                        ComplexQueryCertificationRunner.PatternResult::status));
        assertEquals(ComplexQueryCertificationRunner.PatternStatus.MATCH, statusById.get("cq.test.match"));
        assertEquals(ComplexQueryCertificationRunner.PatternStatus.MISMATCH, statusById.get("cq.test.mismatch"));
        assertEquals(
                ComplexQueryCertificationRunner.PatternStatus.UNSUPPORTED_POLICY,
                statusById.get("cq.test.unsupported"));
        assertEquals(ComplexQueryCertificationRunner.PatternStatus.ERROR, statusById.get("cq.test.error"));

        final ComplexQueryCertificationRunner.ArtifactPaths paths =
                ComplexQueryCertificationRunner.artifactPaths(outputDir);
        assertTrue(Files.exists(paths.jsonArtifact()));
        assertTrue(Files.exists(paths.markdownArtifact()));
        assertTrue(Files.exists(paths.replayBundleDir().resolve(DeterministicReplayBundles.MANIFEST_FILE_NAME)));

        final Document json = Document.parse(Files.readString(paths.jsonArtifact()));
        assertEquals("FAIL", json.get("gate", Document.class).getString("status"));
        assertEquals(4, json.get("summary", Document.class).getInteger("totalPatterns"));
        assertEquals(1, json.get("summary", Document.class).getInteger("mismatchCount"));
        assertEquals(1, json.get("summary", Document.class).getInteger("errorCount"));
        assertEquals(1, json.get("summary", Document.class).getInteger("unsupportedByPolicyCount"));
        assertEquals(3, json.get("replayBundles", Document.class).getInteger("count"));

        final String markdown = Files.readString(paths.markdownArtifact());
        assertTrue(markdown.contains("gate: FAIL"));
        assertTrue(markdown.contains("cq.test.mismatch"));
        assertTrue(markdown.contains("cq.test.unsupported"));
        assertTrue(markdown.contains("rationale=rationale"));
    }

    @Test
    void runRespectsPatternLimit() {
        final List<ComplexQueryPatternPack.PatternCase> pack = List.of(
                pattern("cq.limit.1", ComplexQueryPatternPack.SupportClass.SUPPORTED,
                        ComplexQueryPatternPack.ExpectedOutcome.MATCH, scenario("cq.limit.1")),
                pattern("cq.limit.2", ComplexQueryPatternPack.SupportClass.SUPPORTED,
                        ComplexQueryPatternPack.ExpectedOutcome.MATCH, scenario("cq.limit.2")),
                pattern("cq.limit.3", ComplexQueryPatternPack.SupportClass.SUPPORTED,
                        ComplexQueryPatternPack.ExpectedOutcome.MATCH, scenario("cq.limit.3")));

        final ComplexQueryCertificationRunner runner = new ComplexQueryCertificationRunner(
                Clock.fixed(Instant.parse("2026-02-25T03:20:00Z"), ZoneOffset.UTC),
                () -> new StubBackend("left", scenario -> successOutcome(scenario.id())),
                mongoUri -> new StubBackend("right", scenario -> successOutcome(scenario.id())),
                pack);

        final ComplexQueryCertificationRunner.RunConfig config = new ComplexQueryCertificationRunner.RunConfig(
                tempDir.resolve("limit-out"),
                "seed-limit",
                2,
                "mongodb://example",
                false);

        final ComplexQueryCertificationRunner.RunResult result = runner.run(config);
        assertEquals(2, result.patternResults().size());
    }

    private static ComplexQueryPatternPack.PatternCase pattern(
            final String id,
            final ComplexQueryPatternPack.SupportClass supportClass,
            final ComplexQueryPatternPack.ExpectedOutcome expectedOutcome,
            final Scenario scenario) {
        return new ComplexQueryPatternPack.PatternCase(
                id,
                id,
                supportClass,
                expectedOutcome,
                "rationale",
                "use-case",
                scenario);
    }

    private static Scenario scenario(final String id) {
        return new Scenario(
                id,
                id,
                List.of(new ScenarioCommand("find", Map.of("collection", "users", "filter", Map.of()))));
    }

    private static ScenarioOutcome successOutcome(final String marker) {
        return ScenarioOutcome.success(List.of(Map.of("ok", 1, "marker", marker)));
    }

    private static final class StubBackend implements DifferentialBackend {
        private final String name;
        private final Function<Scenario, ScenarioOutcome> executor;

        private StubBackend(final String name, final Function<Scenario, ScenarioOutcome> executor) {
            this.name = name;
            this.executor = executor;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ScenarioOutcome execute(final Scenario scenario) {
            return executor.apply(scenario);
        }
    }
}
