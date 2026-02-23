package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RealMongodCorpusRunnerTest {
    @Test
    void buildScenarioCorpusUsesDeterministicSeed() {
        List<String> first = scenarioIds(RealMongodCorpusRunner.buildScenarioCorpus("seed-a", 128));
        List<String> second = scenarioIds(RealMongodCorpusRunner.buildScenarioCorpus("seed-a", 128));
        List<String> different = scenarioIds(RealMongodCorpusRunner.buildScenarioCorpus("seed-b", 128));

        assertEquals(first, second);
        assertNotEquals(first, different);
        assertEquals(128, first.size());
        assertEquals(first.size(), first.stream().distinct().count());
    }

    @Test
    void buildScenarioCorpusDefaultsToLargeTierOneTarget() {
        List<Scenario> corpus = RealMongodCorpusRunner.buildScenarioCorpus("seed-default");

        assertTrue(corpus.size() >= 2_000);
        assertEquals(corpus.size(), corpus.stream().map(Scenario::id).distinct().count());
    }

    @Test
    void runAndWriteGeneratesArtifactsWithTopRegressionSection() throws IOException {
        Path outputDir = Files.createTempDirectory("real-mongod-corpus-runner");
        RealMongodCorpusRunner runner = new RealMongodCorpusRunner(
            Clock.fixed(Instant.parse("2026-02-23T13:00:00Z"), ZoneOffset.UTC),
            () -> new StubBackend("wire-backend", true),
            uri -> new StubBackend("real-mongod", false)
        );
        RealMongodCorpusRunner.RunConfig config = new RealMongodCorpusRunner.RunConfig(
            outputDir,
            "mongodb://localhost:27017",
            "seed-for-test",
            128,
            2
        );

        RealMongodCorpusRunner.RunResult result = runner.runAndWrite(config);
        RealMongodCorpusRunner.ArtifactPaths paths = RealMongodCorpusRunner.artifactPaths(outputDir);

        assertTrue(Files.exists(paths.jsonArtifact()));
        assertTrue(Files.exists(paths.markdownArtifact()));
        assertEquals(2, result.topRegressions().size());
        assertEquals(DiffStatus.ERROR, result.topRegressions().get(0).status());
        assertEquals("crud.create-indexes-duplicate-key", result.topRegressions().get(0).scenarioId());
        assertEquals(DiffStatus.MISMATCH, result.topRegressions().get(1).status());
        assertEquals("txn.lifecycle-transition-path", result.topRegressions().get(1).scenarioId());

        String json = Files.readString(paths.jsonArtifact());
        String markdown = Files.readString(paths.markdownArtifact());

        assertTrue(json.contains("\"seed\":\"seed-for-test\""));
        assertTrue(json.contains("\"topRegressions\""));
        assertTrue(json.contains("\"scenarioId\":\"crud.create-indexes-duplicate-key\""));

        assertTrue(markdown.contains("# Real Mongod Differential Baseline"));
        assertTrue(markdown.contains("seed-for-test"));
        assertTrue(markdown.contains("## Top Regressions"));
        assertTrue(markdown.contains("crud.create-indexes-duplicate-key (ERROR)"));
    }

    private static List<String> scenarioIds(List<Scenario> scenarios) {
        return scenarios.stream().map(Scenario::id).toList();
    }

    private static final class StubBackend implements DifferentialBackend {
        private final String name;
        private final boolean wire;

        private StubBackend(String name, boolean wire) {
            this.name = name;
            this.wire = wire;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ScenarioOutcome execute(Scenario scenario) {
            if (!wire && "crud.create-indexes-duplicate-key".equals(scenario.id())) {
                throw new IllegalStateException("forced regression exception");
            }
            int marker = (!wire && scenario.id().startsWith("txn.")) ? 0 : 1;
            return ScenarioOutcome.success(
                List.of(Map.of("ok", 1, "scenarioId", scenario.id(), "marker", marker))
            );
        }
    }
}
