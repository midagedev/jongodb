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

class UnifiedSpecCorpusRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void runAndWriteProducesBreakdownAndFailureReplays() throws IOException {
        final Path specRoot = tempDir.resolve("specs");
        Files.createDirectories(specRoot);
        Files.writeString(
                specRoot.resolve("utf.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "match-case",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {"name": "alice"}}}
                      ]
                    },
                    {
                      "description": "mismatch-case",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {"name": "bob"}}}
                      ]
                    },
                    {
                      "description": "error-case",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {"name": "charlie"}}}
                      ]
                    },
                    {
                      "description": "unsupported-case",
                      "operations": [
                        {"name": "renameCollection", "arguments": {}}
                      ]
                    }
                  ]
                }
                """);

        final Clock fixedClock = Clock.fixed(Instant.parse("2026-02-23T10:15:30Z"), ZoneOffset.UTC);
        final UnifiedSpecCorpusRunner runner = new UnifiedSpecCorpusRunner(
                fixedClock,
                new UnifiedSpecImporter(),
                () -> new StubBackend("left-backend", scenario -> successResult(scenario.id())),
                mongoUri -> new StubBackend("right-backend", scenario -> {
                    if (scenario.description().equals("mismatch-case")) {
                        return successResult("different-" + scenario.id());
                    }
                    if (scenario.description().equals("error-case")) {
                        throw new IllegalStateException("forced-error");
                    }
                    return successResult(scenario.id());
                }));

        final Path outputDir = tempDir.resolve("out");
        final UnifiedSpecCorpusRunner.RunConfig config = new UnifiedSpecCorpusRunner.RunConfig(
                specRoot,
                outputDir,
                "seed-a",
                "mongodb://example",
                10);

        final UnifiedSpecCorpusRunner.RunResult result = runner.runAndWrite(config);
        assertEquals(3, result.importResult().importedCount());
        assertEquals(1, result.importResult().unsupportedCount());
        assertEquals(0, result.importResult().skippedCount());

        assertEquals(3, result.differentialReport().totalScenarios());
        assertEquals(1, result.differentialReport().matchCount());
        assertEquals(1, result.differentialReport().mismatchCount());
        assertEquals(1, result.differentialReport().errorCount());
        assertEquals(2, result.failureReplays().size());

        final UnifiedSpecCorpusRunner.ArtifactPaths artifactPaths = UnifiedSpecCorpusRunner.artifactPaths(outputDir);
        assertTrue(Files.exists(artifactPaths.jsonArtifact()));
        assertTrue(Files.exists(artifactPaths.markdownArtifact()));
        assertTrue(Files.exists(artifactPaths.replayBundleDir().resolve(DeterministicReplayBundles.MANIFEST_FILE_NAME)));

        final Document json = Document.parse(Files.readString(artifactPaths.jsonArtifact()));
        assertEquals(3, json.get("importSummary", Document.class).getInteger("imported"));
        assertEquals(1, json.get("importSummary", Document.class).getInteger("unsupported"));
        assertEquals(1, json.get("differentialSummary", Document.class).getInteger("match"));
        assertEquals(1, json.get("differentialSummary", Document.class).getInteger("mismatch"));
        assertEquals(1, json.get("differentialSummary", Document.class).getInteger("error"));
        assertEquals(2, json.get("replayBundles", Document.class).getInteger("count"));

        final String markdown = Files.readString(artifactPaths.markdownArtifact());
        assertTrue(markdown.contains("pass: 1"));
        assertTrue(markdown.contains("unsupported: 1"));
        assertTrue(markdown.contains("mismatch-case"));
        assertTrue(markdown.contains("error-case"));
        assertTrue(markdown.contains("## Replay Bundles"));
    }

    @Test
    void runAndWriteIncludesDistinctMismatchInReplayCases() throws IOException {
        final Path specRoot = tempDir.resolve("distinct-specs");
        Files.createDirectories(specRoot);
        Files.writeString(
                specRoot.resolve("distinct.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "distinct-mismatch-case",
                      "operations": [
                        {"name": "distinct", "arguments": {"fieldName": "tag", "filter": {"active": true}}}
                      ]
                    }
                  ]
                }
                """);

        final Clock fixedClock = Clock.fixed(Instant.parse("2026-02-23T10:20:30Z"), ZoneOffset.UTC);
        final UnifiedSpecCorpusRunner runner = new UnifiedSpecCorpusRunner(
                fixedClock,
                new UnifiedSpecImporter(),
                () -> new StubBackend("left-backend", scenario -> successResult(scenario.id())),
                mongoUri -> new StubBackend("right-backend", scenario -> {
                    if (scenario.description().equals("distinct-mismatch-case")) {
                        return successResult("different-" + scenario.id());
                    }
                    return successResult(scenario.id());
                }));

        final Path outputDir = tempDir.resolve("distinct-out");
        final UnifiedSpecCorpusRunner.RunResult result = runner.runAndWrite(new UnifiedSpecCorpusRunner.RunConfig(
                specRoot,
                outputDir,
                "seed-distinct",
                "mongodb://example",
                10));

        assertEquals(1, result.importResult().importedCount());
        assertEquals(1, result.differentialReport().mismatchCount());
        assertEquals(1, result.failureReplays().size());
        assertEquals("distinct", result.failureReplays().get(0).commands().get(0).commandName());

        final String markdown = Files.readString(UnifiedSpecCorpusRunner.artifactPaths(outputDir).markdownArtifact());
        assertTrue(markdown.contains("distinct-mismatch-case"));
    }

    private static ScenarioOutcome successResult(final String marker) {
        return ScenarioOutcome.success(List.of(Map.of(
                "ok", 1,
                "marker", marker)));
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
