package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class R3FailureLedgerRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void runAndWriteGeneratesDeterministicFailureLedgerArtifacts() throws Exception {
        final Path specRepoRoot = tempDir.resolve("specifications");
        writeFixtureSpecTree(specRepoRoot);

        final Clock fixedClock = Clock.fixed(Instant.parse("2026-02-24T01:30:00Z"), ZoneOffset.UTC);
        final R3FailureLedgerRunner runner = new R3FailureLedgerRunner(
                fixedClock,
                new UnifiedSpecImporter(),
                () -> new StubBackend("left-backend", scenario -> successResult(scenario.id())),
                mongoUri -> new StubBackend("right-backend", scenario -> {
                    if (scenario.description().contains("mismatch")) {
                        return successResult("different-" + scenario.id());
                    }
                    if (scenario.description().contains("error-case")) {
                        throw new IllegalStateException("forced-error");
                    }
                    return successResult(scenario.id());
                }));

        final R3FailureLedgerRunner.RunConfig config = new R3FailureLedgerRunner.RunConfig(
                specRepoRoot,
                tempDir.resolve("out"),
                "seed-r3",
                "mongodb://example",
                10,
                List.of(
                        new R3FailureLedgerRunner.SuiteConfig("crud-unified", "source/crud/tests/unified"),
                        new R3FailureLedgerRunner.SuiteConfig("transactions-unified", "source/transactions/tests/unified")));

        final R3FailureLedgerRunner.RunResult first = runner.runAndWrite(config);
        final R3FailureLedgerRunner.RunResult second = runner.run(config);

        assertEquals(extractFailureIds(first), extractFailureIds(second));
        assertEquals(3, first.entries().size());
        assertEquals(1, first.byTrack().get("txn"));
        assertEquals(1, first.byTrack().get("aggregation"));
        assertEquals(1, first.byTrack().get("query_update"));

        final R3FailureLedgerRunner.ArtifactPaths artifactPaths = R3FailureLedgerRunner.artifactPaths(config.outputDir());
        assertTrue(Files.exists(artifactPaths.jsonArtifact()));
        assertTrue(Files.exists(artifactPaths.markdownArtifact()));

        final Document json = Document.parse(Files.readString(artifactPaths.jsonArtifact()));
        assertEquals(3, json.getInteger("failureCount"));
        assertEquals(3, json.getList("entries", Document.class).size());
        assertEquals(1, json.get("byTrack", Document.class).getInteger("txn"));
    }

    private static List<String> extractFailureIds(final R3FailureLedgerRunner.RunResult result) {
        final List<String> ids = new ArrayList<>();
        for (final R3FailureLedgerRunner.FailureLedgerEntry entry : result.entries()) {
            ids.add(entry.failureId());
        }
        return List.copyOf(ids);
    }

    private static ScenarioOutcome successResult(final String marker) {
        return ScenarioOutcome.success(List.of(Map.of(
                "ok", 1,
                "marker", marker)));
    }

    private static void writeFixtureSpecTree(final Path specRepoRoot) throws IOException {
        final Path crudDir = specRepoRoot.resolve("source/crud/tests/unified");
        final Path txnDir = specRepoRoot.resolve("source/transactions/tests/unified");
        Files.createDirectories(crudDir);
        Files.createDirectories(txnDir);

        Files.writeString(
                crudDir.resolve("crud.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "orders",
                  "tests": [
                    {
                      "description": "mismatch-agg",
                      "operations": [
                        {"name": "aggregate", "arguments": {"pipeline": []}}
                      ]
                    },
                    {
                      "description": "error-case",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {"status": "pending"}}}
                      ]
                    }
                  ]
                }
                """);

        Files.writeString(
                txnDir.resolve("txn.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "txn-mismatch",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {"name": "kim"}}}
                      ]
                    }
                  ]
                }
                """);
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
