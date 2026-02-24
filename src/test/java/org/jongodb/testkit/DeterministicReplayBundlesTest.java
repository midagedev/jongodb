package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeterministicReplayBundlesTest {
    @TempDir
    Path tempDir;

    @Test
    void writeAndReadBundleManifestRoundTrips() throws Exception {
        final DiffResult diffResult = DiffResult.mismatch(
                "suite/case-1",
                "wire-backend",
                "real-mongod",
                List.of(new DiffEntry("$.commandResults[0].ok", 1, 0, "value mismatch")));
        final List<ScenarioCommand> commands = List.of(
                new ScenarioCommand("find", Map.of("find", "users", "$db", "app")));
        final DeterministicReplayBundles.Bundle bundle =
                DeterministicReplayBundles.fromFailure("utf", diffResult, commands);

        final Path bundleDir = tempDir.resolve("bundles");
        DeterministicReplayBundles.writeBundles(bundleDir, List.of(bundle));

        final DeterministicReplayBundles.Manifest manifest = DeterministicReplayBundles.readManifest(bundleDir);
        assertEquals(1, manifest.bundleCount());
        assertEquals(1, manifest.failures().size());
        assertEquals(bundle.failureId(), manifest.failures().get(0).failureId());

        final DeterministicReplayBundles.Bundle loaded =
                DeterministicReplayBundles.readBundle(bundleDir, bundle.failureId());
        assertEquals(bundle.failureId(), loaded.failureId());
        assertEquals(DiffStatus.MISMATCH, loaded.status());
        assertEquals(1, loaded.commands().size());
        assertNotNull(loaded.stateSnapshot().commandDigest());
        assertFalse(loaded.stateSnapshot().commandDigest().isBlank());
    }

    @Test
    void replayRunnerExecutesSingleFailureIdFromBundle() throws Exception {
        final DiffResult diffResult = DiffResult.mismatch(
                "suite/case-2",
                "wire-backend",
                "real-mongod",
                List.of(new DiffEntry("$.commandResults[0].ok", 1, 0, "value mismatch")));
        final List<ScenarioCommand> commands = List.of(
                new ScenarioCommand("find", Map.of("find", "users", "$db", "app")));
        final DeterministicReplayBundles.Bundle bundle =
                DeterministicReplayBundles.fromFailure("utf", diffResult, commands);

        final Path bundleDir = tempDir.resolve("bundles");
        DeterministicReplayBundles.writeBundles(bundleDir, List.of(bundle));

        final DeterministicReplayBundleRunner.ReplayResult replayResult = DeterministicReplayBundleRunner.replay(
                new DeterministicReplayBundleRunner.ReplayConfig(bundleDir, bundle.failureId()),
                new StubBackend());

        assertEquals(bundle.failureId(), replayResult.failureId());
        assertEquals("$.commandResults[0].ok", replayResult.probePath());
        assertTrue(replayResult.probeMatched());
        assertTrue(replayResult.outcome().success());
    }

    private static final class StubBackend implements DifferentialBackend {
        @Override
        public String name() {
            return "stub-backend";
        }

        @Override
        public ScenarioOutcome execute(final Scenario scenario) {
            return ScenarioOutcome.success(List.of(Map.of("ok", 0, "scenarioId", scenario.id())));
        }
    }
}
