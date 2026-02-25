package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Test;

class FixtureManifestToolTest {
    @Test
    void rendersProfilePlanSummary() {
        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final int exitCode = FixtureManifestTool.run(
                new String[] {
                    "--manifest=testkit/fixture/manifests/baseline-dev-smoke-full.json",
                    "--profile=smoke"
                },
                new PrintStream(outBytes),
                new PrintStream(errBytes));

        assertEquals(0, exitCode);
        final String output = outBytes.toString();
        assertTrue(output.contains("Fixture manifest validated"));
        assertTrue(output.contains("- profile: smoke"));
        assertTrue(output.contains("- fingerprint: "));
        assertEquals("", errBytes.toString());
    }

    @Test
    void failsWithUsageWhenManifestArgIsMissing() {
        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final int exitCode = FixtureManifestTool.run(
                new String[] {"--profile=dev"},
                new PrintStream(outBytes),
                new PrintStream(errBytes));

        assertEquals(2, exitCode);
        final String errorOutput = errBytes.toString();
        assertTrue(errorOutput.contains("--manifest=<path> is required"));
        assertTrue(errorOutput.contains("Usage: FixtureManifestTool"));
    }
}
