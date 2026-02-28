package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixtureArtifactGovernanceToolTest {
    @Test
    void prunesOldArtifactsWhileKeepingFrozenVersions(@TempDir final Path tempDir) throws Exception {
        final Path inputDir = tempDir.resolve("input");
        final Path artifactRoot = tempDir.resolve("artifacts");
        Files.createDirectories(inputDir);

        Files.writeString(inputDir.resolve("app.users.ndjson"), "{\"_id\":1,\"name\":\"alpha\"}\n", StandardCharsets.UTF_8);
        assertEquals(0, pack(inputDir, artifactRoot.resolve("v1.0.0"), "1.0.0", null));

        Files.writeString(inputDir.resolve("app.users.ndjson"),
                "{\"_id\":1,\"name\":\"alpha\"}\n{\"_id\":2,\"name\":\"beta\"}\n",
                StandardCharsets.UTF_8);
        assertEquals(0, pack(inputDir, artifactRoot.resolve("v1.1.0"), "1.1.0", artifactRoot.resolve("v1.0.0/fixture-artifact-manifest.json")));

        Files.writeString(inputDir.resolve("app.users.ndjson"),
                "{\"_id\":1,\"name\":\"alpha\"}\n{\"_id\":2,\"name\":\"beta\"}\n{\"_id\":3,\"name\":\"gamma\"}\n",
                StandardCharsets.UTF_8);
        assertEquals(0, pack(inputDir, artifactRoot.resolve("v1.2.0"), "1.2.0", artifactRoot.resolve("v1.1.0/fixture-artifact-manifest.json")));

        final int governanceExit = FixtureArtifactGovernanceTool.run(
                new String[] {
                    "--artifact-root=" + artifactRoot,
                    "--retain=1",
                    "--freeze-version=1.0.0",
                    "--register-consumer=spring-smoke",
                    "--register-version=1.2.0"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
        assertEquals(0, governanceExit);

        assertTrue(Files.exists(artifactRoot.resolve("v1.0.0")));
        assertTrue(!Files.exists(artifactRoot.resolve("v1.1.0")));
        assertTrue(Files.exists(artifactRoot.resolve("v1.2.0")));

        final Document report = Document.parse(Files.readString(
                artifactRoot.resolve("fixture-artifact-governance-report.json"),
                StandardCharsets.UTF_8));
        assertEquals(2, report.getInteger("keptCount"));
        assertEquals(1, report.getInteger("prunedCount"));

        final Document usageIndex = Document.parse(Files.readString(
                artifactRoot.resolve("fixture-usage-index.json"),
                StandardCharsets.UTF_8));
        final Document consumers = usageIndex.get("consumers", Document.class);
        assertEquals("1.2.0", consumers.getString("spring-smoke"));
    }

    private static int pack(
            final Path inputDir,
            final Path outputDir,
            final String fixtureVersion,
            final Path previousManifest) {
        final java.util.List<String> args = new java.util.ArrayList<>();
        args.add("--input-dir=" + inputDir);
        args.add("--output-dir=" + outputDir);
        args.add("--fixture-version=" + fixtureVersion);
        if (previousManifest != null) {
            args.add("--previous-manifest=" + previousManifest);
        }
        return FixtureArtifactTool.run(
                args.toArray(new String[0]),
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
    }
}
