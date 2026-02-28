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

class FixtureArtifactToolTest {
    @Test
    void packsPortableAndFastArtifactsWithManifest(@TempDir final Path tempDir) throws Exception {
        final Path inputDir = tempDir.resolve("fixture-ndjson");
        final Path outputDir = tempDir.resolve("artifact");
        Files.createDirectories(inputDir);

        Files.writeString(
                inputDir.resolve("app.users.ndjson"),
                "{\"_id\":1,\"name\":\"alpha\"}\n{\"_id\":2,\"name\":\"beta\"}\n",
                StandardCharsets.UTF_8);
        Files.writeString(
                inputDir.resolve("app.orders.ndjson"),
                "{\"_id\":101,\"amount\":10}\n",
                StandardCharsets.UTF_8);

        final int exitCode = FixtureArtifactTool.run(
                new String[] {
                    "--input-dir=" + inputDir,
                    "--output-dir=" + outputDir,
                    "--engine-version=test-engine-v1"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));

        assertEquals(0, exitCode);
        assertTrue(Files.exists(outputDir.resolve("fixture-artifact-manifest.json")));
        assertTrue(Files.exists(outputDir.resolve("fixture-portable.ejsonl.gz")));
        assertTrue(Files.exists(outputDir.resolve("fixture-fast-snapshot.bin")));

        final Document manifest = Document.parse(Files.readString(
                outputDir.resolve("fixture-artifact-manifest.json"),
                StandardCharsets.UTF_8));
        assertEquals("fixture-artifact.v1", manifest.getString("schemaVersion"));
        assertEquals("test-engine-v1", manifest.getString("engineVersion"));
        assertEquals("fast-snapshot.v1", manifest.getString("fastFormatVersion"));

        final Document totals = manifest.get("totals", Document.class);
        assertEquals(2, totals.getInteger("collections"));
        assertEquals(3, totals.getInteger("documents"));

        final Document portable = manifest.get("portable", Document.class);
        final Document fast = manifest.get("fast", Document.class);
        assertEquals(64, portable.getString("sha256").length());
        assertEquals(64, fast.getString("sha256").length());
    }
}
