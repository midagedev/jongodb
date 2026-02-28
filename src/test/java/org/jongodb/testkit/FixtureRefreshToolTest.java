package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixtureRefreshToolTest {
    @Test
    void fullRefreshWritesCandidateSnapshotAndReport(@TempDir final Path tempDir) throws Exception {
        final Path baselineDir = tempDir.resolve("baseline");
        final Path candidateDir = tempDir.resolve("candidate");
        final Path outputDir = tempDir.resolve("out");
        Files.createDirectories(baselineDir);
        Files.createDirectories(candidateDir);

        Files.writeString(
                baselineDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"alpha"}
                {"_id":2,"name":"beta"}
                """,
                StandardCharsets.UTF_8);
        Files.writeString(
                candidateDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"alpha"}
                {"_id":2,"name":"beta-updated"}
                {"_id":3,"name":"gamma"}
                """,
                StandardCharsets.UTF_8);

        final int exitCode = FixtureRefreshTool.run(
                new String[] {
                    "--baseline-dir=" + baselineDir,
                    "--candidate-dir=" + candidateDir,
                    "--output-dir=" + outputDir,
                    "--mode=full"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));

        assertEquals(0, exitCode);

        final Path refreshed = outputDir.resolve("refreshed/app.users.ndjson");
        assertTrue(Files.exists(refreshed));
        assertEquals(3, Files.readAllLines(refreshed).size());

        final Document report = Document.parse(Files.readString(
                outputDir.resolve("fixture-refresh-report.json"),
                StandardCharsets.UTF_8));
        assertEquals("full", report.getString("mode"));
        assertEquals(1, report.getInteger("changedCollections"));
        assertEquals(false, report.getBoolean("requiresApproval"));
    }

    @Test
    void incrementalRefreshWritesOnlyDeltaDocuments(@TempDir final Path tempDir) throws Exception {
        final Path baselineDir = tempDir.resolve("baseline");
        final Path candidateDir = tempDir.resolve("candidate");
        final Path outputDir = tempDir.resolve("out");
        Files.createDirectories(baselineDir);
        Files.createDirectories(candidateDir);

        Files.writeString(
                baselineDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"same"}
                {"_id":2,"name":"before"}
                """,
                StandardCharsets.UTF_8);
        Files.writeString(
                candidateDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"same"}
                {"_id":2,"name":"after"}
                {"_id":3,"name":"new"}
                """,
                StandardCharsets.UTF_8);

        final int exitCode = FixtureRefreshTool.run(
                new String[] {
                    "--baseline-dir=" + baselineDir,
                    "--candidate-dir=" + candidateDir,
                    "--output-dir=" + outputDir,
                    "--mode=incremental"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));

        assertEquals(0, exitCode);

        final List<String> lines = Files.readAllLines(outputDir.resolve("refreshed/app.users.ndjson"));
        assertEquals(2, lines.size());
        final String merged = String.join("\n", lines);
        assertTrue(merged.contains("\"_id\":2"));
        assertTrue(merged.contains("\"_id\":3"));
        assertTrue(!merged.contains("\"_id\":1"));
    }

    @Test
    void requiresApprovalWhenBreakingChangeDetected(@TempDir final Path tempDir) throws Exception {
        final Path baselineDir = tempDir.resolve("baseline");
        final Path candidateDir = tempDir.resolve("candidate");
        final Path outputDir = tempDir.resolve("out");
        Files.createDirectories(baselineDir);
        Files.createDirectories(candidateDir);

        Files.writeString(
                baselineDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"alpha"}
                {"_id":2,"name":"beta"}
                """,
                StandardCharsets.UTF_8);
        Files.writeString(
                candidateDir.resolve("app.users.ndjson"),
                "{" + "\"_id\":1,\"name\":\"alpha\"}" + "\n",
                StandardCharsets.UTF_8);

        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final int exitCode = FixtureRefreshTool.run(
                new String[] {
                    "--baseline-dir=" + baselineDir,
                    "--candidate-dir=" + candidateDir,
                    "--output-dir=" + outputDir,
                    "--mode=full",
                    "--require-approval"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(errBytes));

        assertEquals(1, exitCode);
        assertTrue(errBytes.toString().contains("--approved"));

        final int approvedExit = FixtureRefreshTool.run(
                new String[] {
                    "--baseline-dir=" + baselineDir,
                    "--candidate-dir=" + candidateDir,
                    "--output-dir=" + outputDir,
                    "--mode=full",
                    "--require-approval",
                    "--approved"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
        assertEquals(0, approvedExit);
    }
}
