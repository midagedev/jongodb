package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixtureSanitizationToolTest {
    @Test
    void sanitizesDeterministicallyAndRemovesSensitivePatterns(@TempDir final Path tempDir) throws Exception {
        final Path inputDir = tempDir.resolve("input");
        final Path outputA = tempDir.resolve("out-a");
        final Path outputB = tempDir.resolve("out-b");
        final Path policyFile = tempDir.resolve("policy.json");
        Files.createDirectories(inputDir);

        Files.writeString(
                inputDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"pii":{"email":"alice@example.com","phone":"+1-212-555-0100","ssn":"123-45-6789"},"credentials":{"passwordHash":"abc"},"updatedAt":"2026-02-28T00:00:00Z"}
                {"_id":2,"pii":{"email":"alice@example.com","phone":"+1-212-555-0100","ssn":"123-45-6789"},"credentials":{"passwordHash":"def"},"updatedAt":"2026-02-28T01:00:00Z"}
                """,
                StandardCharsets.UTF_8);
        Files.writeString(
                policyFile,
                """
                {
                  "rules": [
                    {"path":"pii.email","action":"fake","fakeType":"email"},
                    {"path":"pii.phone","action":"tokenize"},
                    {"path":"pii.ssn","action":"nullify"},
                    {"path":"credentials.passwordHash","action":"drop"}
                  ],
                  "volatileFields": ["updatedAt"]
                }
                """,
                StandardCharsets.UTF_8);

        final int firstExit = FixtureSanitizationTool.run(
                new String[] {
                    "--input-dir=" + inputDir,
                    "--output-dir=" + outputA,
                    "--policy-file=" + policyFile,
                    "--seed=test-seed"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
        final int secondExit = FixtureSanitizationTool.run(
                new String[] {
                    "--input-dir=" + inputDir,
                    "--output-dir=" + outputB,
                    "--policy-file=" + policyFile,
                    "--seed=test-seed"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));

        assertEquals(0, firstExit);
        assertEquals(0, secondExit);

        final String firstOutput = Files.readString(outputA.resolve("app.users.ndjson"));
        final String secondOutput = Files.readString(outputB.resolve("app.users.ndjson"));
        assertEquals(firstOutput, secondOutput);
        assertTrue(!firstOutput.contains("alice@example.com"));
        assertTrue(!firstOutput.contains("123-45-6789"));
        assertTrue(!firstOutput.contains("passwordHash"));
    }

    @Test
    void failsWhenPiiPatternsRemain(@TempDir final Path tempDir) throws Exception {
        final Path inputDir = tempDir.resolve("input");
        final Path outputDir = tempDir.resolve("out");
        final Path policyFile = tempDir.resolve("policy.json");
        Files.createDirectories(inputDir);

        Files.writeString(
                inputDir.resolve("app.users.ndjson"),
                "{\"_id\":1,\"email\":\"plain@example.com\"}\n",
                StandardCharsets.UTF_8);
        Files.writeString(policyFile, "{\"rules\":[],\"volatileFields\":[]}\n", StandardCharsets.UTF_8);

        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final int exitCode = FixtureSanitizationTool.run(
                new String[] {
                    "--input-dir=" + inputDir,
                    "--output-dir=" + outputDir,
                    "--policy-file=" + policyFile
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(errBytes));

        assertEquals(1, exitCode);
        assertTrue(errBytes.toString().contains("PII lint failed"));
    }
}
