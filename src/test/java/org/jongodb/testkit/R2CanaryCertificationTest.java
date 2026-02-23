package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class R2CanaryCertificationTest {
    @Test
    void runAndWritePassesWhenThreeProjectsAndRollbackSucceed() throws IOException {
        final Path root = Files.createTempDirectory("r2-canary-pass");
        final Path inputJson = root.resolve("projects.json");
        final Path outputDir = root.resolve("out");

        writeJson(
                inputJson,
                """
                {
                  "projects": [
                    {
                      "projectId": "petclinic",
                      "repository": "https://example/petclinic",
                      "revision": "abc123",
                      "canaryPassed": true,
                      "rollback": {"attempted": true, "success": true, "recoverySeconds": 42}
                    },
                    {
                      "projectId": "commerce",
                      "repository": "https://example/commerce",
                      "revision": "def456",
                      "canaryPassed": true,
                      "rollback": {"attempted": true, "success": true, "recoverySeconds": 55}
                    },
                    {
                      "projectId": "internal-orders",
                      "repository": "https://example/orders",
                      "revision": "ghi789",
                      "canaryPassed": true,
                      "rollback": {"attempted": true, "success": true, "recoverySeconds": 38}
                    }
                  ]
                }
                """);

        final R2CanaryCertification certification = new R2CanaryCertification(
                Clock.fixed(Instant.parse("2026-02-23T15:00:00Z"), ZoneOffset.UTC));
        final R2CanaryCertification.Result result = certification.runAndWrite(new R2CanaryCertification.RunConfig(
                inputJson,
                outputDir,
                false));

        assertTrue(result.passed());
        assertEquals(3, result.projectCount());
        assertEquals(3, result.canaryPassCount());
        assertEquals(3, result.rollbackSuccessCount());

        final R2CanaryCertification.ArtifactPaths paths = R2CanaryCertification.artifactPaths(outputDir);
        assertTrue(Files.exists(paths.jsonArtifact()));
        assertTrue(Files.exists(paths.markdownArtifact()));

        final Document json = Document.parse(Files.readString(paths.jsonArtifact()));
        assertEquals("PASS", json.getString("overallStatus"));
        assertEquals(3, json.get("metrics", Document.class).getInteger("projectCount"));
    }

    @Test
    void failsWhenProjectCountIsInsufficientOrRollbackFails() throws IOException {
        final Path root = Files.createTempDirectory("r2-canary-fail");
        final Path inputJson = root.resolve("projects.json");
        writeJson(
                inputJson,
                """
                {
                  "projects": [
                    {
                      "projectId": "petclinic",
                      "repository": "https://example/petclinic",
                      "revision": "abc123",
                      "canaryPassed": false,
                      "rollback": {"attempted": true, "success": false, "recoverySeconds": 70}
                    },
                    {
                      "projectId": "commerce",
                      "repository": "https://example/commerce",
                      "revision": "def456",
                      "canaryPassed": true,
                      "rollback": {"attempted": true, "success": true, "recoverySeconds": 55}
                    }
                  ]
                }
                """);

        final R2CanaryCertification certification = new R2CanaryCertification(
                Clock.fixed(Instant.parse("2026-02-23T15:10:00Z"), ZoneOffset.UTC));
        final R2CanaryCertification.Result result = certification.run(new R2CanaryCertification.RunConfig(
                inputJson,
                root.resolve("out"),
                false));

        assertFalse(result.passed());
        assertEquals(2, result.projectCount());
        assertTrue(result.diagnostics().stream().anyMatch(diagnostic -> diagnostic.contains("at least 3 projects")));
        assertTrue(result.diagnostics().stream().anyMatch(diagnostic -> diagnostic.contains("canary failures")));
        assertTrue(result.diagnostics().stream().anyMatch(diagnostic -> diagnostic.contains("rollback rehearsal")));
    }

    private static void writeJson(final Path path, final String content) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8);
    }
}
