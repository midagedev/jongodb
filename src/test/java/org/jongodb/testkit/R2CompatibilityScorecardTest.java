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

class R2CompatibilityScorecardTest {
    @Test
    void runAndWriteGeneratesScorecardAndSupportManifest() throws IOException {
        final Path root = Files.createTempDirectory("r2-scorecard");
        final Path utfArtifact = root.resolve("utf.json");
        final Path springArtifact = root.resolve("spring.json");
        final Path outputDir = root.resolve("out");

        writeJson(
                utfArtifact,
                "{\"generatedAt\":\"2026-02-23T13:00:00Z\",\"importSummary\":{\"imported\":120,\"skipped\":10,\"unsupported\":8},"
                        + "\"differentialSummary\":{\"total\":120,\"match\":120,\"mismatch\":0,\"error\":0}}");
        writeJson(
                springArtifact,
                "{\"generatedAt\":\"2026-02-23T13:01:00Z\",\"summary\":{\"pass\":196,\"fail\":4}}");

        final R2CompatibilityScorecard scorecard = new R2CompatibilityScorecard(
                Clock.fixed(Instant.parse("2026-02-23T13:10:00Z"), ZoneOffset.UTC));
        final R2CompatibilityScorecard.RunConfig config = new R2CompatibilityScorecard.RunConfig(
                outputDir,
                utfArtifact,
                springArtifact,
                false);

        final R2CompatibilityScorecard.Result result = scorecard.runAndWrite(config);
        assertTrue(result.overallPassed());
        assertEquals(2, result.passCount());
        assertEquals(0, result.failCount());
        assertEquals(0, result.missingCount());

        final R2CompatibilityScorecard.ArtifactPaths paths = R2CompatibilityScorecard.artifactPaths(outputDir);
        assertTrue(Files.exists(paths.scorecardJson()));
        assertTrue(Files.exists(paths.scorecardMarkdown()));
        assertTrue(Files.exists(paths.supportManifestJson()));

        final Document scorecardJson = Document.parse(Files.readString(paths.scorecardJson()));
        assertEquals("PASS", scorecardJson.getString("overallStatus"));
        assertEquals(2, scorecardJson.get("summary", Document.class).getInteger("pass"));
        assertTrue(scorecardJson.get("supportManifestSummary", Document.class).getInteger("supported") > 0);

        final Document manifestJson = Document.parse(Files.readString(paths.supportManifestJson()));
        assertTrue(manifestJson.get("summary", Document.class).getInteger("supported") > 0);
        assertTrue(manifestJson.getList("features", Document.class).size() >= 10);
    }

    @Test
    void missingUtfArtifactFailsGateDeterministically() throws IOException {
        final Path root = Files.createTempDirectory("r2-scorecard-missing");
        final Path springArtifact = root.resolve("spring.json");
        writeJson(
                springArtifact,
                "{\"generatedAt\":\"2026-02-23T14:01:00Z\",\"summary\":{\"pass\":50,\"fail\":0}}");

        final R2CompatibilityScorecard scorecard = new R2CompatibilityScorecard(
                Clock.fixed(Instant.parse("2026-02-23T14:10:00Z"), ZoneOffset.UTC));
        final R2CompatibilityScorecard.Result result = scorecard.run(new R2CompatibilityScorecard.RunConfig(
                root.resolve("out"),
                root.resolve("missing-utf.json"),
                springArtifact,
                false));

        assertFalse(result.overallPassed());
        assertEquals(1, result.passCount());
        assertEquals(0, result.failCount());
        assertEquals(1, result.missingCount());
    }

    private static void writeJson(final Path path, final String content) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8);
    }
}
