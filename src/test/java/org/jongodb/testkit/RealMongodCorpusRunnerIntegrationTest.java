package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

class RealMongodCorpusRunnerIntegrationTest {
    private static final String REAL_MONGOD_URI_ENV = "JONGODB_REAL_MONGOD_URI";

    @Test
    void runsAgainstRealMongodWhenEnvVarIsSet() throws IOException {
        String mongoUri = System.getenv(REAL_MONGOD_URI_ENV);
        Assumptions.assumeTrue(mongoUri != null && !mongoUri.isBlank(), REAL_MONGOD_URI_ENV + " is not set");

        Path outputDir = Files.createTempDirectory("real-mongod-corpus-it");
        RealMongodCorpusRunner runner = new RealMongodCorpusRunner(
            Clock.fixed(Instant.parse("2026-02-23T13:30:00Z"), ZoneOffset.UTC)
        );
        RealMongodCorpusRunner.RunConfig config = new RealMongodCorpusRunner.RunConfig(
            outputDir,
            mongoUri,
            "integration-seed",
            5
        );

        RealMongodCorpusRunner.RunResult result = runner.runAndWrite(config);
        RealMongodCorpusRunner.ArtifactPaths paths = RealMongodCorpusRunner.artifactPaths(outputDir);

        assertTrue(Files.exists(paths.jsonArtifact()));
        assertTrue(Files.exists(paths.markdownArtifact()));
        assertEquals(
            CrudScenarioCatalog.scenarios().size() + TransactionScenarioCatalog.scenarios().size(),
            result.report().totalScenarios()
        );

        String json = Files.readString(paths.jsonArtifact());
        String markdown = Files.readString(paths.markdownArtifact());
        assertTrue(json.contains("\"seed\":\"integration-seed\""));
        assertTrue(markdown.contains("# Real Mongod Differential Baseline"));
    }
}
