package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.bson.Document;
import org.jongodb.server.TcpMongoServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixtureExtractionToolTest {
    @Test
    void extractsFixtureFilesAndWritesCollectionStats(@TempDir final Path tempDir) throws Exception {
        final Path manifestPath = tempDir.resolve("manifest.json");
        Files.writeString(manifestPath, minimalManifestJson("prod-main"), java.nio.charset.StandardCharsets.UTF_8);
        final Path outputDir = tempDir.resolve("out");

        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                client.getDatabase("app").getCollection("users").insertMany(List.of(
                        new Document("_id", 1).append("name", "alpha"),
                        new Document("_id", 2).append("name", "beta")));
            }

            final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
            final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
            final int exitCode = FixtureExtractionTool.run(
                    new String[] {
                        "--manifest=" + manifestPath,
                        "--profile=dev",
                        "--output-dir=" + outputDir,
                        "--mongo-uri=" + server.connectionString("app"),
                        "--allow-uri-alias=prod-main",
                        "--readonly-check=off",
                        "--batch-size=2"
                    },
                    new PrintStream(outBytes),
                    new PrintStream(errBytes));

            assertEquals(0, exitCode, errBytes.toString());
            final Path extracted = outputDir.resolve("app.users.ndjson");
            assertTrue(Files.exists(extracted));
            assertEquals(2, Files.readAllLines(extracted).size());

            final Path report = outputDir.resolve("fixture-extraction-report.json");
            assertTrue(Files.exists(report));
            final Document reportDocument = Document.parse(Files.readString(report));
            @SuppressWarnings("unchecked")
            final List<Document> collections = (List<Document>) reportDocument.get("collections");
            assertEquals(1, collections.size());
            assertEquals("SUCCESS", collections.get(0).getString("status"));
            assertEquals(2, ((Number) collections.get(0).get("count")).intValue());
        }
    }

    @Test
    void rejectsSourceAliasOutsideAllowlist(@TempDir final Path tempDir) throws Exception {
        final Path manifestPath = tempDir.resolve("manifest.json");
        Files.writeString(manifestPath, minimalManifestJson("prod-main"), java.nio.charset.StandardCharsets.UTF_8);
        final Path outputDir = tempDir.resolve("out");

        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final int exitCode = FixtureExtractionTool.run(
                new String[] {
                    "--manifest=" + manifestPath,
                    "--profile=dev",
                    "--output-dir=" + outputDir,
                    "--mongo-uri=mongodb://localhost:27017",
                    "--allow-uri-alias=another-source",
                    "--readonly-check=off"
                },
                new PrintStream(outBytes),
                new PrintStream(errBytes));

        assertEquals(1, exitCode);
        assertTrue(errBytes.toString().contains("not in allowlist"));
    }

    private static String minimalManifestJson(final String alias) {
        return """
                {
                  "schemaVersion": "fixture-manifest.v1",
                  "source": { "uriAlias": "%s" },
                  "profiles": {
                    "dev": {
                      "refreshMode": "full",
                      "collections": [
                        {
                          "database": "app",
                          "collection": "users",
                          "sort": { "_id": 1 }
                        }
                      ]
                    },
                    "smoke": {
                      "refreshMode": "full",
                      "collections": [
                        {
                          "database": "app",
                          "collection": "users",
                          "sort": { "_id": 1 }
                        }
                      ]
                    },
                    "full": {
                      "refreshMode": "incremental",
                      "collections": [
                        {
                          "database": "app",
                          "collection": "users",
                          "sort": { "_id": 1 }
                        }
                      ]
                    }
                  }
                }
                """.formatted(alias);
    }
}
