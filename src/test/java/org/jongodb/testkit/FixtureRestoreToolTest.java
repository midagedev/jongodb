package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.bson.Document;
import org.jongodb.server.TcpMongoServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixtureRestoreToolTest {
    @Test
    void restoreReplaceReplacesTargetCollection(@TempDir final Path tempDir) throws Exception {
        final Path fixtureDir = tempDir.resolve("fixture");
        Files.createDirectories(fixtureDir);
        Files.writeString(
                fixtureDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"alpha"}
                {"_id":2,"name":"beta"}
                """,
                StandardCharsets.UTF_8);

        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            final String uri = server.connectionString("app");
            try (MongoClient client = MongoClients.create(uri)) {
                final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
                users.insertOne(new Document("_id", 99).append("name", "legacy"));
            }

            final int exitCode = FixtureRestoreTool.run(
                    new String[] {
                        "--input-dir=" + fixtureDir,
                        "--mongo-uri=" + uri,
                        "--mode=replace"
                    },
                    new PrintStream(new ByteArrayOutputStream()),
                    new PrintStream(new ByteArrayOutputStream()));
            assertEquals(0, exitCode);

            try (MongoClient client = MongoClients.create(uri)) {
                final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
                assertEquals(2, users.countDocuments());
                assertEquals("alpha", users.find(new Document("_id", 1)).first().getString("name"));
                assertEquals("beta", users.find(new Document("_id", 2)).first().getString("name"));
            }
        }
    }

    @Test
    void restoreMergeUpsertsWithoutDeletingExistingDocuments(@TempDir final Path tempDir) throws Exception {
        final Path fixtureDir = tempDir.resolve("fixture");
        Files.createDirectories(fixtureDir);
        Files.writeString(
                fixtureDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"merged"}
                {"_id":4,"name":"new"}
                """,
                StandardCharsets.UTF_8);

        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            final String uri = server.connectionString("app");
            try (MongoClient client = MongoClients.create(uri)) {
                final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
                users.insertOne(new Document("_id", 1).append("name", "old"));
                users.insertOne(new Document("_id", 3).append("name", "keep"));
            }

            final int exitCode = FixtureRestoreTool.run(
                    new String[] {
                        "--input-dir=" + fixtureDir,
                        "--mongo-uri=" + uri,
                        "--mode=merge"
                    },
                    new PrintStream(new ByteArrayOutputStream()),
                    new PrintStream(new ByteArrayOutputStream()));
            assertEquals(0, exitCode);

            try (MongoClient client = MongoClients.create(uri)) {
                final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
                assertEquals(3, users.countDocuments());
                assertEquals("merged", users.find(new Document("_id", 1)).first().getString("name"));
                assertEquals("keep", users.find(new Document("_id", 3)).first().getString("name"));
                assertEquals("new", users.find(new Document("_id", 4)).first().getString("name"));
            }

            assertTrue(Files.exists(fixtureDir.resolve("fixture-restore-report.json")));
        }
    }

    @Test
    void restoreUsesFastArtifactWhenCompatible(@TempDir final Path tempDir) throws Exception {
        final Path fixtureDir = tempDir.resolve("fixture");
        final Path artifactDir = tempDir.resolve("artifact");
        Files.createDirectories(fixtureDir);
        Files.writeString(
                fixtureDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"alpha"}
                {"_id":2,"name":"beta"}
                """,
                StandardCharsets.UTF_8);

        final int packExit = FixtureArtifactTool.run(
                new String[] {
                    "--input-dir=" + fixtureDir,
                    "--output-dir=" + artifactDir,
                    "--engine-version=" + FixtureArtifactBundle.currentEngineVersion()
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
        assertEquals(0, packExit);

        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            final String uri = server.connectionString("app");
            final int exitCode = FixtureRestoreTool.run(
                    new String[] {
                        "--input-dir=" + artifactDir,
                        "--mongo-uri=" + uri,
                        "--mode=replace"
                    },
                    new PrintStream(new ByteArrayOutputStream()),
                    new PrintStream(new ByteArrayOutputStream()));
            assertEquals(0, exitCode);

            try (MongoClient client = MongoClients.create(uri)) {
                final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
                assertEquals(2, users.countDocuments());
            }

            final Document report = Document.parse(Files.readString(
                    artifactDir.resolve("fixture-restore-report.json"),
                    StandardCharsets.UTF_8));
            assertEquals("FAST", report.getString("sourceFormat"));
        }
    }

    @Test
    void restoreFallsBackToPortableAndRegeneratesFastCache(@TempDir final Path tempDir) throws Exception {
        final Path fixtureDir = tempDir.resolve("fixture");
        final Path artifactDir = tempDir.resolve("artifact");
        Files.createDirectories(fixtureDir);
        Files.writeString(
                fixtureDir.resolve("app.users.ndjson"),
                """
                {"_id":1,"name":"portable"}
                {"_id":2,"name":"fallback"}
                """,
                StandardCharsets.UTF_8);

        final int packExit = FixtureArtifactTool.run(
                new String[] {
                    "--input-dir=" + fixtureDir,
                    "--output-dir=" + artifactDir,
                    "--engine-version=legacy-engine"
                },
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
        assertEquals(0, packExit);

        Files.delete(artifactDir.resolve("fixture-fast-snapshot.bin"));

        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            final String uri = server.connectionString("app");
            final int exitCode = FixtureRestoreTool.run(
                    new String[] {
                        "--input-dir=" + artifactDir,
                        "--mongo-uri=" + uri,
                        "--mode=replace"
                    },
                    new PrintStream(new ByteArrayOutputStream()),
                    new PrintStream(new ByteArrayOutputStream()));
            assertEquals(0, exitCode);

            try (MongoClient client = MongoClients.create(uri)) {
                final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
                assertEquals(2, users.countDocuments());
            }

            final Document report = Document.parse(Files.readString(
                    artifactDir.resolve("fixture-restore-report.json"),
                    StandardCharsets.UTF_8));
            assertEquals("PORTABLE_FALLBACK", report.getString("sourceFormat"));

            final Document manifest = Document.parse(Files.readString(
                    artifactDir.resolve("fixture-artifact-manifest.json"),
                    StandardCharsets.UTF_8));
            assertEquals(FixtureArtifactBundle.currentEngineVersion(), manifest.getString("engineVersion"));
            assertTrue(Files.exists(artifactDir.resolve("fixture-fast-snapshot.bin")));
        }
    }
}
