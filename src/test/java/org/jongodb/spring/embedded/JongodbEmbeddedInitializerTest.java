package org.jongodb.spring.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoServerException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.jongodb.server.TcpMongoServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

final class JongodbEmbeddedInitializerTest {
    @TempDir
    Path tempDir;

    @Test
    void namedSnapshotsRestoreAndExportWritesMongodumpLayout() {
        final Path dump = tempDir.resolve("dump");
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs0")) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoDatabase db = client.getDatabase("app");
                final MongoDatabase admin = client.getDatabase("admin");
                db.getCollection("Account").createIndex(Indexes.ascending("email"), new IndexOptions().unique(true));
                db.getCollection("Account").insertOne(new Document("_id", 1).append("email", "a@x"));

                admin.runCommand(new Document("jongodbState", "save").append("name", "seeded"));
                db.getCollection("Account").insertOne(new Document("_id", 2).append("email", "b@x"));
                db.getCollection("Scratch").insertOne(new Document("_id", 1));
                admin.runCommand(new Document("jongodbState", "restore").append("name", "seeded"));

                assertEquals(1, db.getCollection("Account").countDocuments());
                assertFalse(db.listCollectionNames().into(new java.util.ArrayList<>()).contains("Scratch"));
                assertEquals(11000, assertThrows(MongoServerException.class, () -> db.getCollection("Account")
                        .insertOne(new Document("_id", 3).append("email", "a@x"))).getCode());
                assertEquals(List.of("seeded"), admin.runCommand(new Document("jongodbState", "list")).getList("snapshots", String.class));
                assertThrows(MongoCommandException.class,
                        () -> admin.runCommand(new Document("jongodbState", "restore").append("name", "missing")));

                admin.runCommand(new Document("jongodbState", "export").append("path", dump.toString()));
            }
        }

        assertTrue(Files.isRegularFile(dump.resolve("app/Account.bson")));
        final Document metadata = Document.parse(readString(dump.resolve("app/Account.metadata.json")));
        final List<Document> indexes = metadata.getList("indexes", Document.class);
        assertEquals(List.of("_id_", "email_1"), indexes.stream().map(index -> index.getString("name")).toList());
        assertFalse(indexes.get(0).containsKey("unique"));
        assertTrue(indexes.get(1).getBoolean("unique"));
    }

    @Test
    void initializerLoadsPersistedStateBeforeBeansAndWritesItBackOnClose() {
        final Path state = tempDir.resolve("world");
        try (AnnotationConfigApplicationContext context = context(state)) {
            final String uri = context.getEnvironment().getProperty("spring.data.mongodb.uri");
            assertEquals(uri.replace("/app?", "/other?"), context.getEnvironment().getProperty("OTHER_URI"));
            try (MongoClient client = MongoClients.create(uri)) {
                client.getDatabase("app").getCollection("Account").insertOne(new Document("_id", 1));
            }
        }
        assertTrue(Files.isRegularFile(state.resolve("app/Account.bson")));

        try (AnnotationConfigApplicationContext context = context(state);
                MongoClient client = MongoClients.create(context.getEnvironment().getProperty("spring.data.mongodb.uri"))) {
            assertEquals(1, client.getDatabase("app").getCollection("Account").countDocuments());
        }
    }

    private static AnnotationConfigApplicationContext context(final Path state) {
        final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("props", Map.of(
                "jongodb.embedded.port", "0",
                "jongodb.embedded.persist-path", state.toString(),
                "jongodb.embedded.uri-properties", "spring.data.mongodb.uri:app, OTHER_URI:other")));
        new JongodbEmbeddedInitializer().initialize(context);
        context.refresh();
        return context;
    }

    private static String readString(final Path file) {
        try {
            return Files.readString(file);
        } catch (final java.io.IOException exception) {
            throw new java.io.UncheckedIOException(exception);
        }
    }
}
