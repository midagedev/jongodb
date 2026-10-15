package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.MongoNamespace;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

final class AdminCommandsTcpTest {
    @Test
    void driverAdminOperationsRoundTripThroughReplicaSetProfile() {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs-test")) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoDatabase app = client.getDatabase("app");

                app.createCollection("made");
                app.getCollection("c").insertOne(new Document("_id", 1).append("a", 1));
                app.getCollection("c").createIndex(Indexes.ascending("a"));
                final List<String> databases = client.listDatabaseNames().into(new ArrayList<>());
                assertEquals(List.of("app"), databases);

                final MongoCommandException exists =
                        org.junit.jupiter.api.Assertions.assertThrows(MongoCommandException.class, () -> app.createCollection("made"));
                assertEquals(48, exists.getErrorCode());

                app.getCollection("c").dropIndex("a_1");
                app.getCollection("c").renameCollection(new MongoNamespace("app", "renamed"));
                assertEquals(List.of("made", "renamed"), app.listCollectionNames().into(new ArrayList<>()));
                assertEquals(1, app.getCollection("renamed").countDocuments());

                final Document explain = app.runCommand(new Document(
                        "explain", new Document("find", "renamed").append("filter", new Document("a", 1))).append("verbosity", "executionStats"));
                assertEquals("COLLSCAN", ((Document) ((Document) explain.get("queryPlanner")).get("winningPlan")).getString("stage"));
                assertEquals(1, ((Document) explain.get("executionStats")).getInteger("nReturned"));

                final Document replStatus = client.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
                assertEquals("PRIMARY", ((Document) ((List<?>) replStatus.get("members")).get(0)).getString("stateStr"));
                assertTrue(client.getDatabase("admin").runCommand(new Document("serverStatus", 1)).containsKey("uptime"));
            }
        }
    }
}
