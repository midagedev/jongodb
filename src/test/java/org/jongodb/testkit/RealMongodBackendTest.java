package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class RealMongodBackendTest {
    @Test
    void scenarioDatabaseNameNormalizesDeterministically() {
        String dbName = RealMongodBackend.scenarioDatabaseName("testkit", "txn.start-commit-path");
        assertEquals("testkit_txn_start_commit_path", dbName);
    }

    @Test
    void executeReturnsFailureWhenClientFactoryThrows() {
        RealMongodBackend backend = new RealMongodBackend(
            "real-mongod",
            "mongodb://localhost:27017",
            "testkit",
            uri -> {
                throw new IllegalStateException("boom");
            }
        );
        Scenario scenario = new Scenario(
            "crud.ping",
            "single ping command",
            List.of(new ScenarioCommand("ping", Map.of()))
        );

        ScenarioOutcome outcome = backend.execute(scenario);

        assertFalse(outcome.success());
        assertTrue(outcome.errorMessage().orElse("").contains("boom"));
    }

    @Test
    void commandDocumentUsesCollectionAsCommandValueAndInjectsDatabase() {
        ScenarioCommand command = new ScenarioCommand(
            "insert",
            Map.of(
                "collection",
                "users",
                "documents",
                List.of(Map.of("_id", 1, "name", "alpha"))
            )
        );

        BsonDocument commandDocument = ScenarioBsonCodec.toCommandDocument(command, "testkit_custom");

        assertEquals("users", commandDocument.getString("insert").getValue());
        assertEquals("testkit_custom", commandDocument.getString("$db").getValue());
        assertTrue(commandDocument.containsKey("documents"));
    }

    @Test
    void constructorRejectsInvalidConnectionUri() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new RealMongodBackend("real-mongod", "localhost:27017")
        );
    }
}
