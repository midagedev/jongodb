package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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

    @Test
    void executeSanitizesCommandBeforeRealRunCommand() {
        AtomicReference<BsonDocument> capturedCommand = new AtomicReference<>();
        MongoDatabase database = mongoDatabaseProxy(capturedCommand);
        MongoClient client = mongoClientProxy(database);

        RealMongodBackend backend = new RealMongodBackend(
            "real-mongod",
            "mongodb://localhost:27017",
            "testkit",
            uri -> client
        );

        Scenario scenario = new Scenario(
            "txn.sanitize-command",
            "sanitization path",
            List.of(
                new ScenarioCommand(
                    "insert",
                    Map.of(
                        "collection",
                        "users",
                        "$db",
                        "override-db",
                        "documents",
                        List.of(Map.of("_id", 1)),
                        "lsid",
                        Map.of("id", "session-for-real"),
                        "txnNumber",
                        1,
                        "autocommit",
                        false,
                        "startTransaction",
                        true
                    )
                )
            )
        );

        ScenarioOutcome outcome = backend.execute(scenario);

        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));
        BsonDocument sent = capturedCommand.get();
        assertNotNull(sent);
        assertFalse(sent.containsKey("$db"));
        assertFalse(sent.containsKey("lsid"));
        assertTrue(sent.get("txnNumber").isInt64());
        assertEquals(1L, sent.getInt64("txnNumber").longValue());
    }

    @Test
    void executeRoutesCommitTransactionCommandToAdminDatabase() {
        AtomicReference<BsonDocument> capturedCommand = new AtomicReference<>();
        AtomicReference<String> runDatabaseName = new AtomicReference<>();
        MongoDatabase scenarioDatabase = mongoDatabaseProxy(capturedCommand, "scenario", runDatabaseName);
        MongoDatabase adminDatabase = mongoDatabaseProxy(capturedCommand, "admin", runDatabaseName);
        MongoClient client = (MongoClient) Proxy.newProxyInstance(
            RealMongodBackendTest.class.getClassLoader(),
            new Class<?>[] {MongoClient.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "getDatabase" -> "admin".equals(args[0]) ? adminDatabase : scenarioDatabase;
                case "startSession" -> clientSessionProxy();
                case "close" -> null;
                case "toString" -> "MongoClientProxy";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException("Unsupported MongoClient method: " + method.getName());
            }
        );

        RealMongodBackend backend = new RealMongodBackend(
            "real-mongod",
            "mongodb://localhost:27017",
            "testkit",
            uri -> client
        );
        Scenario scenario = new Scenario(
            "txn.commit-route",
            "commit command should target admin DB",
            List.of(
                new ScenarioCommand(
                    "commitTransaction",
                    Map.of(
                        "lsid",
                        Map.of("id", "session-commit"),
                        "txnNumber",
                        1,
                        "autocommit",
                        false
                    )
                )
            )
        );

        ScenarioOutcome outcome = backend.execute(scenario);

        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));
        assertEquals("admin", runDatabaseName.get());
    }

    @Test
    void normalizeResponseForComparisonSortsDistinctValuesDeterministically() throws Exception {
        final Method method = RealMongodBackend.class.getDeclaredMethod(
                "normalizeResponseForComparison",
                ScenarioCommand.class,
                BsonDocument.class);
        method.setAccessible(true);

        final ScenarioCommand command = new ScenarioCommand("distinct", Map.of("distinct", "users", "key", "v"));
        final BsonDocument response = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(2),
                        new BsonInt32(1),
                        BsonDocument.parse("{\"a\":1}"))))
                .append("ok", new BsonInt32(1));

        final BsonDocument normalized = (BsonDocument) method.invoke(null, command, response);
        final BsonArray values = normalized.getArray("values");
        assertEquals(3, values.size());
        assertEquals(1, values.get(0).asInt32().getValue());
        assertEquals(2, values.get(1).asInt32().getValue());
        assertTrue(values.get(2).isDocument());
    }

    private static MongoClient mongoClientProxy(MongoDatabase database) {
        return (MongoClient) Proxy.newProxyInstance(
            RealMongodBackendTest.class.getClassLoader(),
            new Class<?>[] {MongoClient.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "getDatabase" -> database;
                case "startSession" -> clientSessionProxy();
                case "close" -> null;
                case "toString" -> "MongoClientProxy";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException("Unsupported MongoClient method: " + method.getName());
            }
        );
    }

    private static MongoDatabase mongoDatabaseProxy(AtomicReference<BsonDocument> capturedCommand) {
        return mongoDatabaseProxy(capturedCommand, "default", new AtomicReference<>());
    }

    private static MongoDatabase mongoDatabaseProxy(
        AtomicReference<BsonDocument> capturedCommand,
        String databaseName,
        AtomicReference<String> runDatabaseName
    ) {
        return (MongoDatabase) Proxy.newProxyInstance(
            RealMongodBackendTest.class.getClassLoader(),
            new Class<?>[] {MongoDatabase.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "drop" -> null;
                case "runCommand" -> {
                    BsonDocument commandDocument = null;
                    for (Object argument : args) {
                        if (argument instanceof BsonDocument bsonDocument) {
                            commandDocument = bsonDocument;
                            break;
                        }
                    }
                    if (commandDocument == null) {
                        throw new UnsupportedOperationException("Unsupported runCommand arguments");
                    }
                    capturedCommand.set(commandDocument.clone());
                    runDatabaseName.set(databaseName);
                    yield BsonDocument.parse("{\"ok\": 1}");
                }
                case "toString" -> "MongoDatabaseProxy";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException("Unsupported MongoDatabase method: " + method.getName());
            }
        );
    }

    private static ClientSession clientSessionProxy() {
        return (ClientSession) Proxy.newProxyInstance(
            RealMongodBackendTest.class.getClassLoader(),
            new Class<?>[] {ClientSession.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "close" -> null;
                case "toString" -> "ClientSessionProxy";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException("Unsupported ClientSession method: " + method.getName());
            }
        );
    }
}
