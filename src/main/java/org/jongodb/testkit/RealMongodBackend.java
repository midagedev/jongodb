package org.jongodb.testkit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;

/**
 * Differential backend adapter that executes scenario commands against real mongod.
 */
public final class RealMongodBackend implements DifferentialBackend {
    private static final String DEFAULT_DATABASE_PREFIX = "testkit";
    private static final String ADMIN_DATABASE_NAME = "admin";

    private final String name;
    private final String connectionUri;
    private final String databasePrefix;
    private final MongoClientFactory clientFactory;

    public RealMongodBackend(String name, String connectionUri) {
        this(name, connectionUri, DEFAULT_DATABASE_PREFIX, MongoClients::create);
    }

    RealMongodBackend(
        String name,
        String connectionUri,
        String databasePrefix,
        MongoClientFactory clientFactory
    ) {
        this.name = requireText(name, "name");
        this.connectionUri = requireText(connectionUri, "connectionUri");
        this.databasePrefix = requireText(databasePrefix, "databasePrefix");
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
        validateConnectionUri(this.connectionUri);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ScenarioOutcome execute(Scenario scenario) {
        Objects.requireNonNull(scenario, "scenario");
        String databaseName = scenarioDatabaseName(scenario.id());

        try (MongoClient client = clientFactory.create(connectionUri)) {
            final Map<String, ClientSession> sessionPool = new HashMap<>();
            MongoDatabase database = client.getDatabase(databaseName);
            resetDatabase(database);

            List<Map<String, Object>> commandResults = new ArrayList<>(scenario.commands().size());
            try {
                for (int i = 0; i < scenario.commands().size(); i++) {
                    ScenarioCommand command = scenario.commands().get(i);
                    MongoDatabase commandDatabase = resolveCommandDatabase(client, database, command.commandName());
                    BsonDocument commandDocument;
                    try {
                        commandDocument = ScenarioBsonCodec.toRealMongodCommandDocument(command, databaseName);
                    } catch (RuntimeException exception) {
                        return ScenarioOutcome.failure(
                            "invalid command payload for " + command.commandName() + ": " + exception.getMessage()
                        );
                    }

                    final ClientSession session = resolveSession(client, command, sessionPool);

                    BsonDocument responseBody;
                    try {
                        responseBody = session == null
                            ? commandDatabase.runCommand(commandDocument, BsonDocument.class)
                            : commandDatabase.runCommand(session, commandDocument, BsonDocument.class);
                    } catch (MongoCommandException commandException) {
                        responseBody = commandException.getResponse();
                    } catch (RuntimeException exception) {
                        return ScenarioOutcome.failure(
                            "real mongod execution failed for " + command.commandName() + ": " + exception.getMessage()
                        );
                    }
                    responseBody = normalizeResponseForComparison(command, responseBody);

                    commandResults.add(ScenarioBsonCodec.toJavaMap(responseBody));
                    if (!ScenarioBsonCodec.isSuccess(responseBody)) {
                        return ScenarioOutcome.failure(ScenarioBsonCodec.formatFailure(command.commandName(), i, responseBody));
                    }
                }
            } finally {
                closeSessions(sessionPool);
            }
            return ScenarioOutcome.success(commandResults);
        } catch (RuntimeException exception) {
            return ScenarioOutcome.failure("failed to execute against real mongod: " + exception.getMessage());
        }
    }

    static String scenarioDatabaseName(String databasePrefix, String scenarioId) {
        String prefix = requireText(databasePrefix, "databasePrefix");
        String normalizedScenarioId = requireText(scenarioId, "scenarioId")
            .toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9]+", "_")
            .replaceAll("^_+", "")
            .replaceAll("_+$", "");
        if (normalizedScenarioId.isEmpty()) {
            normalizedScenarioId = "scenario";
        }
        if (normalizedScenarioId.length() > 40) {
            normalizedScenarioId = normalizedScenarioId.substring(0, 40);
        }
        return prefix + "_" + normalizedScenarioId;
    }

    private String scenarioDatabaseName(String scenarioId) {
        return scenarioDatabaseName(databasePrefix, scenarioId);
    }

    private static MongoDatabase resolveCommandDatabase(
        MongoClient client,
        MongoDatabase defaultDatabase,
        String commandName
    ) {
        if ("commitTransaction".equals(commandName) || "abortTransaction".equals(commandName)) {
            return client.getDatabase(ADMIN_DATABASE_NAME);
        }
        return defaultDatabase;
    }

    private static ClientSession resolveSession(
        final MongoClient client,
        final ScenarioCommand command,
        final Map<String, ClientSession> sessionPool
    ) {
        final String sessionId = readSessionId(command);
        if (sessionId == null) {
            return null;
        }
        return sessionPool.computeIfAbsent(sessionId, ignored -> client.startSession());
    }

    private static String readSessionId(final ScenarioCommand command) {
        final Object lsidValue = command.payload().get("lsid");
        if (!(lsidValue instanceof Map<?, ?> lsid)) {
            return null;
        }
        final Object idValue = lsid.get("id");
        if (idValue == null) {
            return null;
        }
        final String normalized = String.valueOf(idValue).trim();
        if (normalized.isEmpty()) {
            return null;
        }
        return normalized;
    }

    private static void closeSessions(final Map<String, ClientSession> sessions) {
        for (final ClientSession session : sessions.values()) {
            try {
                session.close();
            } catch (RuntimeException ignored) {
                // Best-effort cleanup for test harness sessions.
            }
        }
    }

    private static BsonDocument normalizeResponseForComparison(
        final ScenarioCommand command,
        final BsonDocument responseBody
    ) {
        if (!"countDocuments".equals(command.commandName())) {
            return responseBody;
        }
        if (!responseBody.containsKey("n") || responseBody.containsKey("count")) {
            final BsonValue cursorValue = responseBody.get("cursor");
            if (cursorValue == null || !cursorValue.isDocument()) {
                return responseBody;
            }
            final BsonValue firstBatchValue = cursorValue.asDocument().get("firstBatch");
            if (firstBatchValue == null || !firstBatchValue.isArray()) {
                return responseBody;
            }
            final BsonArray firstBatch = firstBatchValue.asArray();
            long count = 0L;
            if (!firstBatch.isEmpty()) {
                final BsonValue first = firstBatch.get(0);
                if (first != null && first.isDocument()) {
                    final BsonValue nValue = first.asDocument().get("n");
                    if (nValue != null && nValue.isNumber()) {
                        count = nValue.asNumber().longValue();
                    }
                }
            }
            return new BsonDocument()
                .append("n", new BsonInt64(count))
                .append("count", new BsonInt64(count))
                .append("ok", responseBody.get("ok"));
        }
        return responseBody.clone().append("count", responseBody.get("n"));
    }

    private static void resetDatabase(MongoDatabase database) {
        Objects.requireNonNull(database, "database");
        database.drop();
    }

    private static void validateConnectionUri(String connectionUri) {
        try {
            new ConnectionString(connectionUri);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("connectionUri is invalid: " + exception.getMessage(), exception);
        }
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    @FunctionalInterface
    interface MongoClientFactory {
        MongoClient create(String connectionUri);
    }
}
