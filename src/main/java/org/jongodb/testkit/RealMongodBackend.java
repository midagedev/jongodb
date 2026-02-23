package org.jongodb.testkit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * Differential backend adapter that executes scenario commands against real mongod.
 */
public final class RealMongodBackend implements DifferentialBackend {
    private static final String DEFAULT_DATABASE_PREFIX = "testkit";

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
            MongoDatabase database = client.getDatabase(databaseName);
            resetDatabase(database);

            List<Map<String, Object>> commandResults = new ArrayList<>(scenario.commands().size());
            for (int i = 0; i < scenario.commands().size(); i++) {
                ScenarioCommand command = scenario.commands().get(i);
                BsonDocument commandDocument;
                try {
                    commandDocument = ScenarioBsonCodec.toCommandDocument(command, databaseName);
                } catch (RuntimeException exception) {
                    return ScenarioOutcome.failure(
                        "invalid command payload for " + command.commandName() + ": " + exception.getMessage()
                    );
                }

                BsonDocument responseBody;
                try {
                    responseBody = database.runCommand(commandDocument, BsonDocument.class);
                } catch (MongoCommandException commandException) {
                    responseBody = commandException.getResponse();
                } catch (RuntimeException exception) {
                    return ScenarioOutcome.failure(
                        "real mongod execution failed for " + command.commandName() + ": " + exception.getMessage()
                    );
                }

                commandResults.add(ScenarioBsonCodec.toJavaMap(responseBody));
                if (!ScenarioBsonCodec.isSuccess(responseBody)) {
                    return ScenarioOutcome.failure(ScenarioBsonCodec.formatFailure(command.commandName(), i, responseBody));
                }
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
