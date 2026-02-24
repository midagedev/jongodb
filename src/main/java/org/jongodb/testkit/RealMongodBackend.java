package org.jongodb.testkit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonString;
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
                    final ClientSession session = resolveSession(client, command, sessionPool);
                    BsonDocument responseBody;
                    try {
                        if ("bulkWrite".equals(command.commandName())) {
                            responseBody = executeBulkWriteCommand(command, commandDatabase, session, databaseName);
                        } else if ("replaceOne".equals(command.commandName())) {
                            responseBody = executeReplaceOneCommand(command, commandDatabase, session, databaseName);
                        } else {
                            final BsonDocument commandDocument;
                            try {
                                commandDocument = ScenarioBsonCodec.toRealMongodCommandDocument(command, databaseName);
                            } catch (RuntimeException exception) {
                                return ScenarioOutcome.failure(
                                    "invalid command payload for " + command.commandName() + ": " + exception.getMessage()
                                );
                            }
                            responseBody = runCommand(commandDatabase, session, commandDocument);
                        }
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

    private static BsonDocument runCommand(
        final MongoDatabase database,
        final ClientSession session,
        final BsonDocument commandDocument
    ) {
        return session == null
            ? database.runCommand(commandDocument, BsonDocument.class)
            : database.runCommand(session, commandDocument, BsonDocument.class);
    }

    private static BsonDocument executeReplaceOneCommand(
        final ScenarioCommand command,
        final MongoDatabase database,
        final ClientSession session,
        final String defaultDatabase
    ) {
        final BsonDocument source = ScenarioBsonCodec.toCommandDocument(command, defaultDatabase);
        final BsonValue replacementValue = source.get("replacement");
        if (replacementValue == null || !replacementValue.isDocument()) {
            return typeMismatch("replacement must be a document");
        }
        final BsonDocument replacement = replacementValue.asDocument();
        if (replacement.isEmpty()) {
            return badValue("replacement must not be empty");
        }
        if (containsTopLevelOperator(replacement)) {
            return badValue("replacement document must not contain update operators");
        }

        final BsonDocument translatedCommand = ScenarioBsonCodec.toRealMongodCommandDocument(command, defaultDatabase);
        try {
            return runCommand(database, session, translatedCommand);
        } catch (final MongoCommandException exception) {
            return exception.getResponse();
        }
    }

    private static BsonDocument executeBulkWriteCommand(
        final ScenarioCommand command,
        final MongoDatabase database,
        final ClientSession session,
        final String defaultDatabase
    ) {
        final BsonDocument source = ScenarioBsonCodec.toCommandDocument(command, defaultDatabase);
        final String collection = readRequiredString(source, "bulkWrite");
        if (collection == null) {
            return typeMismatch("bulkWrite must be a string");
        }

        BsonValue operationsValue = source.get("operations");
        if (operationsValue == null) {
            operationsValue = source.get("requests");
        }
        if (operationsValue == null || !operationsValue.isArray()) {
            return typeMismatch("operations must be an array");
        }
        final BsonArray operations = operationsValue.asArray();
        if (operations.isEmpty()) {
            return badValue("operations must not be empty");
        }

        final BsonValue orderedValue = source.get("ordered");
        final boolean ordered;
        if (orderedValue == null) {
            ordered = true;
        } else if (!orderedValue.isBoolean()) {
            return typeMismatch("ordered must be a boolean");
        } else {
            ordered = orderedValue.asBoolean().getValue();
        }
        if (!ordered) {
            return badValue("bulkWrite currently supports ordered=true only");
        }

        int insertedCount = 0;
        int matchedCount = 0;
        int modifiedCount = 0;
        int deletedCount = 0;
        int upsertedCount = 0;
        final BsonArray upserted = new BsonArray();
        final BsonArray writeErrors = new BsonArray();

        for (int index = 0; index < operations.size(); index++) {
            final BsonValue operationValue = operations.get(index);
            final BsonDocument operationResponse;
            final String operationName;
            if (operationValue == null || !operationValue.isDocument()) {
                operationName = "unknown";
                operationResponse = typeMismatch("all entries in operations must be BSON documents");
            } else {
                final BsonDocument operationDocument = operationValue.asDocument();
                if (operationDocument.size() != 1) {
                    operationName = "unknown";
                    operationResponse = badValue("each bulkWrite operation must contain exactly one operation");
                } else {
                    operationName = operationDocument.getFirstKey().toLowerCase(Locale.ROOT);
                    final BsonValue specificationValue = operationDocument.get(operationDocument.getFirstKey());
                    if (specificationValue == null || !specificationValue.isDocument()) {
                        operationResponse = typeMismatch(operationDocument.getFirstKey() + " must be a document");
                    } else {
                        operationResponse = executeBulkWriteOperation(
                            database,
                            session,
                            source,
                            collection,
                            operationName,
                            specificationValue.asDocument(),
                            index == 0
                        );
                    }
                }
            }

            if (!ScenarioBsonCodec.isSuccess(operationResponse)) {
                writeErrors.add(bulkWriteError(index, operationResponse));
                break;
            }

            final OperationCounts operationCounts = bulkWriteCounts(operationName, index, operationResponse);
            insertedCount += operationCounts.insertedCount();
            matchedCount += operationCounts.matchedCount();
            modifiedCount += operationCounts.modifiedCount();
            deletedCount += operationCounts.deletedCount();
            upsertedCount += operationCounts.upsertedCount();
            for (final BsonDocument upsertedEntry : operationCounts.upsertedEntries()) {
                upserted.add(upsertedEntry);
            }
        }

        final BsonDocument result = new BsonDocument()
            .append("nInserted", new BsonInt32(insertedCount))
            .append("nMatched", new BsonInt32(matchedCount))
            .append("nModified", new BsonInt32(modifiedCount))
            .append("nDeleted", new BsonInt32(deletedCount))
            .append("nUpserted", new BsonInt32(upsertedCount));
        if (!upserted.isEmpty()) {
            result.append("upserted", upserted);
        }
        if (!writeErrors.isEmpty()) {
            result.append("writeErrors", writeErrors);
        }
        return result.append("ok", new BsonDouble(1.0));
    }

    private static BsonDocument executeBulkWriteOperation(
        final MongoDatabase database,
        final ClientSession session,
        final BsonDocument source,
        final String collection,
        final String operationName,
        final BsonDocument operation,
        final boolean includeStartTransaction
    ) {
        final BsonDocument translatedCommand = switch (operationName) {
            case "insertone" -> translateBulkInsertCommand(source, collection, operation, includeStartTransaction);
            case "updateone" -> translateBulkUpdateCommand(source, collection, operation, false, includeStartTransaction);
            case "updatemany" -> translateBulkUpdateCommand(source, collection, operation, true, includeStartTransaction);
            case "deleteone" -> translateBulkDeleteCommand(source, collection, operation, 1, includeStartTransaction);
            case "deletemany" -> translateBulkDeleteCommand(source, collection, operation, 0, includeStartTransaction);
            case "replaceone" -> translateBulkReplaceCommand(source, collection, operation, includeStartTransaction);
            default -> badValue("unsupported bulkWrite operation: " + operationName);
        };
        if (translatedCommand.containsKey("ok")) {
            return translatedCommand;
        }
        try {
            return runCommand(database, session, translatedCommand);
        } catch (final MongoCommandException exception) {
            return exception.getResponse();
        }
    }

    private static BsonDocument translateBulkInsertCommand(
        final BsonDocument source,
        final String collection,
        final BsonDocument operation,
        final boolean includeStartTransaction
    ) {
        final BsonValue document = operation.get("document");
        if (document == null || !document.isDocument()) {
            return typeMismatch("document must be a document");
        }
        final BsonDocument command = new BsonDocument("insert", new BsonString(collection))
            .append("documents", new BsonArray(List.of(document.asDocument())));
        copyBulkEnvelopeFields(source, command, includeStartTransaction);
        return command;
    }

    private static BsonDocument translateBulkUpdateCommand(
        final BsonDocument source,
        final String collection,
        final BsonDocument operation,
        final boolean multi,
        final boolean includeStartTransaction
    ) {
        final BsonValue filter = operation.get("filter");
        if (filter == null || !filter.isDocument()) {
            return typeMismatch("filter must be a document");
        }
        final BsonValue update = operation.get("update");
        if (update == null) {
            return typeMismatch("update must be a document");
        }
        if (update.isArray()) {
            return badValue("update pipeline is not supported yet");
        }
        if (!update.isDocument()) {
            return typeMismatch("update must be a document");
        }
        if (!isOperatorUpdate(update.asDocument())) {
            return badValue("bulkWrite update operation requires atomic modifiers");
        }

        final BsonValue upsertValue = operation.get("upsert");
        final boolean upsert;
        if (upsertValue == null) {
            upsert = false;
        } else if (!upsertValue.isBoolean()) {
            return typeMismatch("upsert must be a boolean");
        } else {
            upsert = upsertValue.asBoolean().getValue();
        }

        final BsonDocument updateEntry = new BsonDocument()
            .append("q", filter.asDocument())
            .append("u", update.asDocument())
            .append("multi", BsonBoolean.valueOf(multi))
            .append("upsert", BsonBoolean.valueOf(upsert));
        appendIfPresent(operation, updateEntry, "hint");
        appendIfPresent(operation, updateEntry, "collation");
        appendIfPresent(operation, updateEntry, "arrayFilters");

        final BsonDocument command = new BsonDocument("update", new BsonString(collection))
            .append("updates", new BsonArray(List.of(updateEntry)));
        copyBulkEnvelopeFields(source, command, includeStartTransaction);
        return command;
    }

    private static BsonDocument translateBulkDeleteCommand(
        final BsonDocument source,
        final String collection,
        final BsonDocument operation,
        final int limit,
        final boolean includeStartTransaction
    ) {
        final BsonValue filter = operation.get("filter");
        if (filter == null || !filter.isDocument()) {
            return typeMismatch("filter must be a document");
        }

        final BsonDocument deleteEntry = new BsonDocument()
            .append("q", filter.asDocument())
            .append("limit", new BsonInt32(limit));
        appendIfPresent(operation, deleteEntry, "hint");
        appendIfPresent(operation, deleteEntry, "collation");

        final BsonDocument command = new BsonDocument("delete", new BsonString(collection))
            .append("deletes", new BsonArray(List.of(deleteEntry)));
        copyBulkEnvelopeFields(source, command, includeStartTransaction);
        return command;
    }

    private static BsonDocument translateBulkReplaceCommand(
        final BsonDocument source,
        final String collection,
        final BsonDocument operation,
        final boolean includeStartTransaction
    ) {
        final BsonValue filter = operation.get("filter");
        if (filter == null || !filter.isDocument()) {
            return typeMismatch("filter must be a document");
        }
        final BsonValue replacement = operation.get("replacement");
        if (replacement == null || !replacement.isDocument()) {
            return typeMismatch("replacement must be a document");
        }
        if (replacement.asDocument().isEmpty()) {
            return badValue("replacement must not be empty");
        }
        if (containsTopLevelOperator(replacement.asDocument())) {
            return badValue("replacement document must not contain update operators");
        }

        final BsonValue upsertValue = operation.get("upsert");
        final boolean upsert;
        if (upsertValue == null) {
            upsert = false;
        } else if (!upsertValue.isBoolean()) {
            return typeMismatch("upsert must be a boolean");
        } else {
            upsert = upsertValue.asBoolean().getValue();
        }

        final BsonDocument updateEntry = new BsonDocument()
            .append("q", filter.asDocument())
            .append("u", replacement.asDocument())
            .append("multi", BsonBoolean.FALSE)
            .append("upsert", BsonBoolean.valueOf(upsert));
        appendIfPresent(operation, updateEntry, "hint");
        appendIfPresent(operation, updateEntry, "collation");

        final BsonDocument command = new BsonDocument("update", new BsonString(collection))
            .append("updates", new BsonArray(List.of(updateEntry)));
        copyBulkEnvelopeFields(source, command, includeStartTransaction);
        return command;
    }

    private static void copyBulkEnvelopeFields(
        final BsonDocument source,
        final BsonDocument target,
        final boolean includeStartTransaction
    ) {
        appendIfPresent(source, target, "txnNumber");
        appendIfPresent(source, target, "autocommit");
        if (includeStartTransaction) {
            appendIfPresent(source, target, "startTransaction");
            appendIfPresent(source, target, "readConcern");
        }
        appendIfPresent(source, target, "writeConcern");
        appendIfPresent(source, target, "readPreference");
        appendIfPresent(source, target, "$readPreference");
    }

    private static void appendIfPresent(final BsonDocument source, final BsonDocument target, final String key) {
        if (!source.containsKey(key)) {
            return;
        }
        target.put(key, source.get(key));
    }

    private static OperationCounts bulkWriteCounts(
        final String operationName,
        final int operationIndex,
        final BsonDocument response
    ) {
        return switch (operationName) {
            case "insertone" -> new OperationCounts(readCount(response, "n"), 0, 0, 0, 0, List.of());
            case "deleteone", "deletemany" -> new OperationCounts(0, 0, 0, readCount(response, "n"), 0, List.of());
            case "updateone", "updatemany", "replaceone" -> bulkWriteUpdateCounts(operationIndex, response);
            default -> new OperationCounts(0, 0, 0, 0, 0, List.of());
        };
    }

    private static OperationCounts bulkWriteUpdateCounts(final int operationIndex, final BsonDocument response) {
        final int n = readCount(response, "n");
        final int nModified = readCount(response, "nModified");
        final BsonArray upsertedArray = response.containsKey("upserted") && response.get("upserted").isArray()
            ? response.getArray("upserted")
            : new BsonArray();
        final int upsertedCount = upsertedArray.size();
        final int matchedCount = Math.max(0, n - upsertedCount);

        final List<BsonDocument> mappedUpserts = new ArrayList<>(upsertedCount);
        for (final BsonValue upsertedValue : upsertedArray) {
            if (upsertedValue == null || !upsertedValue.isDocument()) {
                continue;
            }
            final BsonValue idValue = upsertedValue.asDocument().get("_id");
            if (idValue == null) {
                continue;
            }
            mappedUpserts.add(new BsonDocument("index", new BsonInt32(operationIndex)).append("_id", idValue));
        }

        return new OperationCounts(0, matchedCount, nModified, 0, mappedUpserts.size(), List.copyOf(mappedUpserts));
    }

    private static BsonDocument bulkWriteError(final int operationIndex, final BsonDocument response) {
        final BsonDocument error = new BsonDocument("index", new BsonInt32(operationIndex));
        final BsonValue code = response.get("code");
        if (code instanceof BsonNumber codeNumber) {
            error.put("code", new BsonInt32(codeNumber.intValue()));
        }
        final BsonValue codeName = response.get("codeName");
        if (codeName != null && codeName.isString()) {
            error.put("codeName", codeName.asString());
        }
        final BsonValue errmsg = response.get("errmsg");
        if (errmsg != null && errmsg.isString()) {
            error.put("errmsg", errmsg.asString());
        }
        return error;
    }

    private static boolean isOperatorUpdate(final BsonDocument update) {
        if (update.isEmpty()) {
            return false;
        }
        for (final String key : update.keySet()) {
            if (!key.startsWith("$")) {
                return false;
            }
        }
        return true;
    }

    private static boolean containsTopLevelOperator(final BsonDocument document) {
        for (final String key : document.keySet()) {
            if (key.startsWith("$")) {
                return true;
            }
        }
        return false;
    }

    private static int readCount(final BsonDocument document, final String key) {
        final BsonValue value = document.get(key);
        if (!(value instanceof BsonNumber numberValue)) {
            return 0;
        }
        return numberValue.intValue();
    }

    private static String readRequiredString(final BsonDocument document, final String key) {
        final BsonValue value = document.get(key);
        if (value == null || !value.isString()) {
            return null;
        }
        return value.asString().getValue();
    }

    private static BsonDocument typeMismatch(final String message) {
        return new BsonDocument()
            .append("ok", new BsonDouble(0.0))
            .append("code", new BsonInt32(14))
            .append("codeName", new BsonString("TypeMismatch"))
            .append("errmsg", new BsonString(message));
    }

    private static BsonDocument badValue(final String message) {
        return new BsonDocument()
            .append("ok", new BsonDouble(0.0))
            .append("code", new BsonInt32(14))
            .append("codeName", new BsonString("BadValue"))
            .append("errmsg", new BsonString(message));
    }

    private record OperationCounts(
        int insertedCount,
        int matchedCount,
        int modifiedCount,
        int deletedCount,
        int upsertedCount,
        List<BsonDocument> upsertedEntries) {
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
        if ("distinct".equals(command.commandName())) {
            final BsonValue valuesValue = responseBody.get("values");
            if (valuesValue != null && valuesValue.isArray()) {
                return responseBody.clone().append("values", normalizeDistinctValues(valuesValue.asArray()));
            }
            return responseBody;
        }

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

    private static BsonArray normalizeDistinctValues(final BsonArray values) {
        final List<BsonValue> sorted = new ArrayList<>(values.getValues());
        sorted.sort(Comparator.comparing(RealMongodBackend::distinctSortKey));
        return new BsonArray(sorted);
    }

    private static String distinctSortKey(final BsonValue value) {
        final StringBuilder builder = new StringBuilder();
        builder.append(typeRank(value)).append('|');
        appendDistinctSortValue(value, builder);
        return builder.toString();
    }

    private static int typeRank(final BsonValue value) {
        if (value == null) {
            return 0;
        }
        if (value.isInt32() || value.isInt64() || value.isDouble() || value.isDecimal128()) {
            return 1;
        }
        if (value.isString()) {
            return 2;
        }
        if (value.isDocument()) {
            return 3;
        }
        if (value.isArray()) {
            return 4;
        }
        if (value.isBoolean()) {
            return 5;
        }
        if (value.isDateTime() || value.isTimestamp()) {
            return 6;
        }
        return 9;
    }

    private static void appendDistinctSortValue(final BsonValue value, final StringBuilder builder) {
        if (value == null) {
            builder.append("NULL:null");
            return;
        }
        builder.append(value.getBsonType().name()).append(':');
        if (value.isDocument()) {
            builder.append('{');
            boolean first = true;
            for (final String key : value.asDocument().keySet()) {
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(key).append('=');
                appendDistinctSortValue(value.asDocument().get(key), builder);
            }
            builder.append('}');
            return;
        }
        if (value.isArray()) {
            builder.append('[');
            for (int index = 0; index < value.asArray().size(); index++) {
                if (index > 0) {
                    builder.append(',');
                }
                appendDistinctSortValue(value.asArray().get(index), builder);
            }
            builder.append(']');
            return;
        }
        builder.append(value);
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
