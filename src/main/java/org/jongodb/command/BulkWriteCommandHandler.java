package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class BulkWriteCommandHandler implements CommandHandler {
    private static final Set<String> SUPPORTED_OPERATIONS =
            Set.of("insertone", "updateone", "updatemany", "deleteone", "deletemany", "replaceone");

    private final InsertCommandHandler insertCommandHandler;
    private final UpdateCommandHandler updateCommandHandler;
    private final DeleteCommandHandler deleteCommandHandler;
    private final ReplaceOneCommandHandler replaceOneCommandHandler;

    public BulkWriteCommandHandler(final CommandStore store) {
        this.insertCommandHandler = new InsertCommandHandler(store);
        this.updateCommandHandler = new UpdateCommandHandler(store);
        this.deleteCommandHandler = new DeleteCommandHandler(store);
        this.replaceOneCommandHandler = new ReplaceOneCommandHandler(store);
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readCollection(command);
        if (collection == null) {
            return CommandErrors.typeMismatch("bulkWrite must be a string");
        }

        BsonDocument optionError = CrudCommandOptionValidator.validateOrdered(command);
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateWriteConcern(command);
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateReadConcern(command);
        if (optionError != null) {
            return optionError;
        }

        final boolean ordered = !command.containsKey("ordered")
                || command.getBoolean("ordered").getValue();
        if (!ordered) {
            return CommandErrors.badValue("bulkWrite currently supports ordered=true only");
        }

        final ParsedOperations parsedOperations = parseOperations(command);
        if (parsedOperations.error() != null) {
            return parsedOperations.error();
        }

        int insertedCount = 0;
        int matchedCount = 0;
        int modifiedCount = 0;
        int deletedCount = 0;
        int upsertedCount = 0;
        final BsonArray upserted = new BsonArray();
        final BsonArray writeErrors = new BsonArray();

        for (final OperationEntry operation : parsedOperations.operations()) {
            final BsonDocument response = executeOperation(operation, database, collection);
            if (!isSuccessful(response)) {
                writeErrors.add(writeError(operation.index(), response));
                break;
            }

            final OperationCounts operationCounts = countsForOperation(operation.name(), operation.index(), response);
            insertedCount += operationCounts.insertedCount();
            matchedCount += operationCounts.matchedCount();
            modifiedCount += operationCounts.modifiedCount();
            deletedCount += operationCounts.deletedCount();
            upsertedCount += operationCounts.upsertedCount();
            for (final BsonDocument upsertedDocument : operationCounts.upsertedEntries()) {
                upserted.add(upsertedDocument);
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

    private BsonDocument executeOperation(final OperationEntry operation, final String database, final String collection) {
        return switch (operation.name()) {
            case "insertone" -> executeInsertOne(operation.specification(), database, collection);
            case "updateone" -> executeUpdate(operation.specification(), database, collection, false);
            case "updatemany" -> executeUpdate(operation.specification(), database, collection, true);
            case "deleteone" -> executeDelete(operation.specification(), database, collection, 1);
            case "deletemany" -> executeDelete(operation.specification(), database, collection, 0);
            case "replaceone" -> executeReplaceOne(operation.specification(), database, collection);
            default -> CommandErrors.badValue("unsupported bulkWrite operation: " + operation.name());
        };
    }

    private BsonDocument executeInsertOne(
            final BsonDocument operation,
            final String database,
            final String collection) {
        final BsonDocument document = readRequiredDocument(operation, "document");
        if (document == null) {
            return CommandErrors.typeMismatch("document must be a document");
        }

        final BsonDocument translatedCommand = new BsonDocument()
                .append("insert", new BsonString(collection))
                .append("$db", new BsonString(database))
                .append("documents", new BsonArray(List.of(document)));
        return insertCommandHandler.handle(translatedCommand);
    }

    private BsonDocument executeUpdate(
            final BsonDocument operation,
            final String database,
            final String collection,
            final boolean multi) {
        final BsonDocument filter = readRequiredDocument(operation, "filter");
        if (filter == null) {
            return CommandErrors.typeMismatch("filter must be a document");
        }

        final BsonValue updateValue = operation.get("update");
        if (updateValue == null) {
            return CommandErrors.typeMismatch("update must be a document");
        }
        if (updateValue.isArray()) {
            return CommandErrors.badValue("update pipeline is not supported yet");
        }
        if (!updateValue.isDocument()) {
            return CommandErrors.typeMismatch("update must be a document");
        }
        if (!isOperatorUpdate(updateValue.asDocument())) {
            return CommandErrors.badValue("bulkWrite update operation requires atomic modifiers");
        }

        final BsonValue upsertValue = operation.get("upsert");
        final boolean upsert;
        if (upsertValue == null) {
            upsert = false;
        } else if (!upsertValue.isBoolean()) {
            return CommandErrors.typeMismatch("upsert must be a boolean");
        } else {
            upsert = upsertValue.asBoolean().getValue();
        }

        final BsonDocument updateSpec = new BsonDocument()
                .append("q", filter)
                .append("u", updateValue.asDocument())
                .append("multi", BsonBoolean.valueOf(multi))
                .append("upsert", BsonBoolean.valueOf(upsert));
        appendIfPresent(operation, updateSpec, "hint");
        appendIfPresent(operation, updateSpec, "collation");
        appendIfPresent(operation, updateSpec, "arrayFilters");

        final BsonDocument translatedCommand = new BsonDocument()
                .append("update", new BsonString(collection))
                .append("$db", new BsonString(database))
                .append("updates", new BsonArray(List.of(updateSpec)));
        return updateCommandHandler.handle(translatedCommand);
    }

    private BsonDocument executeDelete(
            final BsonDocument operation,
            final String database,
            final String collection,
            final int limit) {
        final BsonDocument filter = readRequiredDocument(operation, "filter");
        if (filter == null) {
            return CommandErrors.typeMismatch("filter must be a document");
        }

        final BsonDocument deleteSpec = new BsonDocument()
                .append("q", filter)
                .append("limit", new BsonInt32(limit));
        appendIfPresent(operation, deleteSpec, "hint");
        appendIfPresent(operation, deleteSpec, "collation");

        final BsonDocument translatedCommand = new BsonDocument()
                .append("delete", new BsonString(collection))
                .append("$db", new BsonString(database))
                .append("deletes", new BsonArray(List.of(deleteSpec)));
        return deleteCommandHandler.handle(translatedCommand);
    }

    private BsonDocument executeReplaceOne(
            final BsonDocument operation,
            final String database,
            final String collection) {
        final BsonDocument translatedCommand = new BsonDocument()
                .append("replaceOne", new BsonString(collection))
                .append("$db", new BsonString(database));
        appendIfPresent(operation, translatedCommand, "filter");
        appendIfPresent(operation, translatedCommand, "replacement");
        appendIfPresent(operation, translatedCommand, "upsert");
        appendIfPresent(operation, translatedCommand, "hint");
        appendIfPresent(operation, translatedCommand, "collation");
        return replaceOneCommandHandler.handle(translatedCommand);
    }

    private static OperationCounts countsForOperation(
            final String operationName,
            final int operationIndex,
            final BsonDocument response) {
        return switch (operationName) {
            case "insertone" -> new OperationCounts(readCount(response, "n"), 0, 0, 0, 0, List.of());
            case "deleteone", "deletemany" -> new OperationCounts(0, 0, 0, readCount(response, "n"), 0, List.of());
            case "updateone", "updatemany", "replaceone" -> updateCounts(operationIndex, response);
            default -> new OperationCounts(0, 0, 0, 0, 0, List.of());
        };
    }

    private static OperationCounts updateCounts(final int operationIndex, final BsonDocument response) {
        final int n = readCount(response, "n");
        final int nModified = readCount(response, "nModified");
        final BsonArray upsertedArray =
                response.containsKey("upserted") && response.get("upserted").isArray()
                        ? response.getArray("upserted")
                        : new BsonArray();
        final int upsertedCount = upsertedArray.size();
        final int matchedCount = Math.max(0, n - upsertedCount);

        final List<BsonDocument> mappedUpserts = new ArrayList<>(upsertedCount);
        for (final BsonValue upsertedValue : upsertedArray) {
            if (!upsertedValue.isDocument()) {
                continue;
            }
            final BsonValue idValue = upsertedValue.asDocument().get("_id");
            if (idValue == null) {
                continue;
            }
            mappedUpserts.add(new BsonDocument()
                    .append("index", new BsonInt32(operationIndex))
                    .append("_id", idValue));
        }

        return new OperationCounts(0, matchedCount, nModified, 0, mappedUpserts.size(), List.copyOf(mappedUpserts));
    }

    private static ParsedOperations parseOperations(final BsonDocument command) {
        final BsonValue operationsValue = command.get("operations");
        final BsonValue requestsValue = command.get("requests");
        if (operationsValue != null && requestsValue != null) {
            return ParsedOperations.error(CommandErrors.badValue("operations and requests cannot both be specified"));
        }

        final BsonValue effectiveOperationsValue = operationsValue == null ? requestsValue : operationsValue;
        if (effectiveOperationsValue == null || !effectiveOperationsValue.isArray()) {
            return ParsedOperations.error(CommandErrors.typeMismatch("operations must be an array"));
        }

        final BsonArray operationsArray = effectiveOperationsValue.asArray();
        if (operationsArray.isEmpty()) {
            return ParsedOperations.error(CommandErrors.badValue("operations must not be empty"));
        }

        final List<OperationEntry> operations = new ArrayList<>(operationsArray.size());
        for (int index = 0; index < operationsArray.size(); index++) {
            final BsonValue operationValue = operationsArray.get(index);
            if (!operationValue.isDocument()) {
                return ParsedOperations.error(
                        CommandErrors.typeMismatch("all entries in operations must be BSON documents"));
            }

            final BsonDocument operationDocument = operationValue.asDocument();
            if (operationDocument.size() != 1) {
                return ParsedOperations.error(
                        CommandErrors.badValue("each bulkWrite operation must contain exactly one operation"));
            }

            final String operationName = operationDocument.getFirstKey();
            final String normalizedOperationName = operationName.toLowerCase(Locale.ROOT);
            if (!SUPPORTED_OPERATIONS.contains(normalizedOperationName)) {
                return ParsedOperations.error(CommandErrors.badValue("unsupported bulkWrite operation: " + operationName));
            }

            final BsonValue specificationValue = operationDocument.get(operationName);
            if (specificationValue == null || !specificationValue.isDocument()) {
                return ParsedOperations.error(CommandErrors.typeMismatch(operationName + " must be a document"));
            }
            operations.add(new OperationEntry(index, normalizedOperationName, specificationValue.asDocument()));
        }
        return new ParsedOperations(List.copyOf(operations), null);
    }

    private static BsonDocument writeError(final int index, final BsonDocument response) {
        final BsonDocument writeError = new BsonDocument()
                .append("index", new BsonInt32(index));

        final BsonValue code = response.get("code");
        if (code instanceof BsonNumber numberValue) {
            writeError.append("code", new BsonInt32(numberValue.intValue()));
        }
        final BsonValue codeName = response.get("codeName");
        if (codeName != null && codeName.isString()) {
            writeError.append("codeName", codeName.asString());
        }
        final BsonValue message = response.get("errmsg");
        if (message != null && message.isString()) {
            writeError.append("errmsg", message.asString());
        }
        return writeError;
    }

    private static int readCount(final BsonDocument response, final String fieldName) {
        final BsonValue value = response.get(fieldName);
        if (!(value instanceof BsonNumber numberValue)) {
            return 0;
        }
        return numberValue.intValue();
    }

    private static boolean isSuccessful(final BsonDocument response) {
        final BsonValue okValue = response.get("ok");
        return okValue != null
                && okValue.isNumber()
                && okValue.asNumber().doubleValue() == 1.0d;
    }

    private static void appendIfPresent(final BsonDocument source, final BsonDocument target, final String key) {
        if (!source.containsKey(key)) {
            return;
        }
        target.append(key, source.get(key));
    }

    private static BsonDocument readRequiredDocument(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        if (value == null || !value.isDocument()) {
            return null;
        }
        return value.asDocument();
    }

    private static boolean isOperatorUpdate(final BsonDocument updateDocument) {
        if (updateDocument.isEmpty()) {
            return false;
        }
        for (final String key : updateDocument.keySet()) {
            if (!key.startsWith("$")) {
                return false;
            }
        }
        return true;
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static String readCollection(final BsonDocument command) {
        final BsonValue canonical = command.get("bulkWrite");
        if (canonical != null) {
            if (canonical.isString()) {
                return canonical.asString().getValue();
            }
            return null;
        }

        for (final String key : command.keySet()) {
            if (!"bulkwrite".equalsIgnoreCase(key)) {
                continue;
            }
            final BsonValue value = command.get(key);
            if (value != null && value.isString()) {
                return value.asString().getValue();
            }
            return null;
        }
        return null;
    }

    private record ParsedOperations(List<OperationEntry> operations, BsonDocument error) {
        private static ParsedOperations error(final BsonDocument error) {
            return new ParsedOperations(List.of(), error);
        }
    }

    private record OperationEntry(int index, String name, BsonDocument specification) {}

    private record OperationCounts(
            int insertedCount,
            int matchedCount,
            int modifiedCount,
            int deletedCount,
            int upsertedCount,
            List<BsonDocument> upsertedEntries) {}
}
