package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.engine.DuplicateKeyException;

public final class UpdateCommandHandler implements CommandHandler {
    private static final int CODE_INVALID_ARGUMENT = 14;
    private static final Set<String> SUPPORTED_OPERATORS = Set.of("$set", "$inc", "$unset");

    private final CommandStore store;

    public UpdateCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "update");
        if (collection == null) {
            return CommandDispatcher.error("update must be a string", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonValue updatesValue = command.get("updates");
        if (updatesValue == null || !updatesValue.isArray()) {
            return CommandDispatcher.error("updates must be an array", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonArray updatesArray = updatesValue.asArray();
        if (updatesArray.isEmpty()) {
            return CommandDispatcher.error("updates must not be empty", CODE_INVALID_ARGUMENT, "BadValue");
        }

        final List<CommandStore.UpdateRequest> updates = new ArrayList<>(updatesArray.size());
        for (final BsonValue value : updatesArray) {
            if (!value.isDocument()) {
                return CommandDispatcher.error(
                        "all entries in updates must be BSON documents", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            final BsonDocument updateSpec = value.asDocument();
            final BsonDocument query = readRequiredDocument(updateSpec, "q");
            if (query == null) {
                return CommandDispatcher.error("q must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            final BsonDocument updateDocument = readRequiredDocument(updateSpec, "u");
            if (updateDocument == null) {
                return CommandDispatcher.error("u must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            final BsonValue multiValue = updateSpec.get("multi");
            final boolean multi;
            if (multiValue == null) {
                multi = false;
            } else if (!multiValue.isBoolean()) {
                return CommandDispatcher.error("multi must be a boolean", CODE_INVALID_ARGUMENT, "TypeMismatch");
            } else {
                multi = multiValue.asBoolean().getValue();
            }

            final BsonValue upsertValue = updateSpec.get("upsert");
            if (upsertValue != null && !upsertValue.isBoolean()) {
                return CommandDispatcher.error("upsert must be a boolean", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            final BsonDocument updateValidationError = validateUpdateDocument(updateDocument);
            if (updateValidationError != null) {
                return updateValidationError;
            }

            updates.add(new CommandStore.UpdateRequest(query, updateDocument, multi));
        }

        final CommandStore.UpdateResult result;
        try {
            result = store.update(database, collection, List.copyOf(updates));
        } catch (final DuplicateKeyException exception) {
            return CommandDispatcher.duplicateKeyError(exception.getMessage());
        }
        return new BsonDocument()
                .append("n", new BsonInt32(result.matchedCount()))
                .append("nModified", new BsonInt32(result.modifiedCount()))
                .append("ok", new BsonDouble(1.0));
    }

    private static BsonDocument validateUpdateDocument(final BsonDocument updateDocument) {
        if (updateDocument.isEmpty()) {
            return CommandDispatcher.error("u must not be empty", CODE_INVALID_ARGUMENT, "BadValue");
        }

        final boolean operatorStyle = updateDocument.getFirstKey().startsWith("$");
        for (final String key : updateDocument.keySet()) {
            final boolean currentOperatorStyle = key.startsWith("$");
            if (currentOperatorStyle != operatorStyle) {
                return CommandDispatcher.error(
                        "u must either be an operator document or a replacement document",
                        CODE_INVALID_ARGUMENT,
                        "BadValue");
            }
        }

        if (!operatorStyle) {
            return null;
        }

        for (final String key : updateDocument.keySet()) {
            if (!SUPPORTED_OPERATORS.contains(key)) {
                return CommandDispatcher.error(
                        "unsupported update operator: " + key, CODE_INVALID_ARGUMENT, "BadValue");
            }
        }

        final BsonValue setValue = updateDocument.get("$set");
        if (setValue != null && !setValue.isDocument()) {
            return CommandDispatcher.error("$set must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }
        final BsonValue incValue = updateDocument.get("$inc");
        if (incValue != null && !incValue.isDocument()) {
            return CommandDispatcher.error("$inc must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }
        final BsonValue unsetValue = updateDocument.get("$unset");
        if (unsetValue != null && !unsetValue.isDocument()) {
            return CommandDispatcher.error("$unset must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }
        return null;
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static String readRequiredString(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        if (!(value instanceof BsonString bsonString)) {
            return null;
        }
        return bsonString.getValue();
    }

    private static BsonDocument readRequiredDocument(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        if (value == null || !value.isDocument()) {
            return null;
        }
        return value.asDocument();
    }
}
