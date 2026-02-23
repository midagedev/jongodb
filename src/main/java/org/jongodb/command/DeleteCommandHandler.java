package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class DeleteCommandHandler implements CommandHandler {
    private static final int CODE_INVALID_ARGUMENT = 14;

    private final CommandStore store;

    public DeleteCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "delete");
        if (collection == null) {
            return CommandDispatcher.error("delete must be a string", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonValue deletesValue = command.get("deletes");
        if (deletesValue == null || !deletesValue.isArray()) {
            return CommandDispatcher.error("deletes must be an array", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonArray deletesArray = deletesValue.asArray();
        if (deletesArray.isEmpty()) {
            return CommandDispatcher.error("deletes must not be empty", CODE_INVALID_ARGUMENT, "BadValue");
        }

        final List<CommandStore.DeleteRequest> deletes = new ArrayList<>(deletesArray.size());
        for (final BsonValue value : deletesArray) {
            if (!value.isDocument()) {
                return CommandDispatcher.error(
                        "all entries in deletes must be BSON documents", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            final BsonDocument deleteSpec = value.asDocument();
            final BsonDocument query = readRequiredDocument(deleteSpec, "q");
            if (query == null) {
                return CommandDispatcher.error("q must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            final BsonValue limitValue = deleteSpec.get("limit");
            if (limitValue == null) {
                return CommandDispatcher.error("limit must be an integer", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            final Integer limit = readLimit(limitValue);
            if (limit == null) {
                return CommandDispatcher.error("limit must be an integer", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }

            if (limit != 0 && limit != 1) {
                return CommandDispatcher.error("limit must be 0 or 1", CODE_INVALID_ARGUMENT, "BadValue");
            }

            deletes.add(new CommandStore.DeleteRequest(query, limit));
        }

        final int deletedCount = store.delete(database, collection, List.copyOf(deletes));
        return new BsonDocument()
                .append("n", new BsonInt32(deletedCount))
                .append("ok", new BsonDouble(1.0));
    }

    private static Integer readLimit(final BsonValue limitValue) {
        if (limitValue.isInt32()) {
            return limitValue.asInt32().getValue();
        }
        if (limitValue.isInt64()) {
            final long value = limitValue.asInt64().getValue();
            return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE ? (int) value : null;
        }
        if (limitValue.isDouble()) {
            final double value = limitValue.asDouble().getValue();
            if (!Double.isFinite(value) || Math.rint(value) != value) {
                return null;
            }
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                return null;
            }
            return (int) value;
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
