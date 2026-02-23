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
    private final CommandStore store;

    public DeleteCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "delete");
        if (collection == null) {
            return CommandErrors.typeMismatch("delete must be a string");
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

        final BsonValue deletesValue = command.get("deletes");
        if (deletesValue == null || !deletesValue.isArray()) {
            return CommandErrors.typeMismatch("deletes must be an array");
        }

        final BsonArray deletesArray = deletesValue.asArray();
        if (deletesArray.isEmpty()) {
            return CommandErrors.badValue("deletes must not be empty");
        }

        final List<CommandStore.DeleteRequest> deletes = new ArrayList<>(deletesArray.size());
        for (final BsonValue value : deletesArray) {
            if (!value.isDocument()) {
                return CommandErrors.typeMismatch("all entries in deletes must be BSON documents");
            }

            final BsonDocument deleteSpec = value.asDocument();
            final BsonDocument query = readRequiredDocument(deleteSpec, "q");
            if (query == null) {
                return CommandErrors.typeMismatch("q must be a document");
            }

            optionError = CrudCommandOptionValidator.validateHint(deleteSpec, "hint");
            if (optionError != null) {
                return optionError;
            }
            optionError = CrudCommandOptionValidator.validateCollation(deleteSpec, "collation");
            if (optionError != null) {
                return optionError;
            }

            final BsonValue limitValue = deleteSpec.get("limit");
            if (limitValue == null) {
                return CommandErrors.typeMismatch("limit must be an integer");
            }

            final Integer limit = readLimit(limitValue);
            if (limit == null) {
                return CommandErrors.typeMismatch("limit must be an integer");
            }

            if (limit != 0 && limit != 1) {
                return CommandErrors.badValue("limit must be 0 or 1");
            }

            deletes.add(new CommandStore.DeleteRequest(query, limit));
        }

        final int deletedCount;
        try {
            deletedCount = store.delete(database, collection, List.copyOf(deletes));
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }
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
