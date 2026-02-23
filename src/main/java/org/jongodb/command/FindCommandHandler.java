package org.jongodb.command;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class FindCommandHandler implements CommandHandler {
    private final CommandStore store;
    private final CursorRegistry cursorRegistry;

    public FindCommandHandler(final CommandStore store, final CursorRegistry cursorRegistry) {
        this.store = store;
        this.cursorRegistry = cursorRegistry;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "find");
        if (collection == null) {
            return CommandErrors.typeMismatch("find must be a string");
        }

        BsonDocument optionError = CrudCommandOptionValidator.validateReadConcern(command);
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateHint(command, "hint");
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateCollation(command, "collation");
        if (optionError != null) {
            return optionError;
        }

        final BsonValue filterValue = command.get("filter");
        final BsonDocument filter;
        if (filterValue == null) {
            filter = new BsonDocument();
        } else if (filterValue.isDocument()) {
            filter = filterValue.asDocument();
        } else {
            return CommandErrors.typeMismatch("filter must be a document");
        }

        final List<BsonDocument> foundDocuments;
        try {
            foundDocuments = store.find(database, collection, filter);
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }
        final BsonValue batchSizeValue = command.get("batchSize");
        int batchSize = foundDocuments.size();
        if (batchSizeValue != null) {
            final Long parsedBatchSize = readIntegralLong(batchSizeValue);
            if (parsedBatchSize == null) {
                return CommandErrors.typeMismatch("batchSize must be an integer");
            }
            if (parsedBatchSize < 0 || parsedBatchSize > Integer.MAX_VALUE) {
                return CommandErrors.badValue("batchSize must be a non-negative integer");
            }
            batchSize = parsedBatchSize.intValue();
        }

        final CursorRegistry.FindRegistration registration =
                cursorRegistry.openCursor(database + "." + collection, foundDocuments, batchSize);
        final BsonArray firstBatch = new BsonArray();
        for (final BsonDocument foundDocument : registration.firstBatch()) {
            firstBatch.add(foundDocument);
        }

        final BsonDocument cursor = new BsonDocument()
                .append("id", new BsonInt64(registration.cursorId()))
                .append("ns", new BsonString(database + "." + collection))
                .append("firstBatch", firstBatch);

        return new BsonDocument()
                .append("cursor", cursor)
                .append("ok", new BsonDouble(1.0));
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

    private static Long readIntegralLong(final BsonValue value) {
        if (value.isInt32()) {
            return (long) value.asInt32().getValue();
        }
        if (value.isInt64()) {
            return value.asInt64().getValue();
        }
        if (value.isDouble()) {
            final double doubleValue = value.asDouble().getValue();
            if (!Double.isFinite(doubleValue) || Math.rint(doubleValue) != doubleValue) {
                return null;
            }
            if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                return null;
            }
            return (long) doubleValue;
        }
        return null;
    }
}
