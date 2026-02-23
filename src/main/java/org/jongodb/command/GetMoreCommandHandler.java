package org.jongodb.command;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class GetMoreCommandHandler implements CommandHandler {
    private final CursorRegistry cursorRegistry;

    public GetMoreCommandHandler(final CursorRegistry cursorRegistry) {
        this.cursorRegistry = cursorRegistry;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "collection");
        if (collection == null) {
            return CommandErrors.typeMismatch("collection must be a string");
        }

        final BsonValue getMoreValue = command.get("getMore");
        final Long cursorId = readIntegralLong(getMoreValue);
        if (cursorId == null) {
            return CommandErrors.typeMismatch("getMore must be an integer");
        }
        if (cursorId < 0) {
            return CommandErrors.badValue("getMore must be a non-negative integer");
        }

        final BsonValue batchSizeValue = command.get("batchSize");
        int batchSize = Integer.MAX_VALUE;
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

        final String namespace = database + "." + collection;
        final CursorRegistry.GetMoreResult result = cursorRegistry.getMore(cursorId, namespace, batchSize);
        if (!result.found()) {
            return CommandErrors.cursorNotFound(cursorId);
        }

        final BsonArray nextBatch = new BsonArray();
        for (final BsonDocument document : result.nextBatch()) {
            nextBatch.add(document);
        }

        final BsonDocument cursor = new BsonDocument()
                .append("id", new BsonInt64(result.cursorId()))
                .append("ns", new BsonString(namespace))
                .append("nextBatch", nextBatch);

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
        if (value == null) {
            return null;
        }
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
