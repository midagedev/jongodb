package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class KillCursorsCommandHandler implements CommandHandler {
    private final CursorRegistry cursorRegistry;

    public KillCursorsCommandHandler(final CursorRegistry cursorRegistry) {
        this.cursorRegistry = cursorRegistry;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "killCursors");
        if (collection == null) {
            return CommandErrors.typeMismatch("killCursors must be a string");
        }

        final BsonValue cursorsValue = command.get("cursors");
        if (cursorsValue == null || !cursorsValue.isArray()) {
            return CommandErrors.typeMismatch("cursors must be an array");
        }

        final List<Long> cursorIds = new ArrayList<>();
        for (final BsonValue cursorValue : cursorsValue.asArray()) {
            final Long cursorId = readIntegralLong(cursorValue);
            if (cursorId == null) {
                return CommandErrors.typeMismatch("cursors entries must be integers");
            }
            if (cursorId < 0) {
                return CommandErrors.badValue("cursor ids must be non-negative integers");
            }
            cursorIds.add(cursorId);
        }

        final String namespace = database + "." + collection;
        final CursorRegistry.KillResult killResult = cursorRegistry.killCursors(namespace, List.copyOf(cursorIds));

        return new BsonDocument()
                .append("cursorsKilled", toInt64Array(killResult.killedCursorIds()))
                .append("cursorsNotFound", toInt64Array(killResult.notFoundCursorIds()))
                .append("cursorsAlive", new BsonArray())
                .append("cursorsUnknown", new BsonArray())
                .append("ok", new BsonDouble(1.0));
    }

    private static BsonArray toInt64Array(final List<Long> values) {
        final BsonArray array = new BsonArray();
        for (final Long value : values) {
            array.add(new BsonInt64(value));
        }
        return array;
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
