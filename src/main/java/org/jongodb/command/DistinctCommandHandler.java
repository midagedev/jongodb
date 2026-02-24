package org.jongodb.command;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public final class DistinctCommandHandler implements CommandHandler {
    private final CommandStore store;

    public DistinctCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readCollection(command);
        if (collection == null) {
            return CommandErrors.typeMismatch("distinct must be a string");
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
        if (command.containsKey("collation")) {
            return CommandErrors.notImplemented("collation is not supported for distinct yet");
        }

        final BsonValue keyValue = command.get("key");
        if (keyValue == null || !keyValue.isString()) {
            return CommandErrors.typeMismatch("key must be a string");
        }

        final String key = keyValue.asString().getValue();
        final String[] path = splitPath(key);
        if (path.length == 0) {
            return CommandErrors.badValue("key must be a non-empty dotted field path");
        }

        final BsonValue queryValue = command.containsKey("query") ? command.get("query") : command.get("filter");
        final BsonDocument query;
        if (queryValue == null) {
            query = new BsonDocument();
        } else if (!queryValue.isDocument()) {
            return CommandErrors.typeMismatch("query must be a document");
        } else {
            query = queryValue.asDocument();
        }

        final List<BsonDocument> matches;
        try {
            matches = store.find(database, collection, query);
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }

        final BsonArray values = distinctValues(matches, path);
        return new BsonDocument()
                .append("values", values)
                .append("ok", new BsonDouble(1.0));
    }

    private static BsonArray distinctValues(final List<BsonDocument> matches, final String[] path) {
        final Set<String> seen = new LinkedHashSet<>();
        final BsonArray values = new BsonArray();

        for (final BsonDocument match : matches) {
            final List<BsonValue> extracted = new ArrayList<>();
            collectPathValues(match, path, 0, extracted);
            for (final BsonValue value : extracted) {
                final String key = dedupeKey(value);
                if (seen.add(key)) {
                    values.add(value);
                }
            }
        }
        return values;
    }

    private static void collectPathValues(
            final BsonValue current,
            final String[] path,
            final int depth,
            final List<BsonValue> out) {
        if (current == null) {
            return;
        }
        if (depth == path.length) {
            appendTerminalValues(current, out);
            return;
        }

        if (current.isDocument()) {
            collectPathValues(current.asDocument().get(path[depth]), path, depth + 1, out);
            return;
        }

        if (current.isArray()) {
            for (final BsonValue item : current.asArray()) {
                collectPathValues(item, path, depth, out);
            }
        }
    }

    private static void appendTerminalValues(final BsonValue value, final List<BsonValue> out) {
        if (value == null) {
            return;
        }
        if (value.isArray()) {
            for (final BsonValue item : value.asArray()) {
                appendTerminalValues(item, out);
            }
            return;
        }
        out.add(value);
    }

    private static String dedupeKey(final BsonValue value) {
        if (value == null) {
            return "<null>";
        }
        return value.getBsonType().name() + ":" + value;
    }

    private static String[] splitPath(final String key) {
        if (key == null || key.isBlank()) {
            return new String[0];
        }
        final String[] path = key.split("\\.", -1);
        for (final String segment : path) {
            if (segment == null || segment.isEmpty()) {
                return new String[0];
            }
        }
        return path;
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static String readCollection(final BsonDocument command) {
        final BsonValue canonical = command.get("distinct");
        if (canonical != null) {
            if (canonical.isString()) {
                return canonical.asString().getValue();
            }
            return null;
        }

        for (final String key : command.keySet()) {
            if (!"distinct".equalsIgnoreCase(key)) {
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
}
