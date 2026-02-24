package org.jongodb.command;

import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public final class CountDocumentsCommandHandler implements CommandHandler {
    private final CommandStore store;

    public CountDocumentsCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readCollection(command);
        if (collection == null) {
            return CommandErrors.typeMismatch("countDocuments must be a string");
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

        final BsonValue filterValue = command.containsKey("filter") ? command.get("filter") : command.get("query");
        final BsonDocument filter;
        if (filterValue == null) {
            filter = new BsonDocument();
        } else if (!filterValue.isDocument()) {
            return CommandErrors.typeMismatch("filter must be a document");
        } else {
            filter = filterValue.asDocument();
        }

        final int skip;
        final BsonValue skipValue = command.get("skip");
        if (skipValue == null) {
            skip = 0;
        } else {
            final Long parsedSkip = readIntegralLong(skipValue);
            if (parsedSkip == null) {
                return CommandErrors.typeMismatch("skip must be an integer");
            }
            if (parsedSkip < 0 || parsedSkip > Integer.MAX_VALUE) {
                return CommandErrors.badValue("skip must be a non-negative integer");
            }
            skip = parsedSkip.intValue();
        }

        final int limit;
        final BsonValue limitValue = command.get("limit");
        if (limitValue == null) {
            limit = 0;
        } else {
            final Long parsedLimit = readIntegralLong(limitValue);
            if (parsedLimit == null) {
                return CommandErrors.typeMismatch("limit must be an integer");
            }
            if (parsedLimit < 0 || parsedLimit > Integer.MAX_VALUE) {
                return CommandErrors.badValue("limit must be a non-negative integer");
            }
            limit = parsedLimit.intValue();
        }

        final List<BsonDocument> matches;
        try {
            matches = store.find(database, collection, filter);
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }

        final int fromIndex = Math.min(skip, matches.size());
        int toIndex = matches.size();
        if (limit > 0) {
            toIndex = Math.min(fromIndex + limit, matches.size());
        }
        final long count = toIndex - fromIndex;

        return new BsonDocument()
                .append("n", new BsonInt64(count))
                .append("count", new BsonInt64(count))
                .append("ok", new BsonDouble(1.0));
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static String readCollection(final BsonDocument command) {
        final BsonValue canonical = command.get("countDocuments");
        if (canonical != null) {
            if (canonical.isString()) {
                return canonical.asString().getValue();
            }
            return null;
        }

        final BsonValue countAlias = command.get("count");
        if (countAlias != null) {
            if (countAlias.isString()) {
                return countAlias.asString().getValue();
            }
            return null;
        }

        for (final String key : command.keySet()) {
            if (!"countdocuments".equalsIgnoreCase(key) && !"count".equalsIgnoreCase(key)) {
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
