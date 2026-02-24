package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonType;
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

        final List<SortKey> sortKeys;
        final BsonValue sortValue = command.get("sort");
        if (sortValue == null) {
            sortKeys = List.of();
        } else if (!sortValue.isDocument()) {
            return CommandErrors.typeMismatch("sort must be a document");
        } else {
            try {
                sortKeys = parseSortKeys(sortValue.asDocument());
            } catch (final IllegalArgumentException exception) {
                return CommandErrors.badValue(exception.getMessage());
            }
        }

        final int skip = parseSkip(command.get("skip"));
        if (skip < 0) {
            return CommandErrors.typeMismatch("skip must be an integer");
        }

        final Integer limit = parseLimit(command.get("limit"));
        if (limit == null && command.get("limit") != null) {
            return CommandErrors.typeMismatch("limit must be an integer");
        }

        final List<BsonDocument> foundDocuments;
        try {
            foundDocuments = store.find(database, collection, filter);
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }

        final List<BsonDocument> selectedDocuments = applySortSkipLimit(foundDocuments, sortKeys, skip, limit);

        final BsonValue batchSizeValue = command.get("batchSize");
        int batchSize = selectedDocuments.size();
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
                cursorRegistry.openCursor(database + "." + collection, selectedDocuments, batchSize);
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

    private static int parseSkip(final BsonValue skipValue) {
        if (skipValue == null) {
            return 0;
        }
        final Long parsed = readIntegralLong(skipValue);
        if (parsed == null || parsed > Integer.MAX_VALUE) {
            return -1;
        }
        if (parsed < 0) {
            // Spring Data uses -1 for "unset" in some internal copies.
            return 0;
        }
        return parsed.intValue();
    }

    private static Integer parseLimit(final BsonValue limitValue) {
        if (limitValue == null) {
            return 0;
        }
        final Long parsed = readIntegralLong(limitValue);
        if (parsed == null || Math.abs(parsed) > Integer.MAX_VALUE) {
            return null;
        }
        if (parsed == 0) {
            return 0;
        }
        final long absoluteLimit = Math.abs(parsed);
        return (int) absoluteLimit;
    }

    private static List<SortKey> parseSortKeys(final BsonDocument sortDocument) {
        final List<SortKey> sortKeys = new ArrayList<>(sortDocument.size());
        for (final String field : sortDocument.keySet()) {
            final Long parsedDirection = readIntegralLong(sortDocument.get(field));
            if (parsedDirection == null) {
                throw new IllegalArgumentException("sort directions must be integers");
            }
            if (parsedDirection != 1L && parsedDirection != -1L) {
                throw new IllegalArgumentException("sort directions must be 1 or -1");
            }
            sortKeys.add(new SortKey(field, parsedDirection.intValue()));
        }
        return List.copyOf(sortKeys);
    }

    private static List<BsonDocument> applySortSkipLimit(
            final List<BsonDocument> source, final List<SortKey> sortKeys, final int skip, final Integer limit) {
        final List<BsonDocument> working = new ArrayList<>(source);
        if (!sortKeys.isEmpty()) {
            working.sort((left, right) -> compareBySortKeys(left, right, sortKeys));
        }

        final int from = Math.min(skip, working.size());
        int to = working.size();
        if (limit != null && limit > 0) {
            to = Math.min(from + limit, working.size());
        }
        return List.copyOf(working.subList(from, to));
    }

    private static int compareBySortKeys(
            final BsonDocument left, final BsonDocument right, final List<SortKey> sortKeys) {
        for (final SortKey sortKey : sortKeys) {
            final BsonValue leftValue = resolvePath(left, sortKey.field());
            final BsonValue rightValue = resolvePath(right, sortKey.field());
            final int compared = compareSortValues(leftValue, rightValue);
            if (compared != 0) {
                return sortKey.direction() == 1 ? compared : -compared;
            }
        }
        return 0;
    }

    private static BsonValue resolvePath(final BsonDocument document, final String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }

        final String[] segments = path.split("\\.");
        BsonValue current = document;
        for (final String segment : segments) {
            if (current == null) {
                return null;
            }
            if (current.isDocument()) {
                current = current.asDocument().get(segment);
                continue;
            }
            if (current.isArray()) {
                final BsonArray array = current.asArray();
                if (array.isEmpty()) {
                    return null;
                }
                current = array.get(0);
                if (current.isDocument()) {
                    current = current.asDocument().get(segment);
                    continue;
                }
            }
            return null;
        }
        return current;
    }

    private static int compareSortValues(final BsonValue left, final BsonValue right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left.isNumber() && right.isNumber()) {
            return Double.compare(left.asNumber().doubleValue(), right.asNumber().doubleValue());
        }
        if (left.isString() && right.isString()) {
            return left.asString().getValue().compareTo(right.asString().getValue());
        }
        if (left.isBoolean() && right.isBoolean()) {
            return Boolean.compare(left.asBoolean().getValue(), right.asBoolean().getValue());
        }
        if (left.isDateTime() && right.isDateTime()) {
            return Long.compare(left.asDateTime().getValue(), right.asDateTime().getValue());
        }
        if (left.isObjectId() && right.isObjectId()) {
            return left.asObjectId().getValue().toHexString().compareTo(right.asObjectId().getValue().toHexString());
        }
        if (left.getBsonType() == right.getBsonType()) {
            return left.toString().compareTo(right.toString());
        }

        final int leftRank = sortTypeRank(left.getBsonType());
        final int rightRank = sortTypeRank(right.getBsonType());
        if (leftRank != rightRank) {
            return Integer.compare(leftRank, rightRank);
        }
        return left.toString().compareTo(right.toString());
    }

    private static int sortTypeRank(final BsonType type) {
        if (type == null) {
            return 0;
        }
        return switch (type) {
            case NULL -> 0;
            case INT32, INT64, DOUBLE, DECIMAL128 -> 1;
            case STRING, SYMBOL -> 2;
            case DOCUMENT -> 3;
            case ARRAY -> 4;
            case BINARY -> 5;
            case OBJECT_ID -> 6;
            case BOOLEAN -> 7;
            case DATE_TIME, TIMESTAMP -> 8;
            default -> 9;
        };
    }

    private record SortKey(String field, int direction) {}
}
