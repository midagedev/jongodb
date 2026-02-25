package org.jongodb.command;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.jongodb.engine.CollationSupport;
import org.jongodb.engine.DuplicateKeyException;

public final class FindAndModifyCommandHandler implements CommandHandler {
    private final CommandStore store;

    public FindAndModifyCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readCollection(command);
        if (collection == null) {
            return CommandErrors.typeMismatch("findAndModify must be a string");
        }
        BsonDocument optionError = CrudCommandOptionValidator.validateCollation(command, "collation");
        if (optionError != null) {
            return optionError;
        }
        final CollationSupport.Config collation = CrudCommandOptionValidator.collationOrSimple(command, "collation");

        final BsonDocument query;
        final BsonValue queryValue = command.get("query");
        if (queryValue == null) {
            query = new BsonDocument();
        } else if (!queryValue.isDocument()) {
            return CommandErrors.typeMismatch("query must be a document");
        } else {
            query = queryValue.asDocument();
        }

        final boolean remove = readBoolean(command.get("remove"), false);
        if (command.containsKey("remove") && !command.get("remove").isBoolean()) {
            return CommandErrors.typeMismatch("remove must be a boolean");
        }

        final boolean returnNew = readBoolean(command.get("new"), false);
        if (command.containsKey("new") && !command.get("new").isBoolean()) {
            return CommandErrors.typeMismatch("new must be a boolean");
        }

        final boolean upsert = readBoolean(command.get("upsert"), false);
        if (command.containsKey("upsert") && !command.get("upsert").isBoolean()) {
            return CommandErrors.typeMismatch("upsert must be a boolean");
        }

        final BsonDocument sort;
        final BsonValue sortValue = command.get("sort");
        if (sortValue == null) {
            sort = new BsonDocument();
        } else if (!sortValue.isDocument()) {
            return CommandErrors.typeMismatch("sort must be a document");
        } else {
            sort = sortValue.asDocument();
        }

        if (command.containsKey("fields") && command.containsKey("projection")) {
            return CommandErrors.badValue("fields and projection cannot both be specified");
        }
        final BsonValue fieldsValue = command.containsKey("projection")
                ? command.get("projection")
                : command.get("fields");
        final ProjectionSpec projectionSpec;
        if (fieldsValue == null) {
            projectionSpec = ProjectionSpec.none();
        } else if (!fieldsValue.isDocument()) {
            return CommandErrors.typeMismatch("projection must be a document");
        } else {
            try {
                projectionSpec = parseProjectionSpec(fieldsValue.asDocument());
            } catch (final IllegalArgumentException exception) {
                return CommandErrors.badValue(exception.getMessage());
            }
        }

        final BsonValue updateValue = command.get("update");
        if (remove) {
            if (updateValue != null) {
                return CommandErrors.badValue("update is not allowed when remove=true");
            }
            if (command.containsKey("new")) {
                return CommandErrors.badValue("new is not allowed when remove=true");
            }
            if (command.containsKey("upsert")) {
                return CommandErrors.badValue("upsert is not allowed when remove=true");
            }
        }
        if (!remove && (updateValue == null || !updateValue.isDocument())) {
            return CommandErrors.typeMismatch("update must be a document");
        }
        final BsonDocument update = updateValue != null && updateValue.isDocument() ? updateValue.asDocument() : null;
        final UpdateArrayFiltersSubset.ParseResult parsedArrayFilters =
                UpdateArrayFiltersSubset.parse(command.get("arrayFilters"));
        if (parsedArrayFilters.error() != null) {
            return parsedArrayFilters.error();
        }
        if (remove && !parsedArrayFilters.parsed().isEmpty()) {
            return CommandErrors.badValue("arrayFilters is not allowed when remove=true");
        }

        final List<BsonDocument> matches;
        try {
            matches = store.find(database, collection, query, collation);
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }

        final BsonDocument selected = firstMatched(matches, sort, collation);

        try {
            if (remove) {
                return handleRemove(database, collection, selected, projectionSpec);
            }
            return handleUpdate(
                    database,
                    collection,
                    query,
                    update,
                    selected,
                    upsert,
                    returnNew,
                    projectionSpec,
                    collation,
                    parsedArrayFilters.parsed().filters());
        } catch (final DuplicateKeyException exception) {
            return CommandErrors.duplicateKey(exception.getMessage());
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }
    }

    private BsonDocument handleRemove(
            final String database,
            final String collection,
            final BsonDocument selected,
            final ProjectionSpec projectionSpec) {
        if (selected == null) {
            return successResponse(0, false, null, null);
        }

        final int deleted = store.delete(
                database,
                collection,
                List.of(new CommandStore.DeleteRequest(singleDocumentFilter(selected), 1)));
        final BsonDocument value = deleted > 0 ? applyProjection(selected, projectionSpec) : null;
        return successResponse(deleted > 0 ? 1 : 0, false, null, value);
    }

    private BsonDocument handleUpdate(
            final String database,
            final String collection,
            final BsonDocument query,
            final BsonDocument update,
            final BsonDocument selected,
            final boolean upsert,
            final boolean returnNew,
            final ProjectionSpec projectionSpec,
            final CollationSupport.Config collation,
            final List<BsonDocument> arrayFilters) {
        if (selected != null) {
            final BsonDocument oneFilter = singleDocumentFilter(selected);
            final CommandStore.UpdateResult result = store.update(
                    database,
                    collection,
                    List.of(new CommandStore.UpdateRequest(oneFilter, update, false, false, arrayFilters)));
            final BsonDocument selectedValue = returnNew ? firstMatch(database, collection, oneFilter, collation) : selected;
            final BsonDocument value = applyProjection(selectedValue, projectionSpec);
            return successResponse(result.matchedCount() > 0 ? 1 : 0, result.matchedCount() > 0, null, value);
        }

        if (!upsert) {
            return successResponse(0, false, null, null);
        }

        final CommandStore.UpdateResult result = store.update(
                database,
                collection,
                List.of(new CommandStore.UpdateRequest(query, update, false, true, arrayFilters)));
        final BsonValue upsertedId =
                result.upserted().isEmpty() ? null : result.upserted().get(0).id();

        BsonDocument value = null;
        if (returnNew) {
            if (upsertedId != null) {
                value = firstMatch(database, collection, new BsonDocument("_id", upsertedId), collation);
            } else {
                value = firstMatch(database, collection, query, collation);
            }
            value = applyProjection(value, projectionSpec);
        }

        return successResponse(upsertedId == null ? 0 : 1, false, upsertedId, value);
    }

    private BsonDocument firstMatched(
            final List<BsonDocument> matches, final BsonDocument sort, final CollationSupport.Config collation) {
        if (matches.isEmpty()) {
            return null;
        }
        if (sort == null || sort.isEmpty()) {
            return matches.get(0);
        }

        final List<SortKey> sortKeys = readSortKeys(sort);
        final List<BsonDocument> sorted = new ArrayList<>(matches);
        sorted.sort((left, right) -> compareBySort(left, right, sortKeys, collation));
        return sorted.get(0);
    }

    private static List<SortKey> readSortKeys(final BsonDocument sort) {
        final List<SortKey> sortKeys = new ArrayList<>(sort.size());
        for (final String field : sort.keySet()) {
            final Long parsedDirection = readIntegralLong(sort.get(field));
            if (parsedDirection == null || (parsedDirection != 1L && parsedDirection != -1L)) {
                throw new IllegalArgumentException("sort directions must be 1 or -1");
            }
            sortKeys.add(new SortKey(field, parsedDirection.intValue()));
        }
        return List.copyOf(sortKeys);
    }

    private static int compareBySort(
            final BsonDocument left,
            final BsonDocument right,
            final List<SortKey> sortKeys,
            final CollationSupport.Config collation) {
        for (final SortKey sortKey : sortKeys) {
            final int compared =
                    compareSortValues(resolvePath(left, sortKey.field()), resolvePath(right, sortKey.field()), collation);
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
                final var array = current.asArray();
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

    private static int compareSortValues(
            final BsonValue left, final BsonValue right, final CollationSupport.Config collation) {
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
            return collation.compareStrings(left.asString().getValue(), right.asString().getValue());
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

    private BsonDocument firstMatch(
            final String database,
            final String collection,
            final BsonDocument filter,
            final CollationSupport.Config collation) {
        final List<BsonDocument> matched = store.find(database, collection, filter, collation);
        if (matched.isEmpty()) {
            return null;
        }
        return matched.get(0);
    }

    private static BsonDocument singleDocumentFilter(final BsonDocument document) {
        final BsonValue id = document.get("_id");
        if (id != null) {
            return new BsonDocument("_id", id);
        }
        return document.clone();
    }

    private static BsonDocument successResponse(
            final int n, final boolean updatedExisting, final BsonValue upsertedId, final BsonDocument value) {
        final BsonDocument lastErrorObject = new BsonDocument()
                .append("n", new BsonInt32(n))
                .append("updatedExisting", BsonBoolean.valueOf(updatedExisting));
        if (upsertedId != null) {
            lastErrorObject.append("upserted", upsertedId);
        }

        return new BsonDocument()
                .append("lastErrorObject", lastErrorObject)
                .append("value", value == null ? BsonNull.VALUE : value)
                .append("ok", new BsonDouble(1.0));
    }

    private static boolean readBoolean(final BsonValue value, final boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return value.isBoolean() && value.asBoolean().getValue();
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static String readCollection(final BsonDocument command) {
        final BsonValue canonical = command.get("findAndModify");
        if (canonical != null && canonical.isString()) {
            return canonical.asString().getValue();
        }

        for (final String key : command.keySet()) {
            if (!"findandmodify".equalsIgnoreCase(key)) {
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

    private static ProjectionSpec parseProjectionSpec(final BsonDocument projectionDocument) {
        if (projectionDocument.isEmpty()) {
            return ProjectionSpec.none();
        }

        final Set<String> includePaths = new LinkedHashSet<>();
        final Set<String> excludePaths = new LinkedHashSet<>();
        boolean includeId = true;

        for (final String field : projectionDocument.keySet()) {
            final ProjectionFlag flag = normalizeProjectionFlag(projectionDocument.get(field));
            if ("_id".equals(field)) {
                includeId = flag.include();
                continue;
            }
            if (flag.include()) {
                includePaths.add(field);
            } else {
                excludePaths.add(field);
            }
        }

        if (!includePaths.isEmpty() && !excludePaths.isEmpty()) {
            throw new IllegalArgumentException("projection cannot mix inclusion and exclusion except for _id");
        }
        if (!includePaths.isEmpty()) {
            return ProjectionSpec.include(includePaths, includeId);
        }
        if (!excludePaths.isEmpty()) {
            return ProjectionSpec.exclude(excludePaths, includeId);
        }
        return ProjectionSpec.none(includeId);
    }

    private static ProjectionFlag normalizeProjectionFlag(final BsonValue value) {
        if (value == null) {
            throw new IllegalArgumentException("projection values must be 0 or 1");
        }
        if (value.isBoolean()) {
            return new ProjectionFlag(value.asBoolean().getValue());
        }
        final Long parsed = readIntegralLong(value);
        if (parsed == null || (parsed != 0L && parsed != 1L)) {
            throw new IllegalArgumentException("projection values must be 0 or 1");
        }
        return new ProjectionFlag(parsed == 1L);
    }

    private static BsonDocument applyProjection(final BsonDocument value, final ProjectionSpec projectionSpec) {
        if (value == null) {
            return null;
        }
        if (projectionSpec.noProjection()) {
            return value;
        }

        if (projectionSpec.includeMode()) {
            final BsonDocument projected = new BsonDocument();
            if (projectionSpec.includeId()) {
                final BsonValue id = value.get("_id");
                if (id != null) {
                    projected.put("_id", id);
                }
            }
            for (final String includePath : projectionSpec.paths()) {
                final BsonValue projectedValue = resolveProjectionPath(value, includePath);
                if (projectedValue != null) {
                    setProjectionPath(projected, includePath, projectedValue);
                }
            }
            if (!projectionSpec.includeId()) {
                projected.remove("_id");
            }
            return projected;
        }

        final BsonDocument projected = value.clone();
        for (final String excludedPath : projectionSpec.paths()) {
            removeProjectionPath(projected, excludedPath);
        }
        if (!projectionSpec.includeId()) {
            projected.remove("_id");
        }
        return projected;
    }

    private static BsonValue resolveProjectionPath(final BsonDocument source, final String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        final String[] segments = path.split("\\.");
        BsonValue current = source;
        for (final String segment : segments) {
            if (current == null || !current.isDocument()) {
                return null;
            }
            current = current.asDocument().get(segment);
        }
        return current;
    }

    private static void setProjectionPath(final BsonDocument target, final String path, final BsonValue value) {
        final String[] segments = path.split("\\.");
        BsonDocument current = target;
        for (int index = 0; index < segments.length - 1; index++) {
            final String segment = segments[index];
            final BsonValue existing = current.get(segment);
            if (existing != null && !existing.isDocument()) {
                return;
            }
            if (existing == null) {
                final BsonDocument child = new BsonDocument();
                current.put(segment, child);
                current = child;
            } else {
                current = existing.asDocument();
            }
        }
        current.put(segments[segments.length - 1], value);
    }

    private static void removeProjectionPath(final BsonDocument target, final String path) {
        final String[] segments = path.split("\\.");
        BsonDocument current = target;
        for (int index = 0; index < segments.length - 1; index++) {
            final BsonValue next = current.get(segments[index]);
            if (next == null || !next.isDocument()) {
                return;
            }
            current = next.asDocument();
        }
        current.remove(segments[segments.length - 1]);
    }

    private record ProjectionFlag(boolean include) {}
    private record ProjectionSpec(Set<String> paths, boolean includeMode, boolean includeId, boolean noProjection) {
        private static ProjectionSpec include(final Set<String> paths, final boolean includeId) {
            return new ProjectionSpec(Set.copyOf(paths), true, includeId, false);
        }

        private static ProjectionSpec exclude(final Set<String> paths, final boolean includeId) {
            return new ProjectionSpec(Set.copyOf(paths), false, includeId, false);
        }

        private static ProjectionSpec none() {
            return none(true);
        }

        private static ProjectionSpec none(final boolean includeId) {
            return new ProjectionSpec(Set.of(), false, includeId, true);
        }
    }

    private record SortKey(String field, int direction) {}
}
