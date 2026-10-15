package org.jongodb.command;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
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
        if (!remove && (updateValue == null || (!updateValue.isDocument() && !updateValue.isArray()))) {
            return CommandErrors.typeMismatch("update must be a document or array");
        }
        final BsonDocument update = updateValue != null && updateValue.isDocument() ? updateValue.asDocument() : null;
        final BsonArray updatePipeline;
        if (updateValue != null && updateValue.isArray()) {
            final UpdatePipelineSubset.ParseResult parsedPipeline = UpdatePipelineSubset.parse(updateValue.asArray());
            if (parsedPipeline.error() != null) {
                return parsedPipeline.error();
            }
            updatePipeline = parsedPipeline.updatePipeline();
        } else {
            updatePipeline = null;
        }
        final UpdateArrayFiltersSubset.ParseResult parsedArrayFilters =
                UpdateArrayFiltersSubset.parse(command.get("arrayFilters"));
        if (parsedArrayFilters.error() != null) {
            return parsedArrayFilters.error();
        }
        if (remove && !parsedArrayFilters.parsed().isEmpty()) {
            return CommandErrors.badValue("arrayFilters is not allowed when remove=true");
        }
        if (updatePipeline != null && !parsedArrayFilters.parsed().isEmpty()) {
            return CommandErrors.badValue("arrayFilters is not allowed with pipeline updates");
        }

        try {
            final CommandStore.FindAndModifyOutcome outcome = store.findAndModify(
                    database,
                    collection,
                    new CommandStore.FindAndModifyRequest(
                            query,
                            sort,
                            collation,
                            remove,
                            update,
                            updatePipeline,
                            upsert,
                            parsedArrayFilters.parsed().filters()));
            if (remove) {
                final BsonDocument value =
                        outcome.before() == null ? null : applyProjection(outcome.before(), projectionSpec);
                return removeSuccessResponse(outcome.matched(), value);
            }
            final BsonDocument raw = returnNew ? outcome.after() : outcome.before();
            return successResponse(
                    outcome.matched(),
                    outcome.updatedExisting(),
                    outcome.upsertedId(),
                    applyProjection(raw, projectionSpec));
        } catch (final DuplicateKeyException exception) {
            return CommandErrors.duplicateKey(exception.getMessage());
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }
    }

    static BsonDocument firstMatched(
            final List<BsonDocument> matches, final BsonDocument sort, final CollationSupport.Config collation) {
        if (matches.isEmpty()) {
            return null;
        }
        if (sort == null || sort.isEmpty()) {
            return matches.get(0);
        }

        return FindCommandHandler.sorted(matches, sort, collation).get(0);
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

    private static BsonDocument removeSuccessResponse(final int n, final BsonDocument value) {
        return new BsonDocument()
                .append("lastErrorObject", new BsonDocument("n", new BsonInt32(n)))
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

}
