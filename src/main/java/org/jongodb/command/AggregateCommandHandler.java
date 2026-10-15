package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.engine.CollationSupport;

public final class AggregateCommandHandler implements CommandHandler {
    private static final java.util.Set<String> DATABASE_STAGES =
            java.util.Set.of("$currentOp", "$listLocalSessions", "$listSessions");

    private final CommandStore store;
    private final CursorRegistry cursorRegistry;
    private final ChangeStreamCursors changeStreams;

    public AggregateCommandHandler(final CommandStore store, final CursorRegistry cursorRegistry) {
        this(store, cursorRegistry, null);
    }

    AggregateCommandHandler(
            final CommandStore store, final CursorRegistry cursorRegistry, final ChangeStreamCursors changeStreams) {
        this.store = store;
        this.cursorRegistry = cursorRegistry;
        this.changeStreams = changeStreams;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        if (changeStreams != null && ChangeStreamCursors.isChangeStream(command)) {
            return changeStreams.open(database, command);
        }
        final BsonValue target = command.get("aggregate");
        if (target != null && target.isNumber() && target.asNumber().intValue() == 1) {
            return databaseAggregate(database, command);
        }
        final String collection = readRequiredString(command, "aggregate");
        if (collection == null) {
            return CommandErrors.typeMismatch("aggregate must be a string");
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
        final CollationSupport.Config collation = CrudCommandOptionValidator.collationOrSimple(command, "collation");

        final BsonValue pipelineValue = command.get("pipeline");
        if (pipelineValue == null || !pipelineValue.isArray()) {
            return CommandErrors.typeMismatch("pipeline must be an array");
        }

        final List<BsonDocument> pipeline = new ArrayList<>();
        for (final BsonValue stageValue : pipelineValue.asArray()) {
            if (!stageValue.isDocument()) {
                return CommandErrors.typeMismatch("all entries in pipeline must be BSON documents");
            }
            pipeline.add(stageValue.asDocument());
        }

        int batchSize = Integer.MAX_VALUE;
        final BsonValue cursorValue = command.get("cursor");
        if (cursorValue != null) {
            if (!cursorValue.isDocument()) {
                return CommandErrors.typeMismatch("cursor must be a document");
            }
            final BsonValue batchSizeValue = cursorValue.asDocument().get("batchSize");
            if (batchSizeValue != null) {
                final Long parsedBatchSize = readIntegralLong(batchSizeValue);
                if (parsedBatchSize == null) {
                    return CommandErrors.typeMismatch("cursor.batchSize must be an integer");
                }
                if (parsedBatchSize < 0 || parsedBatchSize > Integer.MAX_VALUE) {
                    return CommandErrors.badValue("cursor.batchSize must be a non-negative integer");
                }
                batchSize = parsedBatchSize.intValue();
            }
        }

        final List<BsonDocument> aggregatedDocuments;
        try {
            if (!pipeline.isEmpty() && pipeline.get(0).containsKey("$collStats")) {
                final BsonDocument error = collStatsStage(database, collection, pipeline);
                if (error != null) {
                    return error;
                }
            }
            aggregatedDocuments = !pipeline.isEmpty() && pipeline.get(0).containsKey("$collStats")
                    ? collStatsDocuments(database, collection, pipeline.get(0).get("$collStats"))
                    : store.aggregate(database, collection, List.copyOf(pipeline), collation);
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }

        if (batchSize == Integer.MAX_VALUE) {
            batchSize = aggregatedDocuments.size();
        }

        final String namespace = database + "." + collection;
        final CursorRegistry.FindRegistration registration =
                cursorRegistry.openCursor(namespace, aggregatedDocuments, batchSize);

        final BsonArray firstBatch = new BsonArray();
        for (final BsonDocument document : registration.firstBatch()) {
            firstBatch.add(document);
        }

        final BsonDocument cursor = new BsonDocument()
                .append("id", new BsonInt64(registration.cursorId()))
                .append("ns", new BsonString(namespace))
                .append("firstBatch", firstBatch);

        return new BsonDocument()
                .append("cursor", cursor)
                .append("ok", new BsonDouble(1.0));
    }

    /**
     * Database-level {@code aggregate: 1}. Only the introspection stages are known; none of them has anything to
     * report because operations finish synchronously and sessions are not tracked beyond transactions.
     */
    private BsonDocument databaseAggregate(final String database, final BsonDocument command) {
        final BsonValue pipelineValue = command.get("pipeline");
        if (pipelineValue == null || !pipelineValue.isArray()) {
            return CommandErrors.typeMismatch("pipeline must be an array");
        }
        final BsonValue first = pipelineValue.asArray().isEmpty() ? null : pipelineValue.asArray().get(0);
        if (first == null || !first.isDocument() || !DATABASE_STAGES.contains(first.asDocument().getFirstKey())) {
            return CommandErrors.badValue("aggregate: 1 requires a $currentOp, $listLocalSessions or $listSessions stage");
        }
        final String namespace = database + ".$cmd.aggregate";
        final CursorRegistry.FindRegistration registration = cursorRegistry.openCursor(namespace, List.of(), 0);
        return new BsonDocument()
                .append("cursor", new BsonDocument()
                        .append("id", new BsonInt64(registration.cursorId()))
                        .append("ns", new BsonString(namespace))
                        .append("firstBatch", new BsonArray()))
                .append("ok", new BsonDouble(1.0));
    }

    private BsonDocument collStatsStage(final String database, final String collection, final List<BsonDocument> pipeline) {
        if (pipeline.size() > 1) {
            return CommandErrors.notImplemented("stages after $collStats are not supported");
        }
        if (!AdminCommandSupport.collectionExists(store, database, collection)) {
            return CommandErrors.namespaceNotFound(database + "." + collection);
        }
        return null;
    }

    private List<BsonDocument> collStatsDocuments(final String database, final String collection, final BsonValue options) {
        final BsonDocument spec = options.isDocument() ? options.asDocument() : new BsonDocument();
        final BsonDocument stats = AdminCommandSupport.collectionStats(store, database, collection, 1.0);
        final BsonDocument result = new BsonDocument()
                .append("ns", new BsonString(database + "." + collection))
                .append("localTime", new org.bson.BsonDateTime(System.currentTimeMillis()));
        if (spec.containsKey("count")) {
            result.append("count", stats.get("count"));
        }
        if (spec.containsKey("storageStats")) {
            final double scale = spec.get("storageStats").isDocument()
                    ? AdminCommandSupport.scale(spec.getDocument("storageStats").get("scale"))
                    : 1.0;
            final BsonDocument storage = AdminCommandSupport.collectionStats(store, database, collection, scale);
            storage.remove("ns");
            result.append("storageStats", storage);
        }
        return List.of(result);
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
