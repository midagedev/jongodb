package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class AggregateCommandHandler implements CommandHandler {
    private final CommandStore store;
    private final CursorRegistry cursorRegistry;

    public AggregateCommandHandler(final CommandStore store, final CursorRegistry cursorRegistry) {
        this.store = store;
        this.cursorRegistry = cursorRegistry;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
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
            aggregatedDocuments = store.aggregate(database, collection, List.copyOf(pipeline));
        } catch (final IllegalArgumentException exception) {
            return CommandErrors.badValue(exception.getMessage());
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
