package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class ListIndexesCommandHandler implements CommandHandler {
    private final CommandStore store;
    private final CursorRegistry cursorRegistry;

    public ListIndexesCommandHandler(final CommandStore store, final CursorRegistry cursorRegistry) {
        this.store = store;
        this.cursorRegistry = cursorRegistry;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "listIndexes");
        if (collection == null) {
            return CommandErrors.typeMismatch("listIndexes must be a string");
        }

        final BsonValue cursorValue = command.get("cursor");
        int batchSize = Integer.MAX_VALUE;
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

        final String namespace = database + "." + collection;
        final List<BsonDocument> indexDocuments = new ArrayList<>();
        for (final CommandStore.IndexMetadata index : store.listIndexes(database, collection)) {
            final BsonDocument metadata = new BsonDocument()
                    .append("v", new BsonInt32(2))
                    .append("key", index.key().clone())
                    .append("name", new BsonString(index.name()))
                    .append("ns", new BsonString(namespace));
            if (index.unique()) {
                metadata.append("unique", BsonBoolean.TRUE);
            }
            if (index.sparse()) {
                metadata.append("sparse", BsonBoolean.TRUE);
            }
            if (index.partialFilterExpression() != null) {
                metadata.append("partialFilterExpression", index.partialFilterExpression().clone());
            }
            if (index.expireAfterSeconds() != null) {
                metadata.append("expireAfterSeconds", new BsonInt64(index.expireAfterSeconds()));
            }
            indexDocuments.add(metadata);
        }

        final CursorRegistry.FindRegistration registration =
                cursorRegistry.openCursor(namespace, List.copyOf(indexDocuments), batchSize);
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
