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

public final class ListCollectionsCommandHandler implements CommandHandler {
    private final CommandStore store;
    private final CursorRegistry cursorRegistry;

    public ListCollectionsCommandHandler(final CommandStore store, final CursorRegistry cursorRegistry) {
        this.store = store;
        this.cursorRegistry = cursorRegistry;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);

        final BsonValue filterValue = command.get("filter");
        final BsonDocument filter;
        if (filterValue == null) {
            filter = new BsonDocument();
        } else if (filterValue.isDocument()) {
            filter = filterValue.asDocument();
        } else {
            return CommandErrors.typeMismatch("filter must be a document");
        }

        final BsonValue nameOnlyValue = command.get("nameOnly");
        final boolean nameOnly;
        if (nameOnlyValue == null) {
            nameOnly = false;
        } else if (!nameOnlyValue.isBoolean()) {
            return CommandErrors.typeMismatch("nameOnly must be a boolean");
        } else {
            nameOnly = nameOnlyValue.asBoolean().getValue();
        }

        final BsonValue authorizedCollectionsValue = command.get("authorizedCollections");
        if (authorizedCollectionsValue != null && !authorizedCollectionsValue.isBoolean()) {
            return CommandErrors.typeMismatch("authorizedCollections must be a boolean");
        }

        final CursorBatch cursorBatch = parseCursorBatch(command.get("cursor"));
        if (cursorBatch.error() != null) {
            return cursorBatch.error();
        }

        final List<BsonDocument> collectionDocuments = new ArrayList<>();
        for (final CommandStore.CollectionMetadata collection : store.listCollections(database)) {
            final BsonDocument metadata = collectionDocument(collection.name(), nameOnly);
            if (matchesFilter(metadata, filter)) {
                collectionDocuments.add(metadata);
            }
        }

        final String namespace = database + ".$cmd.listCollections";
        final CursorRegistry.FindRegistration registration =
                cursorRegistry.openCursor(namespace, List.copyOf(collectionDocuments), cursorBatch.batchSize());
        final BsonArray firstBatch = new BsonArray();
        for (final BsonDocument document : registration.firstBatch()) {
            firstBatch.add(document);
        }

        return new BsonDocument()
                .append("cursor", new BsonDocument()
                        .append("id", new BsonInt64(registration.cursorId()))
                        .append("ns", new BsonString(namespace))
                        .append("firstBatch", firstBatch))
                .append("ok", new BsonDouble(1.0));
    }

    private static BsonDocument collectionDocument(final String name, final boolean nameOnly) {
        final BsonDocument document = new BsonDocument()
                .append("name", new BsonString(name))
                .append("type", new BsonString("collection"));
        if (!nameOnly) {
            document.append("options", new BsonDocument())
                    .append("info", new BsonDocument("readOnly", BsonBoolean.FALSE))
                    .append("idIndex", new BsonDocument()
                            .append("v", new BsonInt32(2))
                            .append("key", new BsonDocument("_id", new BsonInt32(1)))
                            .append("name", new BsonString("_id_")));
        }
        return document;
    }

    private static boolean matchesFilter(final BsonDocument document, final BsonDocument filter) {
        if (filter == null || filter.isEmpty()) {
            return true;
        }
        for (final String key : filter.keySet()) {
            final BsonValue expected = filter.get(key);
            final BsonValue actual = valueAtPath(document, key);
            if (actual == null || !actual.equals(expected)) {
                return false;
            }
        }
        return true;
    }

    private static BsonValue valueAtPath(final BsonDocument document, final String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        BsonValue current = document;
        for (final String segment : path.split("\\.")) {
            if (!current.isDocument()) {
                return null;
            }
            current = current.asDocument().get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static CursorBatch parseCursorBatch(final BsonValue cursorValue) {
        if (cursorValue == null) {
            return new CursorBatch(Integer.MAX_VALUE, null);
        }
        if (!cursorValue.isDocument()) {
            return new CursorBatch(0, CommandErrors.typeMismatch("cursor must be a document"));
        }
        final BsonValue batchSizeValue = cursorValue.asDocument().get("batchSize");
        if (batchSizeValue == null) {
            return new CursorBatch(Integer.MAX_VALUE, null);
        }
        final Long parsedBatchSize = readIntegralLong(batchSizeValue);
        if (parsedBatchSize == null) {
            return new CursorBatch(0, CommandErrors.typeMismatch("cursor.batchSize must be an integer"));
        }
        if (parsedBatchSize < 0 || parsedBatchSize > Integer.MAX_VALUE) {
            return new CursorBatch(0, CommandErrors.badValue("cursor.batchSize must be a non-negative integer"));
        }
        return new CursorBatch(parsedBatchSize.intValue(), null);
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
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

    private record CursorBatch(int batchSize, BsonDocument error) {}
}
