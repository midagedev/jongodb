package org.jongodb.command;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class ListDatabasesCommandHandler implements CommandHandler {
    private final CommandStore store;

    public ListDatabasesCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final BsonValue nameOnlyValue = command.get("nameOnly");
        if (nameOnlyValue != null && !nameOnlyValue.isBoolean()) {
            return CommandErrors.typeMismatch("nameOnly must be a boolean");
        }
        final boolean nameOnly = nameOnlyValue != null && nameOnlyValue.asBoolean().getValue();
        final BsonValue filterValue = command.get("filter");
        if (filterValue != null && !filterValue.isDocument()) {
            return CommandErrors.typeMismatch("filter must be a document");
        }
        final BsonDocument filter = filterValue == null ? new BsonDocument() : filterValue.asDocument();

        final BsonArray databases = new BsonArray();
        long totalSize = 0;
        for (final String database : store.listDatabaseNames()) {
            long size = 0;
            for (final CommandStore.CollectionMetadata collection : store.listCollections(database)) {
                for (final BsonDocument document : store.find(database, collection.name(), new BsonDocument())) {
                    size += AdminCommandSupport.bsonSize(document);
                }
            }
            // ponytail: sizeOnDisk is the logical BSON size, there is no disk
            final BsonDocument entry = new BsonDocument()
                    .append("name", new BsonString(database))
                    .append("sizeOnDisk", new BsonInt64(size))
                    .append("empty", BsonBoolean.valueOf(size == 0));
            if (!matches(entry, filter)) {
                continue;
            }
            totalSize += size;
            databases.add(nameOnly ? new BsonDocument("name", new BsonString(database)) : entry);
        }

        final BsonDocument response = new BsonDocument().append("databases", databases);
        if (!nameOnly) {
            response.append("totalSize", new BsonInt64(totalSize)).append("totalSizeMb", new BsonInt64(totalSize >> 20));
        }
        return AdminCommandSupport.ok(response);
    }

    private static boolean matches(final BsonDocument entry, final BsonDocument filter) {
        for (final String key : filter.keySet()) {
            final BsonValue actual = entry.get(key);
            final BsonValue expected = filter.get(key);
            if (actual == null || !(actual.equals(expected) || numericEquals(actual, expected))) {
                return false;
            }
        }
        return true;
    }

    private static boolean numericEquals(final BsonValue left, final BsonValue right) {
        return left.isNumber() && right.isNumber() && left.asNumber().doubleValue() == right.asNumber().doubleValue();
    }
}
