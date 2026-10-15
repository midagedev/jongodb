package org.jongodb.command;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

/**
 * {@code dbStats} and {@code collStats}. Sizes are logical BSON sizes of live documents.
 */
final class StatsCommandHandlers {
    private StatsCommandHandlers() {}

    static CommandHandler dbStats(final CommandStore store) {
        return command -> {
            final String database = CommandCanonicalizer.readDatabase(command);
            final double scale = AdminCommandSupport.scale(command.get("scale"));
            long objects = 0;
            long dataSize = 0;
            int indexes = 0;
            int collections = 0;
            for (final CommandStore.CollectionMetadata collection : store.listCollections(database)) {
                final BsonDocument stats = AdminCommandSupport.collectionStats(store, database, collection.name(), 1.0);
                collections++;
                objects += stats.getInt32("count").getValue();
                dataSize += stats.getInt64("size").getValue();
                indexes += stats.getInt32("nindexes").getValue();
            }
            return AdminCommandSupport.ok(new BsonDocument()
                    .append("db", new BsonString(database))
                    .append("collections", new BsonInt32(collections))
                    .append("views", new BsonInt32(0))
                    .append("objects", new BsonInt64(objects))
                    .append("avgObjSize", new BsonDouble(objects == 0 ? 0 : (double) dataSize / objects))
                    .append("dataSize", AdminCommandSupport.scaled(dataSize, scale))
                    .append("storageSize", AdminCommandSupport.scaled(dataSize, scale))
                    .append("indexes", new BsonInt32(indexes))
                    .append("indexSize", new BsonInt32(0))
                    .append("totalSize", AdminCommandSupport.scaled(dataSize, scale))
                    .append("scaleFactor", new BsonDouble(scale))
                    .append("fsUsedSize", new BsonInt32(0))
                    .append("fsTotalSize", new BsonInt32(0)));
        };
    }

    static CommandHandler collStats(final CommandStore store) {
        return command -> {
            final String database = CommandCanonicalizer.readDatabase(command);
            final String collection = AdminCommandSupport.string(command, "collStats");
            if (collection == null) {
                return CommandErrors.typeMismatch("collStats must be a string");
            }
            if (!AdminCommandSupport.collectionExists(store, database, collection)) {
                return CommandErrors.namespaceNotFound(database + "." + collection);
            }
            return AdminCommandSupport.ok(AdminCommandSupport.collectionStats(
                    store, database, collection, AdminCommandSupport.scale(command.get("scale"))));
        };
    }
}
