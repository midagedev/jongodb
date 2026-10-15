package org.jongodb.command;

import java.util.List;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

/**
 * Helpers shared by the administrative and diagnostic command handlers.
 */
final class AdminCommandSupport {
    /** Fields drivers attach to every command; they never change a command's meaning here. */
    static final Set<String> ENVELOPE_FIELDS = Set.of(
            "$db",
            "$clusterTime",
            "$readPreference",
            "lsid",
            "txnNumber",
            "autocommit",
            "startTransaction",
            "readConcern",
            "writeConcern",
            "comment",
            "maxTimeMS",
            "apiVersion",
            "apiStrict",
            "apiDeprecationErrors");

    private static final BsonDocumentCodec DOCUMENT_CODEC = new BsonDocumentCodec();

    private AdminCommandSupport() {}

    static String string(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        return value != null && value.isString() ? value.asString().getValue() : null;
    }

    static boolean collectionExists(final CommandStore store, final String database, final String collection) {
        for (final CommandStore.CollectionMetadata metadata : store.listCollections(database)) {
            if (metadata.name().equals(collection)) {
                return true;
            }
        }
        return false;
    }

    static boolean isValidCollectionName(final String name) {
        return name != null && !name.isEmpty() && name.indexOf('$') < 0 && name.indexOf('\0') < 0;
    }

    static long bsonSize(final BsonDocument document) {
        return new RawBsonDocument(document, DOCUMENT_CODEC).getByteBuffer().remaining();
    }

    static BsonDocument ok(final BsonDocument document) {
        return document.append("ok", new BsonDouble(1.0));
    }

    /** Scale divisor for stats commands; falls back to 1 for non-positive or non-numeric input. */
    static double scale(final BsonValue value) {
        if (value != null && value.isNumber() && value.asNumber().doubleValue() >= 1) {
            return value.asNumber().doubleValue();
        }
        return 1.0;
    }

    static BsonValue scaled(final long bytes, final double scale) {
        return new BsonInt64((long) (bytes / scale));
    }

    /** Collection statistics in the {@code collStats} shape; sizes derive from the BSON size of live documents. */
    static BsonDocument collectionStats(
            final CommandStore store, final String database, final String collection, final double scale) {
        final List<BsonDocument> documents = store.find(database, collection, new BsonDocument());
        long size = 0;
        for (final BsonDocument document : documents) {
            size += bsonSize(document);
        }
        final List<CommandStore.IndexMetadata> indexes = store.listIndexes(database, collection);
        final BsonDocument indexSizes = new BsonDocument();
        for (final CommandStore.IndexMetadata index : indexes) {
            indexSizes.append(index.name(), new BsonInt32(0));
        }
        final int count = documents.size();
        return new BsonDocument()
                .append("ns", new BsonString(database + "." + collection))
                .append("size", scaled(size, scale))
                .append("count", new BsonInt32(count))
                .append("avgObjSize", new BsonInt32(count == 0 ? 0 : (int) (size / count)))
                // ponytail: no on-disk layout exists, so storageSize mirrors the logical data size and index sizes are 0
                .append("storageSize", scaled(size, scale))
                .append("freeStorageSize", new BsonInt32(0))
                .append("capped", org.bson.BsonBoolean.FALSE)
                .append("nindexes", new BsonInt32(indexes.size()))
                .append("totalIndexSize", new BsonInt32(0))
                .append("totalSize", scaled(size, scale))
                .append("indexSizes", indexSizes)
                .append("scaleFactor", new BsonDouble(scale));
    }
}
