package org.jongodb.command;

import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public interface CommandStore {
    int insert(String database, String collection, List<BsonDocument> documents);

    List<BsonDocument> find(String database, String collection, BsonDocument filter);

    /**
     * Default fallback for test doubles that do not yet model aggregation pipelines.
     */
    default List<BsonDocument> aggregate(String database, String collection, List<BsonDocument> pipeline) {
        return find(database, collection, new BsonDocument());
    }

    /**
     * Creates a transaction-local snapshot for command execution.
     *
     * <p>The default implementation returns this store, which preserves backward compatibility for lightweight
     * test doubles that do not model transactional isolation.
     */
    default CommandStore snapshotForTransaction() {
        return this;
    }

    /**
     * Publishes a transaction snapshot into this store.
     *
     * <p>The default implementation is a no-op to preserve backward compatibility for lightweight test doubles.
     */
    default void publishTransactionSnapshot(CommandStore snapshot) {}

    /**
     * Default no-op for backward compatibility in tests/doubles that do not care about indexes.
     */
    default CreateIndexesResult createIndexes(String database, String collection, List<IndexRequest> indexes) {
        return new CreateIndexesResult(0, 0);
    }

    /**
     * Default no-op for backward compatibility in tests/doubles that do not care about index metadata.
     */
    default List<IndexMetadata> listIndexes(String database, String collection) {
        return List.of();
    }

    /**
     * Default no-op for backward compatibility in tests/doubles that do not care about updates.
     */
    default UpdateResult update(String database, String collection, List<UpdateRequest> updates) {
        return new UpdateResult(0, 0, List.of());
    }

    /**
     * Default no-op for backward compatibility in tests/doubles that do not care about deletes.
     */
    default int delete(String database, String collection, List<DeleteRequest> deletes) {
        return 0;
    }

    record IndexRequest(
            String name,
            BsonDocument key,
            boolean unique,
            boolean sparse,
            BsonDocument partialFilterExpression,
            BsonDocument collation,
            Long expireAfterSeconds) {
        public IndexRequest(final String name, final BsonDocument key, final boolean unique) {
            this(name, key, unique, false, null, null, null);
        }

        public IndexRequest(
                final String name,
                final BsonDocument key,
                final boolean unique,
                final boolean sparse,
                final BsonDocument partialFilterExpression,
                final Long expireAfterSeconds) {
            this(name, key, unique, sparse, partialFilterExpression, null, expireAfterSeconds);
        }
    }

    record CreateIndexesResult(int numIndexesBefore, int numIndexesAfter) {}

    record IndexMetadata(
            String name,
            BsonDocument key,
            boolean unique,
            boolean sparse,
            BsonDocument partialFilterExpression,
            BsonDocument collation,
            Long expireAfterSeconds) {
        public IndexMetadata(
                final String name,
                final BsonDocument key,
                final boolean unique,
                final boolean sparse,
                final BsonDocument partialFilterExpression,
                final Long expireAfterSeconds) {
            this(name, key, unique, sparse, partialFilterExpression, null, expireAfterSeconds);
        }
    }

    record UpdateRequest(BsonDocument query, BsonDocument update, boolean multi, boolean upsert) {}

    record DeleteRequest(BsonDocument query, int limit) {}

    record Upserted(int index, BsonValue id) {}

    record UpdateResult(int matchedCount, int modifiedCount, List<Upserted> upserted) {
        public UpdateResult(final int matchedCount, final int modifiedCount) {
            this(matchedCount, modifiedCount, List.of());
        }

        public UpdateResult {
            upserted = upserted == null ? List.of() : List.copyOf(upserted);
        }
    }
}
