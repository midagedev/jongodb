package org.jongodb.command;

import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jongodb.engine.CollationSupport;

public interface CommandStore {
    int insert(String database, String collection, List<BsonDocument> documents);

    List<BsonDocument> find(String database, String collection, BsonDocument filter);

    default List<BsonDocument> find(
            final String database,
            final String collection,
            final BsonDocument filter,
            final CollationSupport.Config collation) {
        return find(database, collection, filter);
    }

    /**
     * Default fallback for test doubles that do not yet model aggregation pipelines.
     */
    default List<BsonDocument> aggregate(String database, String collection, List<BsonDocument> pipeline) {
        return find(database, collection, new BsonDocument());
    }

    default List<BsonDocument> aggregate(
            final String database,
            final String collection,
            final List<BsonDocument> pipeline,
            final CollationSupport.Config collation) {
        return aggregate(database, collection, pipeline);
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
     * Clears store state for test-only fast reset flows.
     *
     * <p>The default implementation is a no-op to preserve backward compatibility for lightweight test doubles.
     */
    default void reset() {}

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

    record CreateIndexesResult(int numIndexesBefore, int numIndexesAfter, boolean createdCollectionAutomatically) {
        public CreateIndexesResult(final int numIndexesBefore, final int numIndexesAfter) {
            this(numIndexesBefore, numIndexesAfter, false);
        }
    }

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

    record UpdateRequest(
            BsonDocument query, BsonDocument update, boolean multi, boolean upsert, List<BsonDocument> arrayFilters) {
        public UpdateRequest(final BsonDocument query, final BsonDocument update, final boolean multi, final boolean upsert) {
            this(query, update, multi, upsert, List.of());
        }

        public UpdateRequest {
            arrayFilters = copyArrayFilters(arrayFilters);
        }

        private static List<BsonDocument> copyArrayFilters(final List<BsonDocument> source) {
            if (source == null || source.isEmpty()) {
                return List.of();
            }
            final List<BsonDocument> copied = new java.util.ArrayList<>(source.size());
            for (final BsonDocument filter : source) {
                if (filter == null) {
                    throw new IllegalArgumentException("arrayFilters entries must not be null");
                }
                copied.add(filter.clone());
            }
            return List.copyOf(copied);
        }
    }

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
