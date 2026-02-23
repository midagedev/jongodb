package org.jongodb.command;

import java.util.List;
import org.bson.BsonDocument;

public interface CommandStore {
    int insert(String database, String collection, List<BsonDocument> documents);

    List<BsonDocument> find(String database, String collection, BsonDocument filter);

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
     * Default no-op for backward compatibility in tests/doubles that do not care about updates.
     */
    default UpdateResult update(String database, String collection, List<UpdateRequest> updates) {
        return new UpdateResult(0, 0);
    }

    /**
     * Default no-op for backward compatibility in tests/doubles that do not care about deletes.
     */
    default int delete(String database, String collection, List<DeleteRequest> deletes) {
        return 0;
    }

    record IndexRequest(String name, BsonDocument key, boolean unique) {}

    record CreateIndexesResult(int numIndexesBefore, int numIndexesAfter) {}

    record UpdateRequest(BsonDocument query, BsonDocument update, boolean multi) {}

    record DeleteRequest(BsonDocument query, int limit) {}

    record UpdateResult(int matchedCount, int modifiedCount) {}
}
