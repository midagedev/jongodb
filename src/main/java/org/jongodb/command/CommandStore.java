package org.jongodb.command;

import java.util.List;
import java.util.Objects;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jongodb.engine.CollationSupport;

public interface CommandStore {
    int insert(String database, String collection, List<BsonDocument> documents);

    default List<CollectionMetadata> listCollections(final String database) {
        return List.of();
    }

    default DropCollectionResult dropCollection(final String database, final String collection) {
        return new DropCollectionResult(false, 0);
    }

    default int dropDatabase(final String database) {
        return 0;
    }

    default List<String> listDatabaseNames() {
        return List.of();
    }

    /** Creates an empty collection; returns false when it already exists. */
    default boolean createCollection(final String database, final String collection) {
        return false;
    }

    default void dropIndexes(final String database, final String collection, final List<String> names) {}

    /** Changes the TTL of an existing index and returns the previous value ({@code null} when it had none). */
    default Long modifyIndexTtl(
            final String database, final String collection, final String indexName, final Long expireAfterSeconds) {
        return null;
    }

    default void renameCollection(
            final String fromDatabase,
            final String fromCollection,
            final String toDatabase,
            final String toCollection,
            final boolean dropTarget) {}

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
    /** The live engine behind this store; {@code null} for transaction snapshots and non-engine test doubles. */
    default org.jongodb.engine.InMemoryEngineStore liveEngine() {
        return null;
    }

    /** Change journal backing {@code $changeStream}; {@code null} when this store does not record changes. */
    default org.jongodb.engine.ChangeEventLog changeEventLog() {
        return null;
    }

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

    /**
     * Non-atomic find-then-update for stores that do not override this method.
     * Engine-backed stores replace it with one locked read-modify-write.
     */
    default FindAndModifyOutcome findAndModify(
            final String database, final String collection, final FindAndModifyRequest request) {
        Objects.requireNonNull(request, "request");
        final List<BsonDocument> matches = find(database, collection, request.query(), request.collation());
        final BsonDocument selected =
                FindAndModifyCommandHandler.firstMatched(matches, request.sort(), request.collation());
        if (request.remove()) {
            if (selected == null) {
                return FindAndModifyOutcome.removed(null);
            }
            final BsonDocument oneFilter = findAndModifyDocumentFilter(selected);
            final int deleted = delete(database, collection, List.of(new DeleteRequest(oneFilter, 1)));
            return FindAndModifyOutcome.removed(deleted > 0 ? selected : null);
        }
        if (selected != null) {
            final BsonDocument oneFilter = findAndModifyDocumentFilter(selected);
            final UpdateResult result =
                    update(database, collection, List.of(findAndModifyUpdateRequest(request, oneFilter, false)));
            final BsonDocument after = findAndModifyFirst(database, collection, oneFilter, request.collation());
            final int matched = result.matchedCount() > 0 ? 1 : 0;
            return new FindAndModifyOutcome(selected, after, matched, matched > 0, null);
        }
        if (!request.upsert()) {
            return FindAndModifyOutcome.miss();
        }
        final UpdateResult result =
                update(database, collection, List.of(findAndModifyUpdateRequest(request, request.query(), true)));
        final BsonValue upsertedId = result.upserted().isEmpty() ? null : result.upserted().get(0).id();
        final BsonDocument after = upsertedId != null
                ? findAndModifyFirst(
                        database, collection, new BsonDocument("_id", upsertedId), request.collation())
                : findAndModifyFirst(database, collection, request.query(), request.collation());
        return new FindAndModifyOutcome(null, after, upsertedId == null ? 0 : 1, false, upsertedId);
    }

    private BsonDocument findAndModifyFirst(
            final String database,
            final String collection,
            final BsonDocument filter,
            final CollationSupport.Config collation) {
        final List<BsonDocument> matched = find(database, collection, filter, collation);
        if (matched.isEmpty()) {
            return null;
        }
        return matched.get(0);
    }

    private static BsonDocument findAndModifyDocumentFilter(final BsonDocument document) {
        final BsonValue id = document.get("_id");
        if (id != null) {
            return new BsonDocument("_id", id);
        }
        return document.clone();
    }

    private static UpdateRequest findAndModifyUpdateRequest(
            final FindAndModifyRequest request, final BsonDocument query, final boolean upsert) {
        if (request.updatePipeline() != null) {
            return new UpdateRequest(query, request.updatePipeline(), false, upsert);
        }
        return new UpdateRequest(query, request.update(), false, upsert, request.arrayFilters());
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
            BsonDocument query,
            BsonDocument update,
            BsonArray updatePipeline,
            boolean multi,
            boolean upsert,
            List<BsonDocument> arrayFilters) {
        public UpdateRequest(final BsonDocument query, final BsonDocument update, final boolean multi, final boolean upsert) {
            this(query, update, null, multi, upsert, List.of());
        }

        public UpdateRequest(
                final BsonDocument query,
                final BsonDocument update,
                final boolean multi,
                final boolean upsert,
                final List<BsonDocument> arrayFilters) {
            this(query, update, null, multi, upsert, arrayFilters);
        }

        public UpdateRequest(
                final BsonDocument query,
                final BsonArray updatePipeline,
                final boolean multi,
                final boolean upsert) {
            this(query, null, updatePipeline, multi, upsert, List.of());
        }

        public UpdateRequest {
            if ((update == null) == (updatePipeline == null)) {
                throw new IllegalArgumentException("exactly one of update or updatePipeline must be specified");
            }
            update = update == null ? null : update.clone();
            updatePipeline = updatePipeline == null ? null : updatePipeline.clone();
            arrayFilters = copyArrayFilters(arrayFilters);
            if (updatePipeline != null && !arrayFilters.isEmpty()) {
                throw new IllegalArgumentException("arrayFilters is not allowed with pipeline updates");
            }
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

    record CollectionMetadata(String name) {}

    record DropCollectionResult(boolean dropped, int nIndexesWas) {}

    record Upserted(int index, BsonValue id) {}

    record UpdateResult(int matchedCount, int modifiedCount, List<Upserted> upserted) {
        public UpdateResult(final int matchedCount, final int modifiedCount) {
            this(matchedCount, modifiedCount, List.of());
        }

        public UpdateResult {
            upserted = upserted == null ? List.of() : List.copyOf(upserted);
        }
    }

    record FindAndModifyRequest(
            BsonDocument query,
            BsonDocument sort,
            CollationSupport.Config collation,
            boolean remove,
            BsonDocument update,
            BsonArray updatePipeline,
            boolean upsert,
            List<BsonDocument> arrayFilters) {
        public FindAndModifyRequest {
            query = query == null ? new BsonDocument() : query.clone();
            sort = sort == null ? new BsonDocument() : sort.clone();
            collation = collation == null ? CollationSupport.Config.simple() : collation;
            if (remove) {
                update = null;
                updatePipeline = null;
            } else {
                update = update == null ? null : update.clone();
                updatePipeline = updatePipeline == null ? null : updatePipeline.clone();
                if ((update == null) == (updatePipeline == null)) {
                    throw new IllegalArgumentException("exactly one of update or updatePipeline must be specified");
                }
            }
            arrayFilters = copyArrayFilters(arrayFilters);
            if (updatePipeline != null && !arrayFilters.isEmpty()) {
                throw new IllegalArgumentException("arrayFilters is not allowed with pipeline updates");
            }
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

    record FindAndModifyOutcome(
            BsonDocument before, BsonDocument after, int matched, boolean updatedExisting, BsonValue upsertedId) {
        static FindAndModifyOutcome miss() {
            return new FindAndModifyOutcome(null, null, 0, false, null);
        }

        static FindAndModifyOutcome removed(final BsonDocument before) {
            return new FindAndModifyOutcome(before, null, before == null ? 0 : 1, false, null);
        }
    }
}
