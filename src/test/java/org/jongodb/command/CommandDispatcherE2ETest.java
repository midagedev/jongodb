package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class CommandDispatcherE2ETest {
    @Test
    void helloCommandReturnsHandshakeShape() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse("{\"hello\": 1, \"$db\": \"admin\"}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(true, response.getBoolean("isWritablePrimary").getValue());
        assertEquals(17, response.getInt32("maxWireVersion").getValue());
        assertEquals(30, response.getInt32("logicalSessionTimeoutMinutes").getValue());
    }

    @Test
    void pingCommandReturnsOk() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
    }

    @Test
    void insertCommandCallsStoreAndReturnsCount() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(2, response.getInt32("n").getValue());
        assertEquals("app", store.lastInsertDatabase);
        assertEquals("users", store.lastInsertCollection);
        assertEquals(2, store.lastInsertedDocuments.size());
        assertEquals("a", store.lastInsertedDocuments.get(0).getString("name").getValue());
    }

    @Test
    void findCommandCallsStoreAndReturnsCursorBatch() {
        final RecordingStore store = new RecordingStore();
        store.findResult = List.of(BsonDocument.parse("{\"_id\":1,\"name\":\"a\"}"));
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"name\":\"a\"}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonDocument cursor = response.getDocument("cursor");
        assertEquals(Set.of("id", "ns", "firstBatch"), cursor.keySet());
        assertEquals(0L, cursor.getInt64("id").getValue());
        assertEquals("app.users", cursor.getString("ns").getValue());

        final BsonArray firstBatch = cursor.getArray("firstBatch");
        assertEquals(1, firstBatch.size());
        assertEquals("a", firstBatch.get(0).asDocument().getString("name").getValue());

        assertEquals("app", store.lastFindDatabase);
        assertEquals("users", store.lastFindCollection);
        assertNotNull(store.lastFindFilter);
        assertEquals("a", store.lastFindFilter.getString("name").getValue());
    }

    @Test
    void createIndexesCommandCallsStoreAndReturnsShape() {
        final RecordingStore store = new RecordingStore();
        store.createIndexesResult = new CommandStore.CreateIndexesResult(0, 1);
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(false, response.getBoolean("createdCollectionAutomatically").getValue());
        assertEquals(0, response.getInt32("numIndexesBefore").getValue());
        assertEquals(1, response.getInt32("numIndexesAfter").getValue());
        assertEquals("app", store.lastCreateIndexesDatabase);
        assertEquals("users", store.lastCreateIndexesCollection);
        assertEquals(1, store.lastCreateIndexesRequests.size());
        assertEquals("email_1", store.lastCreateIndexesRequests.get(0).name());
        assertEquals(
                1,
                store.lastCreateIndexesRequests
                        .get(0)
                        .key()
                        .getInt32("email")
                        .getValue());
        assertTrue(store.lastCreateIndexesRequests.get(0).unique());
    }

    @Test
    void insertCommandRejectsDuplicateKeyAfterUniqueIndex() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument createIndexResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true}]}"));
        assertEquals(1.0, createIndexResponse.get("ok").asNumber().doubleValue());

        final BsonDocument firstInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"email\":\"ada@example.com\"}]}"));
        assertEquals(1.0, firstInsert.get("ok").asNumber().doubleValue());

        final BsonDocument duplicateInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":2,\"email\":\"ada@example.com\"}]}"));
        assertDuplicateKeyError(duplicateInsert);

        final BsonDocument findResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, findResponse.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void updateCommandRejectsDuplicateKeyAfterUniqueIndex() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"email\":\"ada@example.com\"},{\"_id\":2,\"email\":\"linus@example.com\"}]}"));

        final BsonDocument duplicateUpdate = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":2},\"u\":{\"$set\":{\"email\":\"ada@example.com\"}}}]}"));
        assertDuplicateKeyError(duplicateUpdate);

        final BsonDocument secondDocResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":2}}"));
        assertEquals(1, secondDocResponse.getDocument("cursor").getArray("firstBatch").size());
        assertEquals(
                "linus@example.com",
                secondDocResponse
                        .getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getString("email")
                        .getValue());
    }

    @Test
    void updateCommandCallsStoreAndReturnsWriteShape() {
        final RecordingStore store = new RecordingStore();
        store.updateResult = new CommandStore.UpdateResult(2, 1);
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"role\":\"user\"},\"u\":{\"$set\":{\"role\":\"member\"}},\"multi\":true}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(2, response.getInt32("n").getValue());
        assertEquals(1, response.getInt32("nModified").getValue());
        assertEquals("app", store.lastUpdateDatabase);
        assertEquals("users", store.lastUpdateCollection);
        assertEquals(1, store.lastUpdateRequests.size());
        assertEquals(
                "user",
                store.lastUpdateRequests.get(0).query().getString("role").getValue());
        assertTrue(store.lastUpdateRequests.get(0).multi());
    }

    @Test
    void deleteCommandCallsStoreAndReturnsWriteShape() {
        final RecordingStore store = new RecordingStore();
        store.deleteResult = 3;
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(
                BsonDocument.parse("{\"delete\":\"users\",\"$db\":\"app\",\"deletes\":[{\"q\":{\"role\":\"inactive\"},\"limit\":0}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(3, response.getInt32("n").getValue());
        assertEquals("app", store.lastDeleteDatabase);
        assertEquals("users", store.lastDeleteCollection);
        assertEquals(1, store.lastDeleteRequests.size());
        assertEquals(
                "inactive",
                store.lastDeleteRequests.get(0).query().getString("role").getValue());
        assertEquals(0, store.lastDeleteRequests.get(0).limit());
    }

    @Test
    void updateCommandRejectsInvalidPayloadShapes() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument typeMismatchResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"update\":\"users\",\"updates\":[{\"q\":1,\"u\":{\"$set\":{\"name\":\"a\"}}}]}"));
        assertCommandError(typeMismatchResponse, "TypeMismatch");

        final BsonDocument badValueResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"update\":\"users\",\"updates\":[{\"q\":{},\"u\":{}}]}"));
        assertCommandError(badValueResponse, "BadValue");
    }

    @Test
    void deleteCommandRejectsInvalidPayloadShapes() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument typeMismatchResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"delete\":\"users\",\"deletes\":[{\"q\":{},\"limit\":\"1\"}]}"));
        assertCommandError(typeMismatchResponse, "TypeMismatch");

        final BsonDocument badValueResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"delete\":\"users\",\"deletes\":[{\"q\":{},\"limit\":2}]}"));
        assertCommandError(badValueResponse, "BadValue");
    }

    @Test
    void transactionLifecycleAllowsCommitAndClearsState() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument inTxnResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"delete\":\"users\",\"$db\":\"app\",\"deletes\":[{\"q\":{\"_id\":1},\"limit\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, inTxnResponse.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument secondCommitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(secondCommitResponse, false);
    }

    @Test
    void transactionalWritesAreNotVisibleOutsideBeforeCommit() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"txn-only\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0, outsideFind.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument inTransactionFind = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1, inTransactionFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void commitTransactionPublishesTransactionWrites() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"committed\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, outsideFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void abortTransactionDiscardsTransactionWrites() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"abort-me\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument abortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"abortTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, abortResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0, outsideFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void transactionStateTransitionsRequireValidLifecycle() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument beforeStartResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(beforeStartResponse, true);

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument concurrentStartResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":2}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":2,\"autocommit\":false,\"startTransaction\":true}"));
        assertCommandError(concurrentStartResponse, "BadValue");

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument afterCommitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(afterCommitResponse, true);
    }

    @Test
    void abortTransactionWithoutActiveTransactionReturnsNoSuchTransaction() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument abortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"abortTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));

        assertNoSuchTransactionError(abortResponse, false);
    }

    @Test
    void abortTransactionClearsActiveTransaction() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument abortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"abortTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, abortResponse.get("ok").asNumber().doubleValue());

        final BsonDocument postAbortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(postAbortResponse, true);
    }

    @Test
    void transactionalCommandRejectsMissingLsid() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));

        assertCommandError(response, "TypeMismatch");
    }

    @Test
    void transactionStartRejectsInvalidFieldCombination() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument invalidStartTransaction = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":false}"));
        assertCommandError(invalidStartTransaction, "BadValue");

        final BsonDocument invalidAutocommit = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":true,\"startTransaction\":true}"));
        assertCommandError(invalidAutocommit, "BadValue");
    }

    @Test
    void transactionalCommandRejectsTxnNumberMismatch() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument mismatchResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"updated\"}}}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":2,\"autocommit\":false}"));
        assertNoSuchTransactionError(mismatchResponse, true);
    }

    private static void assertCommandError(final BsonDocument response, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(14, response.getInt32("code").getValue());
        assertEquals(codeName, response.getString("codeName").getValue());
        assertNotNull(response.getString("errmsg"));
    }

    private static void assertNoSuchTransactionError(final BsonDocument response, final boolean expectTransientLabel) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(251, response.getInt32("code").getValue());
        assertEquals("NoSuchTransaction", response.getString("codeName").getValue());
        assertNotNull(response.getString("errmsg"));
        if (expectTransientLabel) {
            assertEquals(1, response.getArray("errorLabels").size());
            assertEquals(
                    "TransientTransactionError",
                    response.getArray("errorLabels").get(0).asString().getValue());
        } else {
            assertTrue(!response.containsKey("errorLabels") || response.getArray("errorLabels").isEmpty());
        }
    }

    private static void assertDuplicateKeyError(final BsonDocument response) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(11000, response.getInt32("code").getValue());
        assertEquals("DuplicateKey", response.getString("codeName").getValue());
        assertNotNull(response.getString("errmsg"));
    }

    private static final class RecordingStore implements CommandStore {
        private String lastInsertDatabase;
        private String lastInsertCollection;
        private List<BsonDocument> lastInsertedDocuments = List.of();

        private String lastFindDatabase;
        private String lastFindCollection;
        private BsonDocument lastFindFilter;
        private List<BsonDocument> findResult = List.of();

        private String lastUpdateDatabase;
        private String lastUpdateCollection;
        private List<UpdateRequest> lastUpdateRequests = List.of();
        private UpdateResult updateResult = new UpdateResult(0, 0);

        private String lastCreateIndexesDatabase;
        private String lastCreateIndexesCollection;
        private List<IndexRequest> lastCreateIndexesRequests = List.of();
        private CreateIndexesResult createIndexesResult = new CreateIndexesResult(0, 0);

        private String lastDeleteDatabase;
        private String lastDeleteCollection;
        private List<DeleteRequest> lastDeleteRequests = List.of();
        private int deleteResult = 0;

        @Override
        public int insert(final String database, final String collection, final List<BsonDocument> documents) {
            lastInsertDatabase = database;
            lastInsertCollection = collection;
            lastInsertedDocuments = List.copyOf(documents);
            return documents.size();
        }

        @Override
        public List<BsonDocument> find(final String database, final String collection, final BsonDocument filter) {
            lastFindDatabase = database;
            lastFindCollection = collection;
            lastFindFilter = filter;
            return new ArrayList<>(findResult);
        }

        @Override
        public CreateIndexesResult createIndexes(
                final String database, final String collection, final List<IndexRequest> indexes) {
            lastCreateIndexesDatabase = database;
            lastCreateIndexesCollection = collection;
            lastCreateIndexesRequests = List.copyOf(indexes);
            return createIndexesResult;
        }

        @Override
        public UpdateResult update(final String database, final String collection, final List<UpdateRequest> updates) {
            lastUpdateDatabase = database;
            lastUpdateCollection = collection;
            lastUpdateRequests = List.copyOf(updates);
            return updateResult;
        }

        @Override
        public int delete(final String database, final String collection, final List<DeleteRequest> deletes) {
            lastDeleteDatabase = database;
            lastDeleteCollection = collection;
            lastDeleteRequests = List.copyOf(deletes);
            return deleteResult;
        }
    }
}
