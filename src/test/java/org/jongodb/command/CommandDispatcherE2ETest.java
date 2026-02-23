package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
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

    private static void assertCommandError(final BsonDocument response, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(14, response.getInt32("code").getValue());
        assertEquals(codeName, response.getString("codeName").getValue());
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
