package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
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
        assertEquals("app.users", cursor.getString("ns").getValue());

        final BsonArray firstBatch = cursor.getArray("firstBatch");
        assertEquals(1, firstBatch.size());
        assertEquals("a", firstBatch.get(0).asDocument().getString("name").getValue());

        assertEquals("app", store.lastFindDatabase);
        assertEquals("users", store.lastFindCollection);
        assertNotNull(store.lastFindFilter);
        assertEquals("a", store.lastFindFilter.getString("name").getValue());
    }

    private static final class RecordingStore implements CommandStore {
        private String lastInsertDatabase;
        private String lastInsertCollection;
        private List<BsonDocument> lastInsertedDocuments = List.of();

        private String lastFindDatabase;
        private String lastFindCollection;
        private BsonDocument lastFindFilter;
        private List<BsonDocument> findResult = List.of();

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
    }
}
