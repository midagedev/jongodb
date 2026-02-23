package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class AggregateCommandE2ETest {
    @Test
    void aggregateCommandCallsStoreAndReturnsCursorBatch() {
        final RecordingStore store = new RecordingStore();
        store.aggregateResult = List.of(
                BsonDocument.parse("{\"_id\":1,\"name\":\"a\"}"),
                BsonDocument.parse("{\"_id\":2,\"name\":\"b\"}"));
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"users\",\"$db\":\"app\",\"pipeline\":[{\"$match\":{\"name\":\"a\"}}],\"cursor\":{\"batchSize\":1}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonDocument cursor = response.getDocument("cursor");
        assertEquals(Set.of("id", "ns", "firstBatch"), cursor.keySet());
        assertTrue(cursor.getInt64("id").getValue() > 0);
        assertEquals("app.users", cursor.getString("ns").getValue());
        assertEquals(1, cursor.getArray("firstBatch").size());
        assertEquals(
                "a",
                cursor.getArray("firstBatch").get(0).asDocument().getString("name").getValue());

        assertEquals("app", store.lastAggregateDatabase);
        assertEquals("users", store.lastAggregateCollection);
        assertEquals(1, store.lastAggregatePipeline.size());
        assertNotNull(store.lastAggregatePipeline.get(0).getDocument("$match"));
    }

    @Test
    void aggregateGetMoreSupportsMultiBatchCursorLifecycle() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument insertResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"},{\"_id\":3,\"name\":\"c\"}]}"));
        assertEquals(1.0, insertResponse.get("ok").asNumber().doubleValue());

        final BsonDocument aggregateResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"users\",\"$db\":\"app\",\"pipeline\":[{\"$sort\":{\"_id\":1}}],\"cursor\":{\"batchSize\":2}}"));
        assertEquals(1.0, aggregateResponse.get("ok").asNumber().doubleValue());

        final BsonDocument aggregateCursor = aggregateResponse.getDocument("cursor");
        final long cursorId = aggregateCursor.getInt64("id").getValue();
        assertTrue(cursorId > 0);
        assertEquals(2, aggregateCursor.getArray("firstBatch").size());

        final BsonDocument getMoreResponse = dispatcher.dispatch(new BsonDocument()
                .append("getMore", new BsonInt64(cursorId))
                .append("collection", new BsonString("users"))
                .append("$db", new BsonString("app"))
                .append("batchSize", new BsonInt32(2)));
        assertEquals(1.0, getMoreResponse.get("ok").asNumber().doubleValue());

        final BsonDocument getMoreCursor = getMoreResponse.getDocument("cursor");
        assertEquals(0L, getMoreCursor.getInt64("id").getValue());
        assertEquals(1, getMoreCursor.getArray("nextBatch").size());
        assertEquals(
                "c",
                getMoreCursor
                        .getArray("nextBatch")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());
    }

    @Test
    void aggregateCommandRejectsInvalidPayloadShapes() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument invalidPipelineType =
                dispatcher.dispatch(BsonDocument.parse("{\"aggregate\":\"users\",\"pipeline\":{}}"));
        assertCommandError(invalidPipelineType, "TypeMismatch");

        final BsonDocument invalidPipelineEntry = dispatcher.dispatch(
                BsonDocument.parse("{\"aggregate\":\"users\",\"pipeline\":[1]}"));
        assertCommandError(invalidPipelineEntry, "TypeMismatch");

        final BsonDocument invalidCursorType =
                dispatcher.dispatch(BsonDocument.parse("{\"aggregate\":\"users\",\"pipeline\":[],\"cursor\":1}"));
        assertCommandError(invalidCursorType, "TypeMismatch");

        final BsonDocument unsupportedStage = dispatcher.dispatch(
                BsonDocument.parse("{\"aggregate\":\"users\",\"pipeline\":[{\"$foo\":{}}],\"cursor\":{}}"));
        assertCommandError(unsupportedStage, "BadValue");
    }

    private static void assertCommandError(final BsonDocument response, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(14, response.getInt32("code").getValue());
        assertEquals(codeName, response.getString("codeName").getValue());
    }

    private static final class RecordingStore implements CommandStore {
        private String lastAggregateDatabase;
        private String lastAggregateCollection;
        private List<BsonDocument> lastAggregatePipeline = List.of();
        private List<BsonDocument> aggregateResult = List.of();

        @Override
        public int insert(final String database, final String collection, final List<BsonDocument> documents) {
            return documents == null ? 0 : documents.size();
        }

        @Override
        public List<BsonDocument> find(final String database, final String collection, final BsonDocument filter) {
            return List.of();
        }

        @Override
        public List<BsonDocument> aggregate(
                final String database, final String collection, final List<BsonDocument> pipeline) {
            lastAggregateDatabase = database;
            lastAggregateCollection = collection;
            lastAggregatePipeline = List.copyOf(pipeline);
            return new ArrayList<>(aggregateResult);
        }

        @Override
        public CreateIndexesResult createIndexes(
                final String database, final String collection, final List<IndexRequest> indexes) {
            return new CreateIndexesResult(0, 0);
        }

        @Override
        public UpdateResult update(final String database, final String collection, final List<UpdateRequest> updates) {
            return new UpdateResult(0, 0);
        }

        @Override
        public int delete(final String database, final String collection, final List<DeleteRequest> deletes) {
            return 0;
        }
    }
}
