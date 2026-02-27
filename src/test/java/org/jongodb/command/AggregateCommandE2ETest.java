package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
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
    void aggregateCommandAppliesCollationForMatchAndSortSubset() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "insert": "users",
                  "$db": "app",
                  "documents": [
                    {"_id": 1, "name": "z"},
                    {"_id": 2, "name": "ä"},
                    {"_id": 3, "name": "ALPHA"}
                  ]
                }
                """));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "aggregate": "users",
                  "$db": "app",
                  "pipeline": [
                    {"$match": {"name": {"$gte": "a"}}},
                    {"$sort": {"name": 1}}
                  ],
                  "collation": {"locale": "en", "strength": 1},
                  "cursor": {}
                }
                """));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonArray firstBatch = response.getDocument("cursor").getArray("firstBatch");
        assertEquals(3, firstBatch.size());
        assertEquals("ä", firstBatch.get(0).asDocument().getString("name").getValue());
        assertEquals("ALPHA", firstBatch.get(1).asDocument().getString("name").getValue());
        assertEquals("z", firstBatch.get(2).asDocument().getString("name").getValue());
    }

    @Test
    void aggregateCommandSupportsSetUnsetAndReplaceWithStageSubsets() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "insert": "users",
                  "$db": "app",
                  "documents": [
                    {"_id": 1, "name": "alice", "legacy": true, "profile": {"city": "Seoul", "zip": "00000"}},
                    {"_id": 2, "name": "bob", "legacy": true, "profile": {"city": "Busan", "zip": "11111"}}
                  ]
                }
                """));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "aggregate": "users",
                  "$db": "app",
                  "pipeline": [
                    {"$set": {"profile.country": "KR"}},
                    {"$unset": {"legacy": true}},
                    {"$replaceWith": "$profile"},
                    {"$sort": {"city": 1}}
                  ],
                  "cursor": {}
                }
                """));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonArray firstBatch = response.getDocument("cursor").getArray("firstBatch");
        assertEquals(2, firstBatch.size());
        assertEquals("Busan", firstBatch.get(0).asDocument().getString("city").getValue());
        assertEquals("KR", firstBatch.get(0).asDocument().getString("country").getValue());
        assertEquals("Seoul", firstBatch.get(1).asDocument().getString("city").getValue());
    }

    @Test
    void aggregateCommandSupportsTerminalOutStageSubset() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "insert": "users",
                  "$db": "app",
                  "documents": [
                    {"_id": 1, "name": "alice", "active": true},
                    {"_id": 2, "name": "bob", "active": false},
                    {"_id": 3, "name": "carol", "active": true}
                  ]
                }
                """));
        dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "insert": "archive",
                  "$db": "app",
                  "documents": [
                    {"_id": 100, "name": "legacy"}
                  ]
                }
                """));

        final BsonDocument aggregateResponse = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "aggregate": "users",
                  "$db": "app",
                  "pipeline": [
                    {"$match": {"active": true}},
                    {"$project": {"_id": 1, "name": 1}},
                    {"$out": "archive"}
                  ],
                  "cursor": {}
                }
                """));
        assertEquals(1.0, aggregateResponse.get("ok").asNumber().doubleValue());
        assertEquals(0, aggregateResponse.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument archiveFindResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"archive\",\"$db\":\"app\",\"filter\":{},\"sort\":{\"_id\":1}}"));
        assertEquals(1.0, archiveFindResponse.get("ok").asNumber().doubleValue());
        final BsonArray archiveDocuments = archiveFindResponse.getDocument("cursor").getArray("firstBatch");
        assertEquals(2, archiveDocuments.size());
        assertEquals(1, archiveDocuments.get(0).asDocument().getInt32("_id").getValue());
        assertEquals("alice", archiveDocuments.get(0).asDocument().getString("name").getValue());
        assertEquals(3, archiveDocuments.get(1).asDocument().getInt32("_id").getValue());
        assertEquals("carol", archiveDocuments.get(1).asDocument().getString("name").getValue());
    }

    @Test
    void aggregateCommandSupportsTerminalMergeStageSubset() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "insert": "users",
                  "$db": "app",
                  "documents": [
                    {"_id": 1, "name": "alice", "active": true},
                    {"_id": 2, "name": "bob", "active": false},
                    {"_id": 3, "name": "carol", "active": true}
                  ]
                }
                """));
        dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "insert": "archive",
                  "$db": "app",
                  "documents": [
                    {"_id": 1, "name": "legacy", "legacy": true},
                    {"_id": 100, "name": "keep", "legacy": true}
                  ]
                }
                """));

        final BsonDocument aggregateResponse = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "aggregate": "users",
                  "$db": "app",
                  "pipeline": [
                    {"$match": {"active": true}},
                    {"$project": {"_id": 1, "name": 1, "active": 1}},
                    {"$merge": "archive"}
                  ],
                  "cursor": {}
                }
                """));
        assertEquals(1.0, aggregateResponse.get("ok").asNumber().doubleValue());
        assertEquals(0, aggregateResponse.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument archiveFindResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"archive\",\"$db\":\"app\",\"filter\":{},\"sort\":{\"_id\":1}}"));
        assertEquals(1.0, archiveFindResponse.get("ok").asNumber().doubleValue());
        final BsonArray archiveDocuments = archiveFindResponse.getDocument("cursor").getArray("firstBatch");
        assertEquals(3, archiveDocuments.size());

        final BsonDocument first = archiveDocuments.get(0).asDocument();
        assertEquals(1, first.getInt32("_id").getValue());
        assertEquals("alice", first.getString("name").getValue());
        assertTrue(first.getBoolean("legacy").getValue());
        assertTrue(first.getBoolean("active").getValue());

        final BsonDocument second = archiveDocuments.get(1).asDocument();
        assertEquals(3, second.getInt32("_id").getValue());
        assertEquals("carol", second.getString("name").getValue());
        assertTrue(second.getBoolean("active").getValue());

        final BsonDocument third = archiveDocuments.get(2).asDocument();
        assertEquals(100, third.getInt32("_id").getValue());
        assertEquals("keep", third.getString("name").getValue());
        assertTrue(third.getBoolean("legacy").getValue());
    }

    @Test
    void aggregateCommandRejectsNonTerminalOutStage() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alpha\"}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "aggregate": "users",
                  "$db": "app",
                  "pipeline": [
                    {"$out": "archive"},
                    {"$match": {"_id": 1}}
                  ],
                  "cursor": {}
                }
                """));
        assertCommandError(response, 238, "NotImplemented");
    }

    @Test
    void aggregateCommandRejectsNonTerminalMergeStage() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alpha\"}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "aggregate": "users",
                  "$db": "app",
                  "pipeline": [
                    {"$merge": "archive"},
                    {"$match": {"_id": 1}}
                  ],
                  "cursor": {}
                }
                """));
        assertCommandError(response, 238, "NotImplemented");
    }

    @Test
    void aggregateCommandRejectsInvalidPayloadShapes() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alpha\"}]}"));

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
        assertCommandError(unsupportedStage, 238, "NotImplemented");

        final BsonDocument invalidUnsetStage = dispatcher.dispatch(
                BsonDocument.parse("{\"aggregate\":\"users\",\"$db\":\"app\",\"pipeline\":[{\"$unset\":1}],\"cursor\":{}}"));
        assertCommandError(invalidUnsetStage, "BadValue");

        final BsonDocument invalidReplaceWithStage = dispatcher.dispatch(
                BsonDocument.parse(
                        "{\"aggregate\":\"users\",\"$db\":\"app\",\"pipeline\":[{\"$replaceWith\":1}],\"cursor\":{}}"));
        assertCommandError(invalidReplaceWithStage, "BadValue");
    }

    @Test
    void aggregateCommandSupportsLookupWithExprPipeline() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alice\"},{\"_id\":2,\"name\":\"bob\"}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"orders\",\"$db\":\"app\",\"documents\":[{\"_id\":10,\"userId\":1},{\"_id\":11,\"userId\":2}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"orders\",\"$db\":\"app\",\"pipeline\":[{\"$lookup\":{\"from\":\"users\",\"localField\":\"userId\",\"foreignField\":\"_id\",\"as\":\"joinedByField\"}},{\"$lookup\":{\"from\":\"users\",\"let\":{\"uid\":\"$userId\"},\"pipeline\":[{\"$match\":{\"$expr\":{\"$eq\":[\"$_id\",\"$$uid\"]}}},{\"$project\":{\"_id\":0,\"name\":1}}],\"as\":\"joinedByExpr\"}},{\"$sort\":{\"_id\":1}}],\"cursor\":{}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonArray firstBatch = response.getDocument("cursor").getArray("firstBatch");
        assertEquals(2, firstBatch.size());

        final BsonDocument firstOrder = firstBatch.get(0).asDocument();
        assertEquals(1, firstOrder.getArray("joinedByField").size());
        assertEquals(
                "alice",
                firstOrder
                        .getArray("joinedByField")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());
        assertEquals(1, firstOrder.getArray("joinedByExpr").size());
        assertEquals(
                "alice",
                firstOrder
                        .getArray("joinedByExpr")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());
    }

    @Test
    void aggregateCommandSupportsUnionWithPipeline() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alice\"},{\"_id\":2,\"name\":\"bob\"}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"orders\",\"$db\":\"app\",\"documents\":[{\"_id\":10,\"orderNo\":\"A-10\"},{\"_id\":11,\"orderNo\":\"B-11\"}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"orders\",\"$db\":\"app\",\"pipeline\":[{\"$project\":{\"_id\":0,\"type\":\"order\",\"ref\":\"$orderNo\"}},{\"$unionWith\":{\"coll\":\"users\",\"pipeline\":[{\"$project\":{\"_id\":0,\"type\":\"user\",\"ref\":\"$name\"}}]}},{\"$sort\":{\"type\":1,\"ref\":1}}],\"cursor\":{}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonArray firstBatch = response.getDocument("cursor").getArray("firstBatch");
        assertEquals(4, firstBatch.size());
        assertEquals("order", firstBatch.get(0).asDocument().getString("type").getValue());
        assertEquals("A-10", firstBatch.get(0).asDocument().getString("ref").getValue());
        assertEquals("user", firstBatch.get(2).asDocument().getString("type").getValue());
        assertEquals("alice", firstBatch.get(2).asDocument().getString("ref").getValue());
    }

    @Test
    void aggregateCommandSupportsGroupAddToSetAccumulator() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"items\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"category\":\"a\",\"color\":\"red\"},{\"_id\":2,\"category\":\"a\",\"color\":\"blue\"},{\"_id\":3,\"category\":\"a\",\"color\":\"red\"},{\"_id\":4,\"category\":\"b\",\"color\":\"green\"}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"items\",\"$db\":\"app\",\"pipeline\":[{\"$group\":{\"_id\":\"$category\",\"colors\":{\"$addToSet\":\"$color\"}}},{\"$sort\":{\"_id\":1}}],\"cursor\":{}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonArray firstBatch = response.getDocument("cursor").getArray("firstBatch");
        assertEquals(2, firstBatch.size());

        final BsonDocument firstGroup = firstBatch.get(0).asDocument();
        assertEquals("a", firstGroup.getString("_id").getValue());
        assertEquals(2, firstGroup.getArray("colors").size());
        assertEquals("red", firstGroup.getArray("colors").get(0).asString().getValue());
        assertEquals("blue", firstGroup.getArray("colors").get(1).asString().getValue());

        final BsonDocument secondGroup = firstBatch.get(1).asDocument();
        assertEquals("b", secondGroup.getString("_id").getValue());
        assertEquals(1, secondGroup.getArray("colors").size());
        assertEquals("green", secondGroup.getArray("colors").get(0).asString().getValue());
    }

    private static void assertCommandError(final BsonDocument response, final String codeName) {
        assertCommandError(response, 14, codeName);
    }

    private static void assertCommandError(final BsonDocument response, final int code, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(code, response.getInt32("code").getValue());
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
