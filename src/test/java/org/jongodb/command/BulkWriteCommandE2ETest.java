package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class BulkWriteCommandE2ETest {
    @Test
    void bulkWriteCommandSupportsOrderedSubsetAndReturnsDeterministicCounters() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "bulkWrite": "users",
                  "$db": "app",
                  "ordered": true,
                  "operations": [
                    {"insertOne": {"document": {"_id": 1, "name": "alpha"}}},
                    {"updateOne": {"filter": {"_id": 1}, "update": {"$set": {"tier": 1}}}},
                    {"updateMany": {"filter": {"tier": 1}, "update": {"$set": {"active": true}}}},
                    {"replaceOne": {"filter": {"_id": 2}, "replacement": {"name": "beta"}, "upsert": true}},
                    {"deleteOne": {"filter": {"_id": 1}}},
                    {"deleteMany": {"filter": {"name": "beta"}}}
                  ]
                }
                """));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(1, response.getInt32("nInserted").getValue());
        assertEquals(2, response.getInt32("nMatched").getValue());
        assertEquals(2, response.getInt32("nModified").getValue());
        assertEquals(2, response.getInt32("nDeleted").getValue());
        assertEquals(1, response.getInt32("nUpserted").getValue());
        assertEquals(1, response.getArray("upserted").size());
        assertEquals(
                3,
                response.getArray("upserted").get(0).asDocument().getInt32("index").getValue());
        assertEquals(
                2,
                response.getArray("upserted").get(0).asDocument().getInt32("_id").getValue());
        assertEquals(
                List.of("nInserted", "nMatched", "nModified", "nDeleted", "nUpserted", "upserted", "ok"),
                new ArrayList<>(response.keySet()));

        final BsonDocument finalState =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0, finalState.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void bulkWriteCommandStopsAtFirstWriteErrorWhenOrdered() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument createIndexResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true}]}"));
        assertEquals(1.0, createIndexResponse.get("ok").asNumber().doubleValue());

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "bulkWrite": "users",
                  "$db": "app",
                  "ordered": true,
                  "operations": [
                    {"insertOne": {"document": {"_id": 1, "email": "ada@example.com"}}},
                    {"insertOne": {"document": {"_id": 2, "email": "ada@example.com"}}},
                    {"insertOne": {"document": {"_id": 3, "email": "linus@example.com"}}}
                  ]
                }
                """));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(1, response.getInt32("nInserted").getValue());
        assertEquals(0, response.getInt32("nMatched").getValue());
        assertEquals(0, response.getInt32("nModified").getValue());
        assertEquals(0, response.getInt32("nDeleted").getValue());
        assertEquals(0, response.getInt32("nUpserted").getValue());
        assertEquals(1, response.getArray("writeErrors").size());
        assertEquals(
                1,
                response.getArray("writeErrors")
                        .get(0)
                        .asDocument()
                        .getInt32("index")
                        .getValue());
        assertEquals(
                11000,
                response.getArray("writeErrors")
                        .get(0)
                        .asDocument()
                        .getInt32("code")
                        .getValue());
        assertEquals(
                "DuplicateKey",
                response.getArray("writeErrors")
                        .get(0)
                        .asDocument()
                        .getString("codeName")
                        .getValue());
        assertNotNull(response.getArray("writeErrors").get(0).asDocument().getString("errmsg"));
        assertEquals(
                List.of("nInserted", "nMatched", "nModified", "nDeleted", "nUpserted", "writeErrors", "ok"),
                new ArrayList<>(response.keySet()));

        final BsonDocument finalState =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, finalState.getDocument("cursor").getArray("firstBatch").size());
        assertEquals(
                1,
                finalState.getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getInt32("_id")
                        .getValue());
    }

    @Test
    void bulkWriteCommandRejectsOrderedFalse() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"bulkWrite\":\"users\",\"$db\":\"app\",\"ordered\":false,\"operations\":[{\"insertOne\":{\"document\":{\"_id\":1}}}]}"));

        assertCommandError(response, "BadValue");
    }

    @Test
    void bulkWriteCommandRejectsUnsupportedOperationName() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"bulkWrite\":\"users\",\"$db\":\"app\",\"operations\":[{\"renameCollection\":{\"from\":\"users\",\"to\":\"users2\"}}]}"));

        assertCommandError(response, "BadValue");
    }

    @Test
    void bulkWriteCommandReportsOperationValidationErrorsInWriteErrorsAndStops() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "bulkWrite": "users",
                  "$db": "app",
                  "operations": [
                    {"insertOne": {"document": {"_id": 1}}},
                    {"updateMany": {"filter": {}, "update": {"$set": {"a": 1}}, "arrayFilters": []}},
                    {"deleteMany": {"filter": {}}}
                  ]
                }
                """));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(1, response.getInt32("nInserted").getValue());
        assertEquals(0, response.getInt32("nMatched").getValue());
        assertEquals(0, response.getInt32("nModified").getValue());
        assertEquals(0, response.getInt32("nDeleted").getValue());
        assertEquals(0, response.getInt32("nUpserted").getValue());
        assertEquals(1, response.getArray("writeErrors").size());
        assertEquals(
                1,
                response.getArray("writeErrors")
                        .get(0)
                        .asDocument()
                        .getInt32("index")
                        .getValue());
        assertEquals(
                14,
                response.getArray("writeErrors")
                        .get(0)
                        .asDocument()
                        .getInt32("code")
                        .getValue());
        assertEquals(
                "BadValue",
                response.getArray("writeErrors")
                        .get(0)
                        .asDocument()
                        .getString("codeName")
                        .getValue());
        assertNotNull(response.getArray("writeErrors").get(0).asDocument().getString("errmsg"));

        final BsonDocument finalState =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, finalState.getDocument("cursor").getArray("firstBatch").size());
        assertEquals(
                1,
                finalState.getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getInt32("_id")
                        .getValue());
    }

    @Test
    void bulkWriteCommandUsesTransactionalSnapshotAndCommit() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument transactionalBulkWrite = dispatcher.dispatch(BsonDocument.parse(
                """
                {
                  "bulkWrite": "users",
                  "$db": "app",
                  "operations": [{"insertOne": {"document": {"_id": 1, "name": "txn"}}}],
                  "lsid": {"id": "session-bulk"},
                  "txnNumber": 1,
                  "autocommit": false,
                  "startTransaction": true
                }
                """));
        assertEquals(1.0, transactionalBulkWrite.get("ok").asNumber().doubleValue());

        final BsonDocument outsideBeforeCommit =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0, outsideBeforeCommit.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument insideTransaction = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-bulk\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1, insideTransaction.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-bulk\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideAfterCommit =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, outsideAfterCommit.getDocument("cursor").getArray("firstBatch").size());
        assertEquals(
                "txn",
                outsideAfterCommit
                        .getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());
    }

    private static void assertCommandError(final BsonDocument response, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(14, response.getInt32("code").getValue());
        assertEquals(codeName, response.getString("codeName").getValue());
        assertTrue(response.containsKey("errmsg"));
    }
}
