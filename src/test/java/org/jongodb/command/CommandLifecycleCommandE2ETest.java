package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

final class CommandLifecycleCommandE2ETest {
    @Test
    void listCollectionsReturnsCursorBatchWithCollectionMetadata() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"orders\",\"$db\":\"app\",\"documents\":[{\"_id\":1}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"listCollections\":1,\"$db\":\"app\",\"filter\":{\"name\":\"users\"},\"nameOnly\":true}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonDocument cursor = response.getDocument("cursor");
        assertEquals("app.$cmd.listCollections", cursor.getString("ns").getValue());
        final BsonArray firstBatch = cursor.getArray("firstBatch");
        assertEquals(1, firstBatch.size());
        assertEquals("users", firstBatch.get(0).asDocument().getString("name").getValue());
        assertEquals("collection", firstBatch.get(0).asDocument().getString("type").getValue());
    }

    @Test
    void dropRemovesCollectionAndReportsIndexCount() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"email\":\"a@example.com\"}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1}}]}"));

        final BsonDocument dropResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"drop\":\"users\",\"$db\":\"app\"}"));

        assertEquals(1.0, dropResponse.get("ok").asNumber().doubleValue());
        assertEquals("app.users", dropResponse.getString("ns").getValue());
        assertEquals(2, dropResponse.getInt32("nIndexesWas").getValue());

        final BsonDocument listResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"listCollections\":1,\"$db\":\"app\"}"));
        assertEquals(0, listResponse.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void dropDatabaseRemovesAllCollectionsInDatabaseOnly() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"orders\",\"$db\":\"app\",\"documents\":[{\"_id\":1}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"other\",\"documents\":[{\"_id\":1}]}"));

        final BsonDocument dropResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"dropDatabase\":1,\"$db\":\"app\"}"));

        assertEquals(1.0, dropResponse.get("ok").asNumber().doubleValue());
        assertEquals("app", dropResponse.getString("dropped").getValue());

        final BsonDocument appCollections =
                dispatcher.dispatch(BsonDocument.parse("{\"listCollections\":1,\"$db\":\"app\"}"));
        assertEquals(0, appCollections.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument otherCollections =
                dispatcher.dispatch(BsonDocument.parse("{\"listCollections\":1,\"$db\":\"other\"}"));
        assertEquals(1, otherCollections.getDocument("cursor").getArray("firstBatch").size());
    }
}
