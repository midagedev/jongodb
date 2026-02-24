package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.bson.BsonDocument;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class FindAndModifyCommandE2ETest {
    @Test
    void removeModeRejectsUpdateNewAndUpsertOptions() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument updateWithRemove = dispatcher.dispatch(BsonDocument.parse(
                "{\"findAndModify\":\"users\",\"$db\":\"app\",\"query\":{},\"remove\":true,\"update\":{\"$set\":{\"name\":\"neo\"}}}"));
        assertCommandError(updateWithRemove, "BadValue");

        final BsonDocument updateArrayWithRemove = dispatcher.dispatch(BsonDocument.parse(
                "{\"findAndModify\":\"users\",\"$db\":\"app\",\"query\":{},\"remove\":true,\"update\":[{\"$set\":{\"name\":\"neo\"}}]}"));
        assertCommandError(updateArrayWithRemove, "BadValue");

        final BsonDocument updateScalarWithRemove = dispatcher.dispatch(BsonDocument.parse(
                "{\"findAndModify\":\"users\",\"$db\":\"app\",\"query\":{},\"remove\":true,\"update\":1}"));
        assertCommandError(updateScalarWithRemove, "BadValue");

        final BsonDocument newWithRemove = dispatcher.dispatch(BsonDocument.parse(
                "{\"findAndModify\":\"users\",\"$db\":\"app\",\"query\":{},\"remove\":true,\"new\":true}"));
        assertCommandError(newWithRemove, "BadValue");

        final BsonDocument upsertWithRemove = dispatcher.dispatch(BsonDocument.parse(
                "{\"findAndModify\":\"users\",\"$db\":\"app\",\"query\":{},\"remove\":true,\"upsert\":true}"));
        assertCommandError(upsertWithRemove, "BadValue");
    }

    @Test
    void removeModeDeletesSingleMatchedDocument() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alpha\"},{\"_id\":2,\"name\":\"alpha\"}]}"));

        final BsonDocument removeResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"findAndModify\":\"users\",\"$db\":\"app\",\"query\":{\"name\":\"alpha\"},\"remove\":true}"));

        assertEquals(1.0, removeResponse.get("ok").asNumber().doubleValue());
        assertEquals(1, removeResponse.getDocument("lastErrorObject").getInt32("n").getValue());
        assertEquals("alpha", removeResponse.get("value").asDocument().getString("name").getValue());

        final BsonDocument countAfter = dispatcher.dispatch(BsonDocument.parse(
                "{\"count\":\"users\",\"$db\":\"app\",\"query\":{\"name\":\"alpha\"}}"));
        assertEquals(1L, countAfter.getInt64("n").getValue());
    }

    private static void assertCommandError(final BsonDocument response, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(codeName, response.getString("codeName").getValue());
    }
}
