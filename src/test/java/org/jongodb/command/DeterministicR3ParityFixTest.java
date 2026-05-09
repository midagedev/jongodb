package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.BsonDocument;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

final class DeterministicR3ParityFixTest {
    @Test
    void insertRejectsIdDocumentWithDollarPrefixedField() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":{\"$a\":1},\"name\":\"bad\"}]}"));

        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(52, response.getInt32("code").getValue());
        assertEquals("DollarPrefixedFieldName", response.getString("codeName").getValue());
        assertTrue(response.getString("errmsg")
                .getValue()
                .contains("_id fields may not contain '$'-prefixed fields: $a is not valid for storage"));
    }

    @Test
    void updateRejectsMultiReplacementStyleUpdateAsFailedToParse() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{},\"u\":{\"name\":\"replacement\"},\"multi\":true}]}"));

        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(9, response.getInt32("code").getValue());
        assertEquals("FailedToParse", response.getString("codeName").getValue());
        assertEquals(
                "multi update is not supported for replacement-style update",
                response.getString("errmsg").getValue());
    }

    @Test
    void createIndexesCanStartAndCommitTransaction() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1}}],\"lsid\":{\"id\":\"session-createindexes\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-createindexes\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument listIndexesResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"listIndexes\":\"users\",\"$db\":\"app\"}"));
        assertEquals(1.0, listIndexesResponse.get("ok").asNumber().doubleValue());
        assertEquals(2, listIndexesResponse.getDocument("cursor").getArray("firstBatch").size());
    }
}
