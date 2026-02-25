package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class ScenarioBsonCodecTest {
    @Test
    void toCommandDocumentKeepsWireCompatibleDatabaseAndLsidString() {
        ScenarioCommand command = new ScenarioCommand(
            "find",
            Map.of(
                "collection",
                "users",
                "filter",
                Map.of("_id", 1),
                "lsid",
                Map.of("id", "session-wire")
            )
        );

        var commandDocument = ScenarioBsonCodec.toCommandDocument(command, "testkit_wire");

        assertEquals("testkit_wire", commandDocument.getString("$db").getValue());
        assertTrue(commandDocument.getDocument("lsid").get("id").isString());
        assertEquals("session-wire", commandDocument.getDocument("lsid").getString("id").getValue());
    }

    @Test
    void toRealMongodCommandDocumentSanitizesTransportSpecificFields() {
        ScenarioCommand command = new ScenarioCommand(
            "find",
            Map.of(
                "collection",
                "users",
                "$db",
                "explicit-db",
                "filter",
                Map.of("$db", "nested-db"),
                "lsid",
                Map.of("id", "session-real"),
                "txnNumber",
                7
            )
        );

        var commandDocument = ScenarioBsonCodec.toRealMongodCommandDocument(command, "testkit_real");

        assertFalse(commandDocument.containsKey("$db"));
        assertFalse(commandDocument.containsKey("lsid"));
        assertEquals("nested-db", commandDocument.getDocument("filter").getString("$db").getValue());
        assertTrue(commandDocument.get("txnNumber").isInt64());
        assertEquals(7L, commandDocument.getInt64("txnNumber").longValue());
    }

    @Test
    void isSuccessReturnsFalseWhenWriteErrorsArePresent() {
        BsonDocument response = BsonDocument.parse(
            "{\"ok\":1,\"writeErrors\":[{\"code\":11000,\"errmsg\":\"duplicate\"}]}"
        );

        assertFalse(ScenarioBsonCodec.isSuccess(response));
    }

    @Test
    void formatFailureUsesWriteErrorMessageAndCode() {
        BsonDocument response = BsonDocument.parse(
            "{\"ok\":1,\"writeErrors\":[{\"code\":11000,\"errmsg\":\"duplicate\",\"codeName\":\"DuplicateKey\"}]}"
        );

        String message = ScenarioBsonCodec.formatFailure("insert", 2, response);

        assertEquals(
            "command 'insert' failed at index 2: duplicate (code=11000, codeName=DuplicateKey)",
            message
        );
    }

    @Test
    void toRealMongodCommandDocumentTranslatesCountDocumentsToAggregatePipeline() {
        ScenarioCommand command = new ScenarioCommand(
            "countDocuments",
            Map.of(
                "countDocuments",
                "users",
                "filter",
                Map.of("role", "member"),
                "skip",
                1,
                "limit",
                2
            )
        );

        var commandDocument = ScenarioBsonCodec.toRealMongodCommandDocument(command, "testkit_real");

        assertEquals("users", commandDocument.getString("aggregate").getValue());
        assertTrue(commandDocument.get("pipeline").isArray());
        assertEquals(4, commandDocument.getArray("pipeline").size());
        assertTrue(commandDocument.get("cursor").isDocument());
    }

    @Test
    void toRealMongodCommandDocumentTranslatesCountDocumentsQueryAliasToMatchStage() {
        ScenarioCommand command = new ScenarioCommand(
            "countDocuments",
            Map.of(
                "countDocuments",
                "users",
                "query",
                Map.of("active", true)
            )
        );

        var commandDocument = ScenarioBsonCodec.toRealMongodCommandDocument(command, "testkit_real");

        BsonDocument firstStage = commandDocument.getArray("pipeline").get(0).asDocument();
        assertEquals(true, firstStage.getDocument("$match").getBoolean("active").getValue());
    }

    @Test
    void toRealMongodCommandDocumentTranslatesFindOneAndUpdateToFindAndModify() {
        ScenarioCommand command = new ScenarioCommand(
            "findOneAndUpdate",
            Map.of(
                "findOneAndUpdate",
                "users",
                "filter",
                Map.of("_id", 1),
                "update",
                Map.of("$set", Map.of("name", "after")),
                "arrayFilters",
                java.util.List.of(Map.of("elem.name", "before")),
                "returnDocument",
                "after"
            )
        );

        var commandDocument = ScenarioBsonCodec.toRealMongodCommandDocument(command, "testkit_real");

        assertEquals("users", commandDocument.getString("findAndModify").getValue());
        assertEquals(1, commandDocument.getDocument("query").getInt32("_id").getValue());
        assertTrue(commandDocument.containsKey("arrayFilters"));
        assertEquals("before", commandDocument.getArray("arrayFilters")
                .get(0)
                .asDocument()
                .getString("elem.name")
                .getValue());
        assertTrue(commandDocument.getBoolean("new").getValue());
    }
}
