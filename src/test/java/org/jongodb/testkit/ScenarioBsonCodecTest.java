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
}
