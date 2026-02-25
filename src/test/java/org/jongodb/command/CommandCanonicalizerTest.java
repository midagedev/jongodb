package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class CommandCanonicalizerTest {
    @Test
    void readCommandStringSupportsCaseInsensitiveAliases() {
        final BsonDocument command = BsonDocument.parse("{\"CoUnT\": \"users\"}");

        assertEquals("users", CommandCanonicalizer.readCommandString(command, "countDocuments", "count"));
    }

    @Test
    void parseReturnDocumentAsAfterSupportsReturnDocumentAndNewFallback() {
        assertTrue(CommandCanonicalizer.parseReturnDocumentAsAfter(BsonDocument.parse("{\"returnDocument\":\"after\"}")));
        assertFalse(CommandCanonicalizer.parseReturnDocumentAsAfter(BsonDocument.parse("{\"returnDocument\":\"before\"}")));
        assertTrue(CommandCanonicalizer.parseReturnDocumentAsAfter(BsonDocument.parse("{\"new\":true}")));
        assertFalse(CommandCanonicalizer.parseReturnDocumentAsAfter(new BsonDocument()));
    }

    @Test
    void parseReturnDocumentAsAfterReportsValidationKindForInvalidValues() {
        final CommandCanonicalizer.ValidationException invalidType = assertThrows(
                CommandCanonicalizer.ValidationException.class,
                () -> CommandCanonicalizer.parseReturnDocumentAsAfter(BsonDocument.parse("{\"returnDocument\":1}")));
        assertEquals(CommandCanonicalizer.ErrorKind.TYPE_MISMATCH, invalidType.kind());
        assertEquals("returnDocument must be a string", invalidType.getMessage());

        final CommandCanonicalizer.ValidationException invalidValue = assertThrows(
                CommandCanonicalizer.ValidationException.class,
                () -> CommandCanonicalizer.parseReturnDocumentAsAfter(BsonDocument.parse("{\"returnDocument\":\"later\"}")));
        assertEquals(CommandCanonicalizer.ErrorKind.BAD_VALUE, invalidValue.kind());
        assertEquals("returnDocument must be 'before' or 'after'", invalidValue.getMessage());
    }

    @Test
    void optionalBooleanRejectsNonBooleanValues() {
        final CommandCanonicalizer.ValidationException exception = assertThrows(
                CommandCanonicalizer.ValidationException.class,
                () -> CommandCanonicalizer.optionalBoolean(
                        BsonDocument.parse("{\"upsert\":\"yes\"}"),
                        "upsert",
                        false,
                        "upsert must be a boolean"));
        assertEquals(CommandCanonicalizer.ErrorKind.TYPE_MISMATCH, exception.kind());
        assertEquals("upsert must be a boolean", exception.getMessage());
    }
}

