package org.jongodb.obs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class StructuredJsonLinesLoggerSmokeTest {
    @Test
    void emitsCorrelationAndCustomFieldsAsJsonLines() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Clock fixedClock = Clock.fixed(Instant.parse("2026-02-23T10:00:00Z"), ZoneOffset.UTC);
        StructuredJsonLinesLogger logger = new StructuredJsonLinesLogger(output, fixedClock, true);

        CorrelationContext context = CorrelationContext.builder("req-1", "find")
            .sessionId("session-42")
            .txnNumber(9L)
            .build();
        logger.info("command started", context, Map.of("namespace", "db.coll"));

        CorrelationContext noTxnContext = CorrelationContext.of("req-2", "ping");
        logger.info("health check", noTxnContext);
        logger.close();

        String[] lines = output.toString(StandardCharsets.UTF_8).trim().split("\\R");
        assertEquals(2, lines.length);

        Document first = Document.parse(lines[0]);
        assertEquals("2026-02-23T10:00:00Z", first.getString("timestamp"));
        assertEquals("INFO", first.getString("level"));
        assertEquals("command started", first.getString("message"));
        assertEquals("req-1", first.getString("requestId"));
        assertEquals("find", first.getString("commandName"));
        assertEquals("session-42", first.getString("sessionId"));
        assertEquals(9, ((Number) first.get("txnNumber")).intValue());
        assertEquals("db.coll", first.getString("namespace"));

        Document second = Document.parse(lines[1]);
        assertEquals("2026-02-23T10:00:00Z", second.getString("timestamp"));
        assertEquals("INFO", second.getString("level"));
        assertEquals("health check", second.getString("message"));
        assertEquals("req-2", second.getString("requestId"));
        assertEquals("ping", second.getString("commandName"));
        assertNull(second.get("sessionId"));
        assertNull(second.get("txnNumber"));
        assertFalse(second.containsKey("namespace"));
    }
}
