package org.jongodb.obs;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

public final class StructuredJsonLinesLoggerSmokeTest {
    public static void main(String[] args) {
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
        expect(lines.length == 2, "expected 2 log lines");

        String first = lines[0];
        expect(first.contains("\"requestId\":\"req-1\""), "missing requestId");
        expect(first.contains("\"commandName\":\"find\""), "missing commandName");
        expect(first.contains("\"sessionId\":\"session-42\""), "missing sessionId");
        expect(first.contains("\"txnNumber\":9"), "missing txnNumber");
        expect(first.contains("\"namespace\":\"db.coll\""), "missing custom field");

        String second = lines[1];
        expect(second.contains("\"requestId\":\"req-2\""), "missing second requestId");
        expect(second.contains("\"commandName\":\"ping\""), "missing second commandName");
        expect(!second.contains("\"sessionId\""), "unexpected sessionId field");
        expect(!second.contains("\"txnNumber\""), "unexpected txnNumber field");
    }

    private static void expect(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
