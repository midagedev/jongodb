package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jongodb.obs.StructuredJsonLinesLogger;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;
import org.junit.jupiter.api.Test;

class WireCommandIngressE2ETest {
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-02-23T10:00:00Z"), ZoneOffset.UTC);

    @Test
    void handlesWireCommandsAndEmitsCorrelationLogs() {
        final ByteArrayOutputStream logOutput = new ByteArrayOutputStream();
        final StructuredJsonLinesLogger logger = new StructuredJsonLinesLogger(logOutput, FIXED_CLOCK, true);
        final WireCommandIngress ingress = WireCommandIngress.inMemory(logger);
        final OpMsgCodec codec = new OpMsgCodec();

        final OpMsg helloResponse = roundTrip(
                ingress, codec, 101, BsonDocument.parse("{\"hello\": 1, \"$db\": \"admin\"}"));
        assertEquals(1, helloResponse.requestId());
        assertEquals(101, helloResponse.responseTo());
        assertEquals(1.0, helloResponse.body().get("ok").asNumber().doubleValue());
        assertTrue(helloResponse.body().getBoolean("helloOk").getValue());

        final OpMsg pingResponse =
                roundTrip(ingress, codec, 102, BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}"));
        assertEquals(2, pingResponse.requestId());
        assertEquals(102, pingResponse.responseTo());
        assertEquals(1.0, pingResponse.body().get("ok").asNumber().doubleValue());

        final OpMsg insertResponse = roundTrip(
                ingress,
                codec,
                103,
                BsonDocument.parse(
                        "{\"insert\": \"users\", \"$db\": \"app\", \"documents\": [{\"_id\": 1, \"name\": \"alpha\"}, {\"_id\": 2, \"name\": \"beta\"}]}"));
        assertEquals(3, insertResponse.requestId());
        assertEquals(103, insertResponse.responseTo());
        assertEquals(1.0, insertResponse.body().get("ok").asNumber().doubleValue());
        assertEquals(2, insertResponse.body().getInt32("n").getValue());

        final OpMsg findResponse = roundTrip(
                ingress,
                codec,
                104,
                BsonDocument.parse("{\"find\": \"users\", \"$db\": \"app\", \"filter\": {\"name\": \"alpha\"}}"));
        assertEquals(4, findResponse.requestId());
        assertEquals(104, findResponse.responseTo());
        assertEquals(1.0, findResponse.body().get("ok").asNumber().doubleValue());
        final BsonDocument cursor = findResponse.body().getDocument("cursor");
        assertEquals("app.users", cursor.getString("ns").getValue());
        final BsonArray firstBatch = cursor.getArray("firstBatch");
        assertEquals(1, firstBatch.size());
        assertEquals("alpha", firstBatch.get(0).asDocument().getString("name").getValue());

        logger.close();

        final String[] logLines = logOutput.toString(StandardCharsets.UTF_8).trim().split("\\R");
        assertEquals(8, logLines.length);
        assertEquals(4, countByMessage(logLines, "command.start"));
        assertEquals(4, countByMessage(logLines, "command.complete"));
        assertEquals(2, countByCorrelation(logLines, "101", "hello"));
        assertEquals(2, countByCorrelation(logLines, "102", "ping"));
        assertEquals(2, countByCorrelation(logLines, "103", "insert"));
        assertEquals(2, countByCorrelation(logLines, "104", "find"));
    }

    private static OpMsg roundTrip(
            final WireCommandIngress ingress, final OpMsgCodec codec, final int requestId, final BsonDocument body) {
        final OpMsg request = new OpMsg(requestId, 0, 0, body);
        final byte[] responseBytes = ingress.handle(codec.encode(request));
        return codec.decode(responseBytes);
    }

    private static int countByMessage(final String[] lines, final String message) {
        return (int) Arrays.stream(lines)
                .filter(line -> line.contains("\"message\":\"" + message + "\""))
                .count();
    }

    private static int countByCorrelation(final String[] lines, final String requestId, final String commandName) {
        return (int) Arrays.stream(lines)
                .filter(line -> line.contains("\"requestId\":\"" + requestId + "\""))
                .filter(line -> line.contains("\"commandName\":\"" + commandName + "\""))
                .count();
    }
}
