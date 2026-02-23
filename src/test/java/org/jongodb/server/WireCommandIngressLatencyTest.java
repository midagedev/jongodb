package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;
import org.junit.jupiter.api.Test;

class WireCommandIngressLatencyTest {
    @Test
    void p95LatencyIsUnderEightMillisecondsForCrudPath() {
        final WireCommandIngress ingress = WireCommandIngress.inMemory();
        final OpMsgCodec codec = new OpMsgCodec();

        // Seed baseline documents.
        send(
                ingress,
                codec,
                1,
                BsonDocument.parse(
                        "{\"insert\":\"users\",\"$db\":\"bench\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"},{\"_id\":3,\"name\":\"c\"}]}"));

        final List<BsonDocument> commands = List.of(
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"bench\",\"filter\":{\"name\":\"a\"}}"),
                BsonDocument.parse("{\"update\":\"users\",\"$db\":\"bench\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"aa\"}},\"multi\":false}]}"),
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"bench\",\"filter\":{\"_id\":1}}"),
                BsonDocument.parse("{\"delete\":\"users\",\"$db\":\"bench\",\"deletes\":[{\"q\":{\"_id\":3},\"limit\":1}]}"),
                BsonDocument.parse(
                        "{\"insert\":\"users\",\"$db\":\"bench\",\"documents\":[{\"_id\":3,\"name\":\"c\"}]}"));

        // Warm-up.
        int requestId = 100;
        for (int i = 0; i < 100; i++) {
            send(ingress, codec, requestId++, commands.get(i % commands.size()));
        }

        final List<Long> samplesNanos = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            final BsonDocument command = commands.get(i % commands.size());
            final long startedAt = System.nanoTime();
            send(ingress, codec, requestId++, command);
            samplesNanos.add(System.nanoTime() - startedAt);
        }

        Collections.sort(samplesNanos);
        final int p95Index = (int) Math.ceil(samplesNanos.size() * 0.95) - 1;
        final long p95Nanos = samplesNanos.get(Math.max(0, p95Index));
        final double p95Millis = p95Nanos / 1_000_000.0;

        assertTrue(p95Millis <= 8.0, "p95 latency was " + p95Millis + "ms");
    }

    private static void send(
            final WireCommandIngress ingress,
            final OpMsgCodec codec,
            final int requestId,
            final BsonDocument body) {
        final OpMsg request = new OpMsg(requestId, 0, 0, body);
        final byte[] responseBytes = ingress.handle(codec.encode(request));
        codec.decode(responseBytes);
    }
}

