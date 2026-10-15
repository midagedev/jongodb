package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.junit.jupiter.api.Test;

class HelloCommandHandlerTest {
    private final HelloCommandHandler handler =
            new HelloCommandHandler(TopologyProfile.SINGLE_NODE_REPLICA_SET, "127.0.0.1:27017", "rs0");

    @Test
    void awaitableHelloWithCurrentTopologyVersionWaitsForMaxAwaitTime() {
        final BsonDocument first = handler.handle(BsonDocument.parse("{\"hello\": 1}"));
        final BsonDocument awaitable = new BsonDocument("hello", new BsonInt64(1))
                .append("topologyVersion", first.getDocument("topologyVersion"))
                .append("maxAwaitTimeMS", new BsonInt64(300));

        final long started = System.nanoTime();
        handler.handle(awaitable);
        final long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

        assertTrue(elapsedMillis >= 250, "awaitable hello returned after " + elapsedMillis + " ms");
    }

    @Test
    void plainHelloAndForeignTopologyVersionAnswerImmediately() {
        final BsonDocument foreign = BsonDocument.parse(
                "{\"hello\": 1, \"maxAwaitTimeMS\": 5000,"
                        + " \"topologyVersion\": {\"processId\": {\"$oid\": \"000000000000000000000000\"}, \"counter\": 1}}");

        final long started = System.nanoTime();
        handler.handle(BsonDocument.parse("{\"hello\": 1}"));
        handler.handle(foreign);
        final long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

        assertTrue(elapsedMillis < 1_000, "non-awaitable hello took " + elapsedMillis + " ms");
    }
}
