package org.jongodb.server;

import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.jongodb.command.CommandDispatcher;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

public final class WireCommandIngress {
    private final CommandDispatcher dispatcher;
    private final OpMsgCodec codec;
    private final AtomicInteger responseRequestId;

    public WireCommandIngress(final CommandDispatcher dispatcher) {
        this(dispatcher, new OpMsgCodec(), 1);
    }

    public WireCommandIngress(
            final CommandDispatcher dispatcher, final OpMsgCodec codec, final int initialResponseRequestId) {
        this.dispatcher = dispatcher;
        this.codec = codec;
        this.responseRequestId = new AtomicInteger(initialResponseRequestId);
    }

    public byte[] handle(final byte[] requestBytes) {
        final OpMsg request = codec.decode(requestBytes);
        final BsonDocument responseBody = dispatcher.dispatch(request.body());

        final OpMsg response =
                new OpMsg(responseRequestId.getAndIncrement(), request.requestId(), 0, responseBody);
        return codec.encode(response);
    }
}
