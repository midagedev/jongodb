package org.jongodb.server;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.jongodb.command.CommandStore;
import org.jongodb.command.CommandDispatcher;
import org.jongodb.command.EngineBackedCommandStore;
import org.jongodb.engine.InMemoryEngineStore;
import org.jongodb.obs.CorrelationContext;
import org.jongodb.obs.JsonLinesLogger;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

public final class WireCommandIngress {
    private static final JsonLinesLogger NOOP_LOGGER = new JsonLinesLogger() {
        @Override
        public void log(
                final String level,
                final String message,
                final CorrelationContext correlationContext,
                final Map<String, ?> fields) {}

        @Override
        public void close() {}
    };

    private final CommandDispatcher dispatcher;
    private final OpMsgCodec codec;
    private final AtomicInteger responseRequestId;
    private final JsonLinesLogger logger;

    public WireCommandIngress(final CommandDispatcher dispatcher) {
        this(dispatcher, new OpMsgCodec(), 1, NOOP_LOGGER);
    }

    public WireCommandIngress(
            final CommandDispatcher dispatcher, final OpMsgCodec codec, final int initialResponseRequestId) {
        this(dispatcher, codec, initialResponseRequestId, NOOP_LOGGER);
    }

    public WireCommandIngress(
            final CommandDispatcher dispatcher,
            final OpMsgCodec codec,
            final int initialResponseRequestId,
            final JsonLinesLogger logger) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.responseRequestId = new AtomicInteger(initialResponseRequestId);
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    public static WireCommandIngress inMemory() {
        return inMemory(NOOP_LOGGER);
    }

    public static WireCommandIngress inMemory(final JsonLinesLogger logger) {
        final CommandStore commandStore = new EngineBackedCommandStore(new InMemoryEngineStore());
        final CommandDispatcher dispatcher = new CommandDispatcher(commandStore);
        return new WireCommandIngress(dispatcher, new OpMsgCodec(), 1, logger);
    }

    public byte[] handle(final byte[] requestBytes) {
        final OpMsg request = codec.decode(requestBytes);
        final String commandName = readCommandName(request.body());
        final CorrelationContext correlation = CorrelationContext.of(Integer.toString(request.requestId()), commandName);

        logger.info("command.start", correlation);
        final BsonDocument responseBody;
        try {
            responseBody = dispatcher.dispatch(request.body());
        } finally {
            logger.info("command.complete", correlation);
        }

        final OpMsg response =
                new OpMsg(responseRequestId.getAndIncrement(), request.requestId(), 0, responseBody);
        return codec.encode(response);
    }

    private static String readCommandName(final BsonDocument command) {
        if (command == null || command.isEmpty()) {
            return "unknown";
        }
        return command.getFirstKey().toLowerCase(Locale.ROOT);
    }
}
