package org.jongodb.command;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;

public final class HelloCommandHandler implements CommandHandler {
    @Override
    public BsonDocument handle(final BsonDocument command) {
        return new BsonDocument()
                .append("isWritablePrimary", BsonBoolean.TRUE)
                .append("ismaster", BsonBoolean.TRUE)
                .append("helloOk", BsonBoolean.TRUE)
                .append("minWireVersion", new BsonInt32(0))
                .append("maxWireVersion", new BsonInt32(17))
                .append("maxBsonObjectSize", new BsonInt32(16 * 1024 * 1024))
                .append("maxMessageSizeBytes", new BsonInt32(48_000_000))
                .append("maxWriteBatchSize", new BsonInt32(100_000))
                .append("logicalSessionTimeoutMinutes", new BsonInt32(30))
                .append("connectionId", new BsonInt32(1))
                .append("ok", new BsonDouble(1.0));
    }
}
