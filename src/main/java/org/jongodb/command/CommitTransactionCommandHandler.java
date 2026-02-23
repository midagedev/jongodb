package org.jongodb.command;

import org.bson.BsonDocument;
import org.bson.BsonDouble;

public final class CommitTransactionCommandHandler implements CommandHandler {
    @Override
    public BsonDocument handle(final BsonDocument command) {
        return new BsonDocument().append("ok", new BsonDouble(1.0));
    }
}
