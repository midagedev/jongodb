package org.jongodb.command;

import org.bson.BsonDocument;

public interface CommandHandler {
    BsonDocument handle(BsonDocument command);
}
