package org.jongodb.command;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;

public final class BuildInfoCommandHandler implements CommandHandler {
    @Override
    public BsonDocument handle(final BsonDocument command) {
        return new BsonDocument()
                .append("version", new BsonString("8.0.0-jongodb"))
                .append(
                        "versionArray",
                        new BsonArray(List.of(new BsonInt32(8), new BsonInt32(0), new BsonInt32(0), new BsonInt32(0))))
                .append("gitVersion", new BsonString("jongodb"))
                .append("modules", new BsonArray())
                .append("maxBsonObjectSize", new BsonInt32(16 * 1024 * 1024))
                .append("ok", new BsonDouble(1.0));
    }
}
