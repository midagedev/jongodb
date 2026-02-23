package org.jongodb.command;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class FindCommandHandler implements CommandHandler {
    private static final int CODE_INVALID_ARGUMENT = 14;

    private final CommandStore store;

    public FindCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "find");
        if (collection == null) {
            return CommandDispatcher.error("find must be a string", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonValue filterValue = command.get("filter");
        final BsonDocument filter;
        if (filterValue == null) {
            filter = new BsonDocument();
        } else if (filterValue.isDocument()) {
            filter = filterValue.asDocument();
        } else {
            return CommandDispatcher.error("filter must be a document", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final List<BsonDocument> foundDocuments = store.find(database, collection, filter);
        final BsonArray firstBatch = new BsonArray();
        for (final BsonDocument foundDocument : foundDocuments) {
            firstBatch.add(foundDocument);
        }

        final BsonDocument cursor = new BsonDocument()
                .append("id", new BsonInt64(0))
                .append("ns", new BsonString(database + "." + collection))
                .append("firstBatch", firstBatch);

        return new BsonDocument()
                .append("cursor", cursor)
                .append("ok", new BsonDouble(1.0));
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static String readRequiredString(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        if (!(value instanceof BsonString bsonString)) {
            return null;
        }
        return bsonString.getValue();
    }
}
