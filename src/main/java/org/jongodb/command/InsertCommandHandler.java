package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.engine.DuplicateKeyException;

public final class InsertCommandHandler implements CommandHandler {
    private static final int CODE_INVALID_ARGUMENT = 14;

    private final CommandStore store;

    public InsertCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "insert");
        if (collection == null) {
            return CommandDispatcher.error("insert must be a string", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonValue documentsValue = command.get("documents");
        if (documentsValue == null || !documentsValue.isArray()) {
            return CommandDispatcher.error("documents must be an array", CODE_INVALID_ARGUMENT, "TypeMismatch");
        }

        final BsonArray documentsArray = documentsValue.asArray();
        final List<BsonDocument> documents = new ArrayList<>(documentsArray.size());
        for (final BsonValue value : documentsArray) {
            if (!value.isDocument()) {
                return CommandDispatcher.error(
                        "all entries in documents must be BSON documents", CODE_INVALID_ARGUMENT, "TypeMismatch");
            }
            documents.add(value.asDocument());
        }

        final int insertedCount;
        try {
            insertedCount = store.insert(database, collection, List.copyOf(documents));
        } catch (final DuplicateKeyException exception) {
            return CommandDispatcher.duplicateKeyError(exception.getMessage());
        }
        return new BsonDocument()
                .append("n", new BsonInt32(insertedCount))
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
