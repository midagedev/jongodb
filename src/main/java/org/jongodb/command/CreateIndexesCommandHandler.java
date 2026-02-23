package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.engine.DuplicateKeyException;

public final class CreateIndexesCommandHandler implements CommandHandler {
    private final CommandStore store;

    public CreateIndexesCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "createIndexes");
        if (collection == null) {
            return CommandErrors.typeMismatch("createIndexes must be a string");
        }

        final BsonValue indexesValue = command.get("indexes");
        if (indexesValue == null || !indexesValue.isArray()) {
            return CommandErrors.typeMismatch("indexes must be an array");
        }

        final BsonArray indexesArray = indexesValue.asArray();
        if (indexesArray.isEmpty()) {
            return CommandErrors.badValue("indexes must not be empty");
        }

        final List<CommandStore.IndexRequest> indexes = new ArrayList<>(indexesArray.size());
        for (final BsonValue value : indexesArray) {
            if (!value.isDocument()) {
                return CommandErrors.typeMismatch("all entries in indexes must be BSON documents");
            }

            final BsonDocument indexSpec = value.asDocument();
            final String name = readRequiredString(indexSpec, "name");
            if (name == null) {
                return CommandErrors.typeMismatch("name must be a string");
            }

            final BsonDocument key = readRequiredDocument(indexSpec, "key");
            if (key == null) {
                return CommandErrors.typeMismatch("key must be a document");
            }
            if (key.isEmpty()) {
                return CommandErrors.badValue("key must not be empty");
            }

            final BsonValue uniqueValue = indexSpec.get("unique");
            final boolean unique;
            if (uniqueValue == null) {
                unique = false;
            } else if (!uniqueValue.isBoolean()) {
                return CommandErrors.typeMismatch("unique must be a boolean");
            } else {
                unique = uniqueValue.asBoolean().getValue();
            }

            indexes.add(new CommandStore.IndexRequest(name, key, unique));
        }

        final CommandStore.CreateIndexesResult result;
        try {
            result = store.createIndexes(database, collection, List.copyOf(indexes));
        } catch (final DuplicateKeyException exception) {
            return CommandErrors.duplicateKey(exception.getMessage());
        }

        return new BsonDocument()
                .append("createdCollectionAutomatically", BsonBoolean.FALSE)
                .append("numIndexesBefore", new BsonInt32(result.numIndexesBefore()))
                .append("numIndexesAfter", new BsonInt32(result.numIndexesAfter()))
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

    private static BsonDocument readRequiredDocument(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        if (value == null || !value.isDocument()) {
            return null;
        }
        return value.asDocument();
    }
}
