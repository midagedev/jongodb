package org.jongodb.command;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class DropCommandHandler implements CommandHandler {
    private final CommandStore store;

    public DropCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "drop");
        if (collection == null) {
            return CommandErrors.typeMismatch("drop must be a string");
        }

        final BsonDocument optionError = CrudCommandOptionValidator.validateWriteConcern(command);
        if (optionError != null) {
            return optionError;
        }

        final CommandStore.DropCollectionResult result = store.dropCollection(database, collection);
        final String namespace = database + "." + collection;
        if (!result.dropped()) {
            return CommandErrors.namespaceNotFound(namespace);
        }
        return new BsonDocument()
                .append("nIndexesWas", new BsonInt32(result.nIndexesWas()))
                .append("ns", new BsonString(namespace))
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
