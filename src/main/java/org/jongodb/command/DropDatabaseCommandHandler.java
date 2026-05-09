package org.jongodb.command;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class DropDatabaseCommandHandler implements CommandHandler {
    private final CommandStore store;

    public DropDatabaseCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);

        final BsonDocument optionError = CrudCommandOptionValidator.validateWriteConcern(command);
        if (optionError != null) {
            return optionError;
        }

        store.dropDatabase(database);
        return new BsonDocument()
                .append("dropped", new BsonString(database))
                .append("ok", new BsonDouble(1.0));
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }
}
