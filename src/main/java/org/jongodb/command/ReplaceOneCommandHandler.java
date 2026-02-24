package org.jongodb.command;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class ReplaceOneCommandHandler implements CommandHandler {
    private final UpdateCommandHandler updateCommandHandler;

    public ReplaceOneCommandHandler(final CommandStore store) {
        this.updateCommandHandler = new UpdateCommandHandler(store);
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readCollection(command);
        if (collection == null) {
            return CommandErrors.typeMismatch("replaceOne must be a string");
        }

        BsonDocument optionError = CrudCommandOptionValidator.validateWriteConcern(command);
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateReadConcern(command);
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateHint(command, "hint");
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateCollation(command, "collation");
        if (optionError != null) {
            return optionError;
        }

        final BsonDocument filter = readRequiredDocument(command, "filter");
        if (filter == null) {
            return CommandErrors.typeMismatch("filter must be a document");
        }

        final BsonDocument replacement = readRequiredDocument(command, "replacement");
        if (replacement == null) {
            return CommandErrors.typeMismatch("replacement must be a document");
        }
        if (replacement.isEmpty()) {
            return CommandErrors.badValue("replacement must not be empty");
        }
        if (containsTopLevelOperator(replacement)) {
            return CommandErrors.badValue("replacement document must not contain update operators");
        }

        final BsonValue upsertValue = command.get("upsert");
        final boolean upsert;
        if (upsertValue == null) {
            upsert = false;
        } else if (!upsertValue.isBoolean()) {
            return CommandErrors.typeMismatch("upsert must be a boolean");
        } else {
            upsert = upsertValue.asBoolean().getValue();
        }

        final BsonDocument updateSpec = new BsonDocument()
                .append("q", filter)
                .append("u", replacement)
                .append("multi", BsonBoolean.FALSE)
                .append("upsert", BsonBoolean.valueOf(upsert));
        appendIfPresent(command, updateSpec, "hint");
        appendIfPresent(command, updateSpec, "collation");

        final BsonDocument translatedUpdateCommand = new BsonDocument()
                .append("update", new BsonString(collection))
                .append("$db", new BsonString(database))
                .append("updates", new BsonArray(List.of(updateSpec)));
        appendIfPresent(command, translatedUpdateCommand, "ordered");
        appendIfPresent(command, translatedUpdateCommand, "writeConcern");
        appendIfPresent(command, translatedUpdateCommand, "readConcern");

        return updateCommandHandler.handle(translatedUpdateCommand);
    }

    private static boolean containsTopLevelOperator(final BsonDocument document) {
        for (final String key : document.keySet()) {
            if (key.startsWith("$")) {
                return true;
            }
        }
        return false;
    }

    private static void appendIfPresent(final BsonDocument source, final BsonDocument target, final String key) {
        if (!source.containsKey(key)) {
            return;
        }
        target.append(key, source.get(key));
    }

    private static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    private static String readCollection(final BsonDocument command) {
        final BsonValue canonical = command.get("replaceOne");
        if (canonical != null) {
            if (canonical.isString()) {
                return canonical.asString().getValue();
            }
            return null;
        }

        for (final String key : command.keySet()) {
            if (!"replaceone".equalsIgnoreCase(key)) {
                continue;
            }
            final BsonValue value = command.get(key);
            if (value != null && value.isString()) {
                return value.asString().getValue();
            }
            return null;
        }
        return null;
    }

    private static BsonDocument readRequiredDocument(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        if (value == null || !value.isDocument()) {
            return null;
        }
        return value.asDocument();
    }
}
