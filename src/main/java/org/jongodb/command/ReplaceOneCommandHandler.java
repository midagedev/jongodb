package org.jongodb.command;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.jongodb.command.CommandCanonicalizer.ValidationException;

public final class ReplaceOneCommandHandler implements CommandHandler {
    private final UpdateCommandHandler updateCommandHandler;

    public ReplaceOneCommandHandler(final CommandStore store) {
        this.updateCommandHandler = new UpdateCommandHandler(store);
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = CommandCanonicalizer.readDatabase(command);
        final String collection = CommandCanonicalizer.readCommandString(command, "replaceOne");
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

        final BsonDocument filter;
        final BsonDocument replacement;
        try {
            filter = CommandCanonicalizer.requireDocument(command, "filter", "filter must be a document");
            replacement = CommandCanonicalizer.requireDocument(
                    command,
                    "replacement",
                    "replacement must be a document");
        } catch (final ValidationException validationException) {
            return mapValidationError(validationException);
        }
        if (replacement.isEmpty()) {
            return CommandErrors.badValue("replacement must not be empty");
        }
        if (CommandCanonicalizer.containsTopLevelOperator(replacement)) {
            return CommandErrors.badValue("replacement document must not contain update operators");
        }

        final boolean upsert;
        try {
            upsert = CommandCanonicalizer.optionalBoolean(command, "upsert", false, "upsert must be a boolean");
        } catch (final ValidationException validationException) {
            return mapValidationError(validationException);
        }

        final BsonDocument updateSpec = new BsonDocument()
                .append("q", filter)
                .append("u", replacement)
                .append("multi", BsonBoolean.FALSE)
                .append("upsert", BsonBoolean.valueOf(upsert));
        CommandCanonicalizer.appendIfPresent(command, updateSpec, "hint");
        CommandCanonicalizer.appendIfPresent(command, updateSpec, "collation");

        final BsonDocument translatedUpdateCommand = new BsonDocument()
                .append("update", new BsonString(collection))
                .append("$db", new BsonString(database))
                .append("updates", new BsonArray(List.of(updateSpec)));
        CommandCanonicalizer.appendIfPresent(command, translatedUpdateCommand, "ordered");
        CommandCanonicalizer.appendIfPresent(command, translatedUpdateCommand, "writeConcern");
        CommandCanonicalizer.appendIfPresent(command, translatedUpdateCommand, "readConcern");

        return updateCommandHandler.handle(translatedUpdateCommand);
    }

    private static BsonDocument mapValidationError(final ValidationException validationException) {
        if (validationException.kind() == CommandCanonicalizer.ErrorKind.BAD_VALUE) {
            return CommandErrors.badValue(validationException.getMessage());
        }
        return CommandErrors.typeMismatch(validationException.getMessage());
    }
}
