package org.jongodb.command;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.command.CommandCanonicalizer.ValidationException;

public final class FindOneAndReplaceCommandHandler implements CommandHandler {
    private final FindAndModifyCommandHandler findAndModifyCommandHandler;

    public FindOneAndReplaceCommandHandler(final CommandStore store) {
        this.findAndModifyCommandHandler = new FindAndModifyCommandHandler(store);
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = CommandCanonicalizer.readDatabase(command);
        final String collection = CommandCanonicalizer.readCommandString(command, "findOneAndReplace");
        if (collection == null) {
            return CommandErrors.typeMismatch("findOneAndReplace must be a string");
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

        final BsonDocument sort;
        final BsonDocument projection;
        try {
            sort = CommandCanonicalizer.optionalDocument(command, "sort", "sort must be a document");
            projection = CommandCanonicalizer.optionalDocument(command, "projection", "projection must be a document");
        } catch (final ValidationException validationException) {
            return mapValidationError(validationException);
        }

        final boolean upsert;
        try {
            upsert = CommandCanonicalizer.optionalBoolean(command, "upsert", false, "upsert must be a boolean");
        } catch (final ValidationException validationException) {
            return mapValidationError(validationException);
        }

        final boolean returnNew;
        try {
            returnNew = CommandCanonicalizer.parseReturnDocumentAsAfter(command);
        } catch (final ValidationException validationException) {
            return mapValidationError(validationException);
        }

        final BsonDocument translatedFindAndModify = new BsonDocument()
                .append("findAndModify", new BsonString(collection))
                .append("$db", new BsonString(database))
                .append("query", filter)
                .append("update", replacement)
                .append("remove", BsonBoolean.FALSE)
                .append("new", BsonBoolean.valueOf(returnNew))
                .append("upsert", BsonBoolean.valueOf(upsert));
        if (sort != null) {
            translatedFindAndModify.append("sort", sort);
        }
        if (projection != null) {
            translatedFindAndModify.append("fields", projection);
        }

        CommandCanonicalizer.appendIfPresent(command, translatedFindAndModify, "hint");
        CommandCanonicalizer.appendIfPresent(command, translatedFindAndModify, "collation");

        return findAndModifyCommandHandler.handle(translatedFindAndModify);
    }

    private static BsonDocument mapValidationError(final ValidationException validationException) {
        if (validationException.kind() == CommandCanonicalizer.ErrorKind.BAD_VALUE) {
            return CommandErrors.badValue(validationException.getMessage());
        }
        return CommandErrors.typeMismatch(validationException.getMessage());
    }
}
