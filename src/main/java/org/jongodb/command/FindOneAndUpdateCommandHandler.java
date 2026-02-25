package org.jongodb.command;

import java.util.Set;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.command.CommandCanonicalizer.ValidationException;

public final class FindOneAndUpdateCommandHandler implements CommandHandler {
    private static final Set<String> SUPPORTED_OPERATORS = Set.of("$set", "$setOnInsert", "$inc", "$unset", "$addToSet");

    private final FindAndModifyCommandHandler findAndModifyCommandHandler;

    public FindOneAndUpdateCommandHandler(final CommandStore store) {
        this.findAndModifyCommandHandler = new FindAndModifyCommandHandler(store);
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = CommandCanonicalizer.readDatabase(command);
        final String collection = CommandCanonicalizer.readCommandString(command, "findOneAndUpdate");
        if (collection == null) {
            return CommandErrors.typeMismatch("findOneAndUpdate must be a string");
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
        try {
            filter = CommandCanonicalizer.requireDocument(command, "filter", "filter must be a document");
        } catch (final ValidationException validationException) {
            return mapValidationError(validationException);
        }

        final BsonValue updateValue = command.get("update");
        final BsonDocument update;
        if (updateValue == null) {
            return CommandErrors.typeMismatch("update must be a document or array");
        } else if (updateValue.isDocument()) {
            update = updateValue.asDocument();
        } else if (updateValue.isArray()) {
            final UpdatePipelineSubset.ParseResult parsedPipeline = UpdatePipelineSubset.parse(updateValue.asArray());
            if (parsedPipeline.error() != null) {
                return parsedPipeline.error();
            }
            update = parsedPipeline.updateDocument();
        } else {
            return CommandErrors.typeMismatch("update must be a document or array");
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

        final UpdateArrayFiltersSubset.ParseResult parsedArrayFilters =
                UpdateArrayFiltersSubset.parse(command.get("arrayFilters"));
        if (parsedArrayFilters.error() != null) {
            return parsedArrayFilters.error();
        }

        optionError = validateOperatorUpdateDocument(update, parsedArrayFilters.parsed());
        if (optionError != null) {
            return optionError;
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
                .append("update", update)
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
        if (!parsedArrayFilters.parsed().isEmpty()) {
            translatedFindAndModify.append("arrayFilters", parsedArrayFilters.parsed().toBsonArray());
        }

        return findAndModifyCommandHandler.handle(translatedFindAndModify);
    }

    private static BsonDocument validateOperatorUpdateDocument(
            final BsonDocument update, final UpdateArrayFiltersSubset.ParsedArrayFilters parsedArrayFilters) {
        if (update.isEmpty()) {
            return CommandErrors.badValue("update must not be empty");
        }

        if (!update.getFirstKey().startsWith("$")) {
            return CommandErrors.badValue("update document must contain update operators");
        }
        for (final String key : update.keySet()) {
            if (!key.startsWith("$")) {
                return CommandErrors.badValue("update document must contain update operators");
            }
        }

        for (final String key : update.keySet()) {
            if (!SUPPORTED_OPERATORS.contains(key)) {
                return CommandErrors.badValue("unsupported update operator: " + key);
            }
        }

        final BsonValue setValue = update.get("$set");
        if (setValue != null && !setValue.isDocument()) {
            return CommandErrors.typeMismatch("$set must be a document");
        }

        final BsonValue setOnInsertValue = update.get("$setOnInsert");
        if (setOnInsertValue != null && !setOnInsertValue.isDocument()) {
            return CommandErrors.typeMismatch("$setOnInsert must be a document");
        }

        final BsonValue incValue = update.get("$inc");
        if (incValue != null && !incValue.isDocument()) {
            return CommandErrors.typeMismatch("$inc must be a document");
        }

        final BsonValue unsetValue = update.get("$unset");
        if (unsetValue != null && !unsetValue.isDocument()) {
            return CommandErrors.typeMismatch("$unset must be a document");
        }

        final BsonValue addToSetValue = update.get("$addToSet");
        if (addToSetValue != null && !addToSetValue.isDocument()) {
            return CommandErrors.typeMismatch("$addToSet must be a document");
        }

        return UpdateArrayFiltersSubset.validateUpdatePaths(update, parsedArrayFilters);
    }

    private static BsonDocument mapValidationError(final ValidationException validationException) {
        if (validationException.kind() == CommandCanonicalizer.ErrorKind.BAD_VALUE) {
            return CommandErrors.badValue(validationException.getMessage());
        }
        return CommandErrors.typeMismatch(validationException.getMessage());
    }
}
