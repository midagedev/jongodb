package org.jongodb.command;

import java.util.Set;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class FindOneAndUpdateCommandHandler implements CommandHandler {
    private static final Set<String> SUPPORTED_OPERATORS = Set.of("$set", "$setOnInsert", "$inc", "$unset", "$addToSet");

    private final FindAndModifyCommandHandler findAndModifyCommandHandler;

    public FindOneAndUpdateCommandHandler(final CommandStore store) {
        this.findAndModifyCommandHandler = new FindAndModifyCommandHandler(store);
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readCollection(command);
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

        final BsonDocument filter = readRequiredDocument(command, "filter");
        if (filter == null) {
            return CommandErrors.typeMismatch("filter must be a document");
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

        final BsonValue sortValue = command.get("sort");
        final BsonDocument sort;
        if (sortValue == null) {
            sort = null;
        } else if (!sortValue.isDocument()) {
            return CommandErrors.typeMismatch("sort must be a document");
        } else {
            sort = sortValue.asDocument();
        }

        final BsonValue projectionValue = command.get("projection");
        final BsonDocument projection;
        if (projectionValue == null) {
            projection = null;
        } else if (!projectionValue.isDocument()) {
            return CommandErrors.typeMismatch("projection must be a document");
        } else {
            projection = projectionValue.asDocument();
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

        final UpdateArrayFiltersSubset.ParseResult parsedArrayFilters =
                UpdateArrayFiltersSubset.parse(command.get("arrayFilters"));
        if (parsedArrayFilters.error() != null) {
            return parsedArrayFilters.error();
        }

        optionError = validateOperatorUpdateDocument(update, parsedArrayFilters.parsed());
        if (optionError != null) {
            return optionError;
        }

        final ParsedReturnDocument parsedReturnDocument = parseReturnDocument(command);
        if (parsedReturnDocument.error() != null) {
            return parsedReturnDocument.error();
        }

        final BsonDocument translatedFindAndModify = new BsonDocument()
                .append("findAndModify", new BsonString(collection))
                .append("$db", new BsonString(database))
                .append("query", filter)
                .append("update", update)
                .append("remove", BsonBoolean.FALSE)
                .append("new", BsonBoolean.valueOf(parsedReturnDocument.returnNew()))
                .append("upsert", BsonBoolean.valueOf(upsert));
        if (sort != null) {
            translatedFindAndModify.append("sort", sort);
        }
        if (projection != null) {
            translatedFindAndModify.append("fields", projection);
        }

        appendIfPresent(command, translatedFindAndModify, "hint");
        appendIfPresent(command, translatedFindAndModify, "collation");
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

    private static ParsedReturnDocument parseReturnDocument(final BsonDocument command) {
        final BsonValue returnDocumentValue = command.get("returnDocument");
        if (returnDocumentValue == null) {
            final BsonValue newValue = command.get("new");
            if (newValue == null) {
                return new ParsedReturnDocument(false, null);
            }
            if (!newValue.isBoolean()) {
                return new ParsedReturnDocument(false, CommandErrors.typeMismatch("new must be a boolean"));
            }
            return new ParsedReturnDocument(newValue.asBoolean().getValue(), null);
        }

        if (!returnDocumentValue.isString()) {
            return new ParsedReturnDocument(false, CommandErrors.typeMismatch("returnDocument must be a string"));
        }

        final String value = returnDocumentValue.asString().getValue();
        if ("before".equals(value)) {
            return new ParsedReturnDocument(false, null);
        }
        if ("after".equals(value)) {
            return new ParsedReturnDocument(true, null);
        }
        return new ParsedReturnDocument(false, CommandErrors.badValue("returnDocument must be 'before' or 'after'"));
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
        final BsonValue canonical = command.get("findOneAndUpdate");
        if (canonical != null) {
            if (canonical.isString()) {
                return canonical.asString().getValue();
            }
            return null;
        }

        for (final String key : command.keySet()) {
            if (!"findoneandupdate".equalsIgnoreCase(key)) {
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

    private record ParsedReturnDocument(boolean returnNew, BsonDocument error) {}
}
