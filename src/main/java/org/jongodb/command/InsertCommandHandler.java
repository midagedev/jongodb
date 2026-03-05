package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.engine.DuplicateKeyException;

public final class InsertCommandHandler implements CommandHandler {
    private final CommandStore store;

    public InsertCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = readDatabase(command);
        final String collection = readRequiredString(command, "insert");
        if (collection == null) {
            return CommandErrors.typeMismatch("insert must be a string");
        }

        BsonDocument optionError = CrudCommandOptionValidator.validateOrdered(command);
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateWriteConcern(command);
        if (optionError != null) {
            return optionError;
        }
        optionError = CrudCommandOptionValidator.validateReadConcern(command);
        if (optionError != null) {
            return optionError;
        }

        final BsonValue documentsValue = command.get("documents");
        if (documentsValue == null || !documentsValue.isArray()) {
            return CommandErrors.typeMismatch("documents must be an array");
        }

        final BsonArray documentsArray = documentsValue.asArray();
        final List<BsonDocument> documents = new ArrayList<>(documentsArray.size());
        for (final BsonValue value : documentsArray) {
            if (!value.isDocument()) {
                return CommandErrors.typeMismatch("all entries in documents must be BSON documents");
            }
            final BsonDocument document = value.asDocument();
            final BsonDocument idFieldValidationError = validateIdField(document);
            if (idFieldValidationError != null) {
                return idFieldValidationError;
            }
            documents.add(document);
        }

        final int insertedCount;
        try {
            insertedCount = store.insert(database, collection, List.copyOf(documents));
        } catch (final DuplicateKeyException exception) {
            return CommandErrors.duplicateKey(exception.getMessage());
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

    private static BsonDocument validateIdField(final BsonDocument document) {
        final BsonValue idValue = document.get("_id");
        if (idValue == null || !idValue.isDocument()) {
            return null;
        }
        return findDollarPrefixedFieldInId(idValue.asDocument());
    }

    private static BsonDocument findDollarPrefixedFieldInId(final BsonDocument idDocument) {
        for (final Map.Entry<String, BsonValue> entry : idDocument.entrySet()) {
            final String fieldName = entry.getKey();
            if (fieldName.startsWith("$")) {
                return CommandErrors.dollarPrefixedFieldName(
                        "_id fields may not contain '$'-prefixed fields: "
                                + fieldName
                                + " is not valid for storage.");
            }
            final BsonValue value = entry.getValue();
            if (value.isDocument()) {
                final BsonDocument nestedError = findDollarPrefixedFieldInId(value.asDocument());
                if (nestedError != null) {
                    return nestedError;
                }
            }
        }
        return null;
    }
}
