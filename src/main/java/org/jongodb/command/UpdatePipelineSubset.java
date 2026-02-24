package org.jongodb.command;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

final class UpdatePipelineSubset {
    private static final String STAGE_SET = "$set";
    private static final String STAGE_UNSET = "$unset";

    private UpdatePipelineSubset() {}

    static ParseResult parse(final BsonArray pipeline) {
        if (pipeline == null || pipeline.isEmpty()) {
            return ParseResult.error(CommandErrors.badValue("update pipeline must not be empty"));
        }

        final Map<String, BsonValue> setOperations = new LinkedHashMap<>();
        final LinkedHashSet<String> unsetOperations = new LinkedHashSet<>();

        for (final BsonValue stageValue : pipeline) {
            if (!stageValue.isDocument()) {
                return ParseResult.error(CommandErrors.typeMismatch("update pipeline stages must be documents"));
            }

            final BsonDocument stage = stageValue.asDocument();
            if (stage.size() != 1) {
                return ParseResult.error(CommandErrors.badValue("update pipeline stage must contain exactly one operator"));
            }

            final String stageName = stage.getFirstKey();
            final BsonValue stageArgument = stage.get(stageName);
            if (STAGE_SET.equals(stageName)) {
                final BsonDocument error = applySetStage(stageArgument, setOperations, unsetOperations);
                if (error != null) {
                    return ParseResult.error(error);
                }
                continue;
            }
            if (STAGE_UNSET.equals(stageName)) {
                final BsonDocument error = applyUnsetStage(stageArgument, setOperations, unsetOperations);
                if (error != null) {
                    return ParseResult.error(error);
                }
                continue;
            }
            return ParseResult.error(CommandErrors.badValue("unsupported update pipeline stage: " + stageName));
        }

        final BsonDocument normalized = new BsonDocument();
        if (!setOperations.isEmpty()) {
            final BsonDocument setDocument = new BsonDocument();
            for (final Map.Entry<String, BsonValue> entry : setOperations.entrySet()) {
                setDocument.append(entry.getKey(), entry.getValue());
            }
            normalized.append(STAGE_SET, setDocument);
        }
        if (!unsetOperations.isEmpty()) {
            final BsonDocument unsetDocument = new BsonDocument();
            for (final String path : unsetOperations) {
                unsetDocument.append(path, new BsonInt32(1));
            }
            normalized.append(STAGE_UNSET, unsetDocument);
        }
        if (normalized.isEmpty()) {
            return ParseResult.error(CommandErrors.badValue("update pipeline must include at least one field operation"));
        }
        return ParseResult.success(normalized);
    }

    private static BsonDocument applySetStage(
            final BsonValue stageArgument,
            final Map<String, BsonValue> setOperations,
            final LinkedHashSet<String> unsetOperations) {
        if (stageArgument == null || !stageArgument.isDocument()) {
            return CommandErrors.typeMismatch("$set stage must be a document");
        }

        for (final Map.Entry<String, BsonValue> entry : stageArgument.asDocument().entrySet()) {
            final String path = entry.getKey();
            if (containsUnsupportedExpression(entry.getValue())) {
                return CommandErrors.badValue("update pipeline expressions are not supported for path '" + path + "'");
            }
            unsetOperations.remove(path);
            setOperations.put(path, entry.getValue());
        }
        return null;
    }

    private static BsonDocument applyUnsetStage(
            final BsonValue stageArgument,
            final Map<String, BsonValue> setOperations,
            final LinkedHashSet<String> unsetOperations) {
        if (stageArgument == null) {
            return CommandErrors.typeMismatch("$unset stage must be a string, array, or document");
        }

        if (stageArgument.isString()) {
            markUnset(stageArgument.asString().getValue(), setOperations, unsetOperations);
            return null;
        }
        if (stageArgument.isArray()) {
            for (final BsonValue value : stageArgument.asArray()) {
                if (!value.isString()) {
                    return CommandErrors.typeMismatch("$unset stage array entries must be strings");
                }
                markUnset(value.asString().getValue(), setOperations, unsetOperations);
            }
            return null;
        }
        if (!stageArgument.isDocument()) {
            return CommandErrors.typeMismatch("$unset stage must be a string, array, or document");
        }

        for (final String path : stageArgument.asDocument().keySet()) {
            markUnset(path, setOperations, unsetOperations);
        }
        return null;
    }

    private static void markUnset(
            final String path,
            final Map<String, BsonValue> setOperations,
            final LinkedHashSet<String> unsetOperations) {
        setOperations.remove(path);
        unsetOperations.add(path);
    }

    private static boolean containsUnsupportedExpression(final BsonValue value) {
        if (value == null) {
            return false;
        }
        if (value.isString()) {
            final String raw = value.asString().getValue();
            return raw != null && raw.startsWith("$");
        }
        if (value.isDocument()) {
            for (final Map.Entry<String, BsonValue> entry : value.asDocument().entrySet()) {
                if (entry.getKey().startsWith("$")) {
                    return true;
                }
                if (containsUnsupportedExpression(entry.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (value.isArray()) {
            for (final BsonValue item : value.asArray()) {
                if (containsUnsupportedExpression(item)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    static final class ParseResult {
        private final BsonDocument updateDocument;
        private final BsonDocument error;

        private ParseResult(final BsonDocument updateDocument, final BsonDocument error) {
            this.updateDocument = updateDocument;
            this.error = error;
        }

        static ParseResult success(final BsonDocument updateDocument) {
            return new ParseResult(updateDocument, null);
        }

        static ParseResult error(final BsonDocument error) {
            return new ParseResult(null, error);
        }

        BsonDocument updateDocument() {
            return updateDocument;
        }

        BsonDocument error() {
            return error;
        }
    }
}
