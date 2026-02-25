package org.jongodb.command;

import java.util.Locale;
import java.util.Objects;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Shared command-field canonicalization helpers used by runtime handlers and testkit adapters.
 */
public final class CommandCanonicalizer {
    private CommandCanonicalizer() {}

    public static String readDatabase(final BsonDocument command) {
        final BsonValue value = command.get("$db");
        if (value == null || !value.isString()) {
            return "test";
        }
        return value.asString().getValue();
    }

    public static String readCommandString(final BsonDocument command, final String... commandNames) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(commandNames, "commandNames");
        for (final String commandName : commandNames) {
            final BsonValue canonical = command.get(commandName);
            if (canonical != null) {
                if (canonical.isString()) {
                    return canonical.asString().getValue();
                }
                return null;
            }
        }

        for (final String key : command.keySet()) {
            for (final String commandName : commandNames) {
                if (!commandName.equalsIgnoreCase(key)) {
                    continue;
                }
                final BsonValue value = command.get(key);
                if (value != null && value.isString()) {
                    return value.asString().getValue();
                }
                return null;
            }
        }
        return null;
    }

    public static BsonDocument requireDocument(
            final BsonDocument command,
            final String key,
            final String message) {
        final BsonValue value = command.get(key);
        if (value == null || !value.isDocument()) {
            throw new ValidationException(ErrorKind.TYPE_MISMATCH, message);
        }
        return value.asDocument();
    }

    public static BsonDocument optionalDocument(
            final BsonDocument command,
            final String key,
            final String message) {
        final BsonValue value = command.get(key);
        if (value == null) {
            return null;
        }
        if (!value.isDocument()) {
            throw new ValidationException(ErrorKind.TYPE_MISMATCH, message);
        }
        return value.asDocument();
    }

    public static boolean optionalBoolean(
            final BsonDocument command,
            final String key,
            final boolean defaultValue,
            final String message) {
        final BsonValue value = command.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (!value.isBoolean()) {
            throw new ValidationException(ErrorKind.TYPE_MISMATCH, message);
        }
        return value.asBoolean().getValue();
    }

    public static boolean parseReturnDocumentAsAfter(final BsonDocument command) {
        final BsonValue returnDocumentValue = command.get("returnDocument");
        if (returnDocumentValue == null) {
            return optionalBoolean(command, "new", false, "new must be a boolean");
        }
        if (!returnDocumentValue.isString()) {
            throw new ValidationException(ErrorKind.TYPE_MISMATCH, "returnDocument must be a string");
        }

        final String value = returnDocumentValue.asString().getValue().toLowerCase(Locale.ROOT);
        if ("before".equals(value)) {
            return false;
        }
        if ("after".equals(value)) {
            return true;
        }
        throw new ValidationException(ErrorKind.BAD_VALUE, "returnDocument must be 'before' or 'after'");
    }

    public static boolean containsTopLevelOperator(final BsonDocument document) {
        for (final String key : document.keySet()) {
            if (key.startsWith("$")) {
                return true;
            }
        }
        return false;
    }

    public static void appendIfPresent(
            final BsonDocument source,
            final BsonDocument target,
            final String key) {
        if (!source.containsKey(key)) {
            return;
        }
        target.append(key, source.get(key));
    }

    public enum ErrorKind {
        TYPE_MISMATCH,
        BAD_VALUE
    }

    public static final class ValidationException extends IllegalArgumentException {
        private final ErrorKind kind;

        public ValidationException(final ErrorKind kind, final String message) {
            super(message);
            this.kind = Objects.requireNonNull(kind, "kind");
        }

        public ErrorKind kind() {
            return kind;
        }
    }
}

