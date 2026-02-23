package org.jongodb.command;

import org.bson.BsonDocument;
import org.bson.BsonValue;

final class CrudCommandOptionValidator {
    private CrudCommandOptionValidator() {}

    static BsonDocument validateOrdered(final BsonDocument command) {
        final BsonValue orderedValue = command.get("ordered");
        if (orderedValue != null && !orderedValue.isBoolean()) {
            return CommandErrors.typeMismatch("ordered must be a boolean");
        }
        return null;
    }

    static BsonDocument validateWriteConcern(final BsonDocument command) {
        final BsonValue writeConcernValue = command.get("writeConcern");
        if (writeConcernValue == null) {
            return null;
        }
        if (!writeConcernValue.isDocument()) {
            return CommandErrors.typeMismatch("writeConcern must be a document");
        }

        final BsonDocument writeConcern = writeConcernValue.asDocument();

        final BsonValue wValue = writeConcern.get("w");
        if (wValue != null) {
            if (wValue.isString()) {
                // Accepted as-is (e.g. "majority").
            } else {
                final Long parsedW = readIntegralLong(wValue);
                if (parsedW == null) {
                    return CommandErrors.typeMismatch("writeConcern.w must be a string or integer");
                }
                if (parsedW < 0) {
                    return CommandErrors.badValue("writeConcern.w must be non-negative when numeric");
                }
            }
        }

        final BsonValue jValue = writeConcern.get("j");
        if (jValue != null && !jValue.isBoolean()) {
            return CommandErrors.typeMismatch("writeConcern.j must be a boolean");
        }

        final BsonValue wtimeoutValue = writeConcern.get("wtimeout");
        if (wtimeoutValue != null) {
            final Long parsedWtimeout = readIntegralLong(wtimeoutValue);
            if (parsedWtimeout == null) {
                return CommandErrors.typeMismatch("writeConcern.wtimeout must be an integer");
            }
            if (parsedWtimeout < 0) {
                return CommandErrors.badValue("writeConcern.wtimeout must be non-negative");
            }
        }

        return null;
    }

    static BsonDocument validateReadConcern(final BsonDocument command) {
        final BsonValue readConcernValue = command.get("readConcern");
        if (readConcernValue == null) {
            return null;
        }
        if (!readConcernValue.isDocument()) {
            return CommandErrors.typeMismatch("readConcern must be a document");
        }

        final BsonValue levelValue = readConcernValue.asDocument().get("level");
        if (levelValue != null && !levelValue.isString()) {
            return CommandErrors.typeMismatch("readConcern.level must be a string");
        }
        return null;
    }

    static BsonDocument validateHint(final BsonDocument command, final String fieldName) {
        final BsonValue hintValue = command.get(fieldName);
        if (hintValue == null) {
            return null;
        }
        if (hintValue.isString()) {
            return null;
        }
        if (!hintValue.isDocument()) {
            return CommandErrors.typeMismatch(fieldName + " must be a string or document");
        }
        if (hintValue.asDocument().isEmpty()) {
            return CommandErrors.badValue(fieldName + " document must not be empty");
        }
        return null;
    }

    static BsonDocument validateCollation(final BsonDocument command, final String fieldName) {
        final BsonValue collationValue = command.get(fieldName);
        if (collationValue == null) {
            return null;
        }
        if (!collationValue.isDocument()) {
            return CommandErrors.typeMismatch(fieldName + " must be a document");
        }

        final BsonValue localeValue = collationValue.asDocument().get("locale");
        if (localeValue != null && !localeValue.isString()) {
            return CommandErrors.typeMismatch(fieldName + ".locale must be a string");
        }
        return null;
    }

    private static Long readIntegralLong(final BsonValue value) {
        if (value.isInt32()) {
            return (long) value.asInt32().getValue();
        }
        if (value.isInt64()) {
            return value.asInt64().getValue();
        }
        if (value.isDouble()) {
            final double doubleValue = value.asDouble().getValue();
            if (!Double.isFinite(doubleValue) || Math.rint(doubleValue) != doubleValue) {
                return null;
            }
            if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                return null;
            }
            return (long) doubleValue;
        }
        return null;
    }
}
