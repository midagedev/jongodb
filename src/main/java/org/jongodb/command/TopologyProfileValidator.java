package org.jongodb.command;

import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

final class TopologyProfileValidator {
    private static final Set<String> SUPPORTED_READ_CONCERN_LEVELS = Set.of("local", "majority", "snapshot");

    private TopologyProfileValidator() {}

    static BsonDocument validate(
            final TopologyProfile topologyProfile,
            final String commandName,
            final BsonDocument command) {
        if (topologyProfile == null || !topologyProfile.replicaSetSemanticsEnabled()) {
            return null;
        }
        final BsonDocument readPreferenceError = validatePrimaryReadPreference(command);
        if (readPreferenceError != null) {
            return readPreferenceError;
        }

        final BsonDocument writeConcernError = validateWriteConcern(command);
        if (writeConcernError != null) {
            return writeConcernError;
        }

        return validateReadConcern(commandName, command);
    }

    private static BsonDocument validatePrimaryReadPreference(final BsonDocument command) {
        final BsonValue readPreference = command.get("readPreference");
        final BsonValue dollarReadPreference = command.get("$readPreference");
        if (readPreference != null && dollarReadPreference != null) {
            return CommandErrors.badValue("readPreference and $readPreference cannot both be specified");
        }
        final BsonValue effective = readPreference != null ? readPreference : dollarReadPreference;
        if (effective == null) {
            return null;
        }
        if (!effective.isDocument()) {
            return CommandErrors.typeMismatch("readPreference must be a document");
        }
        final BsonValue mode = effective.asDocument().get("mode");
        if (!(mode instanceof BsonString modeString)) {
            return CommandErrors.typeMismatch("readPreference.mode must be a string");
        }
        if (!"primary".equals(modeString.getValue())) {
            return CommandErrors.badValue("single-node replica-set profile supports readPreference mode 'primary' only");
        }
        return null;
    }

    private static BsonDocument validateWriteConcern(final BsonDocument command) {
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
            if (wValue.isNumber()) {
                if (wValue.asNumber().intValue() != 1) {
                    return CommandErrors.badValue(
                            "single-node replica-set profile supports writeConcern w=1 or w='majority' only");
                }
            } else if (wValue.isString()) {
                final String mode = wValue.asString().getValue();
                if (!"majority".equals(mode) && !"1".equals(mode)) {
                    return CommandErrors.badValue(
                            "single-node replica-set profile supports writeConcern w=1 or w='majority' only");
                }
            } else {
                return CommandErrors.typeMismatch("writeConcern.w must be a string or integer");
            }
        }

        final BsonValue wtimeoutValue = writeConcern.get("wtimeout");
        if (wtimeoutValue != null) {
            if (!wtimeoutValue.isNumber()) {
                return CommandErrors.typeMismatch("writeConcern.wtimeout must be an integer");
            }
            if (wtimeoutValue.asNumber().intValue() < 0) {
                return CommandErrors.badValue("writeConcern.wtimeout must be non-negative");
            }
        }
        return null;
    }

    private static BsonDocument validateReadConcern(final String commandName, final BsonDocument command) {
        final BsonValue readConcernValue = command.get("readConcern");
        if (readConcernValue == null) {
            return null;
        }
        if (!readConcernValue.isDocument()) {
            return CommandErrors.typeMismatch("readConcern must be a document");
        }
        final BsonValue levelValue = readConcernValue.asDocument().get("level");
        if (levelValue == null) {
            return null;
        }
        if (!levelValue.isString()) {
            return CommandErrors.typeMismatch("readConcern.level must be a string");
        }
        final String level = levelValue.asString().getValue();
        if (!SUPPORTED_READ_CONCERN_LEVELS.contains(level)) {
            return CommandErrors.badValue(
                    "single-node replica-set profile does not support readConcern.level '" + level + "' for "
                            + commandName);
        }
        return null;
    }
}
