package org.jongodb.command;

import org.bson.BsonDocument;
import org.jongodb.engine.UnsupportedFeatureException;

/**
 * Maps engine-level argument exceptions to deterministic command error envelopes.
 */
final class CommandExceptionMapper {
    private CommandExceptionMapper() {}

    static BsonDocument fromIllegalArgument(final IllegalArgumentException exception) {
        if (exception instanceof UnsupportedFeatureException unsupported) {
            return CommandErrors.notImplemented(
                    unsupported.getMessage() + " [feature=" + unsupported.featureKey() + "]");
        }
        final String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return CommandErrors.badValue("invalid argument");
        }
        return CommandErrors.badValue(message);
    }
}
