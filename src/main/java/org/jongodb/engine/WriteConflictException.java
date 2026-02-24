package org.jongodb.engine;

/**
 * Raised when publishing a transaction snapshot detects write-write conflicts.
 */
public final class WriteConflictException extends RuntimeException {
    public WriteConflictException(final String message) {
        super(message);
    }
}
