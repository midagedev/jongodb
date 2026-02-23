package org.jongodb.engine;

/**
 * Thrown when a write violates a unique index.
 */
public final class DuplicateKeyException extends RuntimeException {
    public DuplicateKeyException(final String message) {
        super(message);
    }
}
