package org.jongodb.engine;

/**
 * Result of a deleteMany operation.
 */
public record DeleteManyResult(long matchedCount, long deletedCount) {
    public DeleteManyResult {
        if (matchedCount < 0) {
            throw new IllegalArgumentException("matchedCount must be >= 0");
        }
        if (deletedCount < 0) {
            throw new IllegalArgumentException("deletedCount must be >= 0");
        }
        if (deletedCount > matchedCount) {
            throw new IllegalArgumentException("deletedCount must be <= matchedCount");
        }
    }
}
