package org.jongodb.engine;

/**
 * Result of an updateMany operation.
 */
public record UpdateManyResult(long matchedCount, long modifiedCount) {
    public UpdateManyResult {
        if (matchedCount < 0) {
            throw new IllegalArgumentException("matchedCount must be >= 0");
        }
        if (modifiedCount < 0) {
            throw new IllegalArgumentException("modifiedCount must be >= 0");
        }
        if (modifiedCount > matchedCount) {
            throw new IllegalArgumentException("modifiedCount must be <= matchedCount");
        }
    }
}
