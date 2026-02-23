package org.jongodb.engine;

/**
 * Result of an update operation.
 */
public record UpdateManyResult(long matchedCount, long modifiedCount, Object upsertedId) {
    public UpdateManyResult(final long matchedCount, final long modifiedCount) {
        this(matchedCount, modifiedCount, null);
    }

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
        if (upsertedId != null && matchedCount != 0) {
            throw new IllegalArgumentException("upsertedId requires matchedCount to be 0");
        }
        if (upsertedId != null && modifiedCount != 0) {
            throw new IllegalArgumentException("upsertedId requires modifiedCount to be 0");
        }
    }
}
