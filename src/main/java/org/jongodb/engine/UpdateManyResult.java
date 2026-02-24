package org.jongodb.engine;

/**
 * Result of an update operation.
 */
public record UpdateManyResult(long matchedCount, long modifiedCount, boolean upserted, Object upsertedId) {
    public UpdateManyResult(final long matchedCount, final long modifiedCount) {
        this(matchedCount, modifiedCount, false, null);
    }

    public UpdateManyResult(final long matchedCount, final long modifiedCount, final Object upsertedId) {
        this(matchedCount, modifiedCount, true, upsertedId);
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
        if (upserted) {
            if (matchedCount != 0) {
                throw new IllegalArgumentException("upserted result requires matchedCount to be 0");
            }
            if (modifiedCount != 0) {
                throw new IllegalArgumentException("upserted result requires modifiedCount to be 0");
            }
        } else if (upsertedId != null) {
            throw new IllegalArgumentException("upsertedId requires upserted=true");
        }
    }
}
