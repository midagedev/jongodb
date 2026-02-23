package org.jongodb.engine;

/**
 * Signals deterministic fail-fast for features that are intentionally unsupported.
 */
public final class UnsupportedFeatureException extends IllegalArgumentException {
    private final String featureKey;

    public UnsupportedFeatureException(final String featureKey, final String message) {
        super(requireText(message, "message"));
        this.featureKey = requireText(featureKey, "featureKey");
    }

    public String featureKey() {
        return featureKey;
    }

    private static String requireText(final String value, final String fieldName) {
        final String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
