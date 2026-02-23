package org.jongodb.testkit;

/**
 * Threshold configuration for quality gates.
 */
public final class QualityGateThresholds {
    private final double minCompatibilityPassRate;
    private final double maxFlakeRate;
    private final double maxP95LatencyMillis;
    private final double maxReproTimeP50Minutes;

    public QualityGateThresholds(
        double minCompatibilityPassRate,
        double maxFlakeRate,
        double maxP95LatencyMillis,
        double maxReproTimeP50Minutes
    ) {
        this.minCompatibilityPassRate = requireRatio(minCompatibilityPassRate, "minCompatibilityPassRate");
        this.maxFlakeRate = requireRatio(maxFlakeRate, "maxFlakeRate");
        this.maxP95LatencyMillis = requireNonNegative(maxP95LatencyMillis, "maxP95LatencyMillis");
        this.maxReproTimeP50Minutes = requireNonNegative(maxReproTimeP50Minutes, "maxReproTimeP50Minutes");
    }

    /**
     * Recommended M3 hardening thresholds from success criteria.
     */
    public static QualityGateThresholds recommendedM3() {
        return new QualityGateThresholds(0.95d, 0.005d, 5.0d, 5.0d);
    }

    public double minCompatibilityPassRate() {
        return minCompatibilityPassRate;
    }

    public double maxFlakeRate() {
        return maxFlakeRate;
    }

    public double maxP95LatencyMillis() {
        return maxP95LatencyMillis;
    }

    public double maxReproTimeP50Minutes() {
        return maxReproTimeP50Minutes;
    }

    public double thresholdFor(QualityGateMetric metric) {
        return switch (metric) {
            case COMPATIBILITY_PASS_RATE -> minCompatibilityPassRate;
            case FLAKE_RATE -> maxFlakeRate;
            case P95_LATENCY_MILLIS -> maxP95LatencyMillis;
            case REPRO_TIME_P50_MINUTES -> maxReproTimeP50Minutes;
        };
    }

    private static double requireRatio(double value, String fieldName) {
        requireFinite(value, fieldName);
        if (value < 0.0d || value > 1.0d) {
            throw new IllegalArgumentException(fieldName + " must be in range [0.0, 1.0]");
        }
        return value;
    }

    private static double requireNonNegative(double value, String fieldName) {
        requireFinite(value, fieldName);
        if (value < 0.0d) {
            throw new IllegalArgumentException(fieldName + " must be >= 0.0");
        }
        return value;
    }

    private static void requireFinite(double value, String fieldName) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(fieldName + " must be finite");
        }
    }
}
