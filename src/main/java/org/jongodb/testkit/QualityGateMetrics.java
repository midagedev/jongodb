package org.jongodb.testkit;

/**
 * Measured KPI values used for quality gate evaluation.
 */
public final class QualityGateMetrics {
    private final double compatibilityPassRate;
    private final double flakeRate;
    private final double p95LatencyMillis;
    private final double reproTimeP50Minutes;

    public QualityGateMetrics(
        double compatibilityPassRate,
        double flakeRate,
        double p95LatencyMillis,
        double reproTimeP50Minutes
    ) {
        this.compatibilityPassRate = requireRatio(compatibilityPassRate, "compatibilityPassRate");
        this.flakeRate = requireRatio(flakeRate, "flakeRate");
        this.p95LatencyMillis = requireNonNegative(p95LatencyMillis, "p95LatencyMillis");
        this.reproTimeP50Minutes = requireNonNegative(reproTimeP50Minutes, "reproTimeP50Minutes");
    }

    public double compatibilityPassRate() {
        return compatibilityPassRate;
    }

    public double flakeRate() {
        return flakeRate;
    }

    public double p95LatencyMillis() {
        return p95LatencyMillis;
    }

    public double reproTimeP50Minutes() {
        return reproTimeP50Minutes;
    }

    public double valueFor(QualityGateMetric metric) {
        return switch (metric) {
            case COMPATIBILITY_PASS_RATE -> compatibilityPassRate;
            case FLAKE_RATE -> flakeRate;
            case P95_LATENCY_MILLIS -> p95LatencyMillis;
            case REPRO_TIME_P50_MINUTES -> reproTimeP50Minutes;
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
