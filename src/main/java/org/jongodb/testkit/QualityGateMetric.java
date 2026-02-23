package org.jongodb.testkit;

/**
 * Supported KPI metrics for quality gate evaluation.
 */
public enum QualityGateMetric {
    COMPATIBILITY_PASS_RATE("compatibilityPassRate"),
    FLAKE_RATE("flakeRate"),
    P95_LATENCY_MILLIS("p95LatencyMillis"),
    REPRO_TIME_P50_MINUTES("reproTimeP50Minutes");

    private final String key;

    QualityGateMetric(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
