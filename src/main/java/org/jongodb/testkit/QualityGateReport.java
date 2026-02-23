package org.jongodb.testkit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Aggregate output for a quality gate evaluation run.
 */
public final class QualityGateReport {
    private final Instant generatedAt;
    private final QualityGateMetrics metrics;
    private final List<QualityGateResult> gateResults;

    public QualityGateReport(Instant generatedAt, QualityGateMetrics metrics, List<QualityGateResult> gateResults) {
        this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.gateResults = copyGateResults(gateResults);
    }

    public Instant generatedAt() {
        return generatedAt;
    }

    public QualityGateMetrics metrics() {
        return metrics;
    }

    public List<QualityGateResult> gateResults() {
        return gateResults;
    }

    public boolean overallPassed() {
        return failCount() == 0;
    }

    public int passCount() {
        int count = 0;
        for (QualityGateResult gateResult : gateResults) {
            if (gateResult.passed()) {
                count++;
            }
        }
        return count;
    }

    public int failCount() {
        return gateResults.size() - passCount();
    }

    private static List<QualityGateResult> copyGateResults(List<QualityGateResult> source) {
        Objects.requireNonNull(source, "gateResults");
        return List.copyOf(new ArrayList<>(source));
    }
}
