package org.jongodb.testkit;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Evaluates KPI measurements against threshold configuration.
 */
public final class QualityGateEvaluator {
    private static final String COMPATIBILITY_GATE = "compatibility-pass-rate";
    private static final String FLAKE_GATE = "flake-rate";
    private static final String P95_LATENCY_GATE = "p95-latency";
    private static final String REPRO_TIME_GATE = "repro-time-p50";

    private final Clock clock;

    public QualityGateEvaluator() {
        this(Clock.systemUTC());
    }

    public QualityGateEvaluator(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public QualityGateReport evaluate(QualityGateMetrics metrics, QualityGateThresholds thresholds) {
        Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(thresholds, "thresholds");
        Instant generatedAt = Instant.now(clock);
        List<QualityGateResult> results = List.of(
            evaluateGate(
                COMPATIBILITY_GATE,
                QualityGateMetric.COMPATIBILITY_PASS_RATE,
                QualityGateOperator.GREATER_OR_EQUAL,
                metrics,
                thresholds
            ),
            evaluateGate(
                FLAKE_GATE,
                QualityGateMetric.FLAKE_RATE,
                QualityGateOperator.LESS_OR_EQUAL,
                metrics,
                thresholds
            ),
            evaluateGate(
                P95_LATENCY_GATE,
                QualityGateMetric.P95_LATENCY_MILLIS,
                QualityGateOperator.LESS_OR_EQUAL,
                metrics,
                thresholds
            ),
            evaluateGate(
                REPRO_TIME_GATE,
                QualityGateMetric.REPRO_TIME_P50_MINUTES,
                QualityGateOperator.LESS_OR_EQUAL,
                metrics,
                thresholds
            )
        );
        return new QualityGateReport(generatedAt, metrics, results);
    }

    private static QualityGateResult evaluateGate(
        String gateId,
        QualityGateMetric metric,
        QualityGateOperator operator,
        QualityGateMetrics metrics,
        QualityGateThresholds thresholds
    ) {
        double measuredValue = metrics.valueFor(metric);
        double thresholdValue = thresholds.thresholdFor(metric);
        QualityGateStatus status = operator.test(measuredValue, thresholdValue)
            ? QualityGateStatus.PASS
            : QualityGateStatus.FAIL;
        return new QualityGateResult(gateId, metric, operator, measuredValue, thresholdValue, status);
    }
}
