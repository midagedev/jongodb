package org.jongodb.testkit;

import java.util.Objects;

/**
 * Evaluation output for a single gate.
 */
public final class QualityGateResult {
    private final String gateId;
    private final QualityGateMetric metric;
    private final QualityGateOperator operator;
    private final double measuredValue;
    private final double thresholdValue;
    private final QualityGateStatus status;

    QualityGateResult(
        String gateId,
        QualityGateMetric metric,
        QualityGateOperator operator,
        double measuredValue,
        double thresholdValue,
        QualityGateStatus status
    ) {
        this.gateId = requireText(gateId, "gateId");
        this.metric = Objects.requireNonNull(metric, "metric");
        this.operator = Objects.requireNonNull(operator, "operator");
        this.measuredValue = requireFinite(measuredValue, "measuredValue");
        this.thresholdValue = requireFinite(thresholdValue, "thresholdValue");
        this.status = Objects.requireNonNull(status, "status");
    }

    public String gateId() {
        return gateId;
    }

    public QualityGateMetric metric() {
        return metric;
    }

    public QualityGateOperator operator() {
        return operator;
    }

    public double measuredValue() {
        return measuredValue;
    }

    public double thresholdValue() {
        return thresholdValue;
    }

    public QualityGateStatus status() {
        return status;
    }

    public boolean passed() {
        return status == QualityGateStatus.PASS;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    private static double requireFinite(double value, String fieldName) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(fieldName + " must be finite");
        }
        return value;
    }
}
