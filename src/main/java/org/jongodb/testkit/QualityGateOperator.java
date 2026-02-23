package org.jongodb.testkit;

/**
 * Numeric comparison operator for gate checks.
 */
public enum QualityGateOperator {
    GREATER_OR_EQUAL(">=") {
        @Override
        boolean test(double measuredValue, double thresholdValue) {
            return measuredValue >= thresholdValue;
        }
    },
    LESS_OR_EQUAL("<=") {
        @Override
        boolean test(double measuredValue, double thresholdValue) {
            return measuredValue <= thresholdValue;
        }
    };

    private final String symbol;

    QualityGateOperator(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    abstract boolean test(double measuredValue, double thresholdValue);
}
