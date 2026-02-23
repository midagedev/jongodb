package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QualityGateEvaluatorTest {
    @Test
    void evaluatesAllGatesAsPassAtThresholdBoundaries() {
        QualityGateEvaluator evaluator = new QualityGateEvaluator(
            Clock.fixed(Instant.parse("2026-02-23T11:00:00Z"), ZoneOffset.UTC)
        );
        QualityGateMetrics metrics = new QualityGateMetrics(0.95d, 0.005d, 5.0d, 5.0d);
        QualityGateThresholds thresholds = new QualityGateThresholds(0.95d, 0.005d, 5.0d, 5.0d);

        QualityGateReport report = evaluator.evaluate(metrics, thresholds);

        assertTrue(report.overallPassed());
        assertEquals(4, report.passCount());
        assertEquals(0, report.failCount());
        assertEquals(Instant.parse("2026-02-23T11:00:00Z"), report.generatedAt());

        Map<String, QualityGateStatus> gateStatuses = statusesByGate(report);
        assertEquals(QualityGateStatus.PASS, gateStatuses.get("compatibility-pass-rate"));
        assertEquals(QualityGateStatus.PASS, gateStatuses.get("flake-rate"));
        assertEquals(QualityGateStatus.PASS, gateStatuses.get("p95-latency"));
        assertEquals(QualityGateStatus.PASS, gateStatuses.get("repro-time-p50"));
    }

    @Test
    void evaluatesMixedPassAndFailOutcomesPerGate() {
        QualityGateEvaluator evaluator = new QualityGateEvaluator(
            Clock.fixed(Instant.parse("2026-02-23T11:05:00Z"), ZoneOffset.UTC)
        );
        QualityGateMetrics metrics = new QualityGateMetrics(0.90d, 0.006d, 6.2d, 4.8d);
        QualityGateThresholds thresholds = QualityGateThresholds.recommendedM3();

        QualityGateReport report = evaluator.evaluate(metrics, thresholds);

        assertFalse(report.overallPassed());
        assertEquals(1, report.passCount());
        assertEquals(3, report.failCount());
        assertEquals(4, report.gateResults().size());

        Map<String, QualityGateStatus> gateStatuses = statusesByGate(report);
        assertEquals(QualityGateStatus.FAIL, gateStatuses.get("compatibility-pass-rate"));
        assertEquals(QualityGateStatus.FAIL, gateStatuses.get("flake-rate"));
        assertEquals(QualityGateStatus.FAIL, gateStatuses.get("p95-latency"));
        assertEquals(QualityGateStatus.PASS, gateStatuses.get("repro-time-p50"));
    }

    private static Map<String, QualityGateStatus> statusesByGate(QualityGateReport report) {
        Map<String, QualityGateStatus> statuses = new LinkedHashMap<>();
        for (QualityGateResult gateResult : report.gateResults()) {
            statuses.put(gateResult.gateId(), gateResult.status());
        }
        return statuses;
    }
}
