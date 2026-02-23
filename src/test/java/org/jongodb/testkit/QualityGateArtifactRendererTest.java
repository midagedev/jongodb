package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class QualityGateArtifactRendererTest {
    @Test
    void rendersDeterministicMarkdownAndJsonArtifacts() {
        QualityGateEvaluator evaluator = new QualityGateEvaluator(
            Clock.fixed(Instant.parse("2026-02-23T11:10:00Z"), ZoneOffset.UTC)
        );
        QualityGateMetrics metrics = new QualityGateMetrics(0.94d, 0.003d, 4.5d, 4.0d);
        QualityGateThresholds thresholds = QualityGateThresholds.recommendedM3();
        QualityGateReport report = evaluator.evaluate(metrics, thresholds);

        QualityGateArtifactRenderer renderer = new QualityGateArtifactRenderer();
        String markdown = renderer.toMarkdown(report);
        String json = renderer.toJson(report);

        String expectedMarkdown = """
            # Quality Gate Report

            - generatedAt: 2026-02-23T11:10:00Z
            - overall: FAIL
            - pass: 3
            - fail: 1

            ## Metrics
            - compatibilityPassRate: 94.00%
            - flakeRate: 0.30%
            - p95LatencyMillis: 4.50ms
            - reproTimeP50Minutes: 4.00min

            ## Gates
            - compatibility-pass-rate: FAIL (compatibilityPassRate 94.00% >= 95.00%)
            - flake-rate: PASS (flakeRate 0.30% <= 0.50%)
            - p95-latency: PASS (p95LatencyMillis 4.50ms <= 5.00ms)
            - repro-time-p50: PASS (reproTimeP50Minutes 4.00min <= 5.00min)
            """;

        String expectedJson =
            "{\"generatedAt\":\"2026-02-23T11:10:00Z\",\"overallStatus\":\"FAIL\",\"summary\":{\"pass\":3,"
                + "\"fail\":1},\"metrics\":{\"compatibilityPassRate\":0.94,\"flakeRate\":0.003,"
                + "\"p95LatencyMillis\":4.5,\"reproTimeP50Minutes\":4.0},\"gates\":[{\"gateId\":"
                + "\"compatibility-pass-rate\",\"metric\":\"COMPATIBILITY_PASS_RATE\","
                + "\"metricKey\":\"compatibilityPassRate\",\"measuredValue\":0.94,"
                + "\"operator\":\">=\",\"thresholdValue\":0.95,\"status\":\"FAIL\"},{\"gateId\":"
                + "\"flake-rate\",\"metric\":\"FLAKE_RATE\",\"metricKey\":\"flakeRate\","
                + "\"measuredValue\":0.003,\"operator\":\"<=\",\"thresholdValue\":0.005,"
                + "\"status\":\"PASS\"},{\"gateId\":\"p95-latency\",\"metric\":\"P95_LATENCY_MILLIS\","
                + "\"metricKey\":\"p95LatencyMillis\",\"measuredValue\":4.5,\"operator\":\"<=\","
                + "\"thresholdValue\":5.0,\"status\":\"PASS\"},{\"gateId\":\"repro-time-p50\","
                + "\"metric\":\"REPRO_TIME_P50_MINUTES\",\"metricKey\":\"reproTimeP50Minutes\","
                + "\"measuredValue\":4.0,\"operator\":\"<=\",\"thresholdValue\":5.0,"
                + "\"status\":\"PASS\"}]}";

        assertEquals(expectedMarkdown, markdown);
        assertEquals(expectedJson, json);
    }
}
