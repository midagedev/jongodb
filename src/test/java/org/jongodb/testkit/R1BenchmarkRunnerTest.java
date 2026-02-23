package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class R1BenchmarkRunnerTest {
    @Test
    void percentileUsesNearestRankRule() {
        double p95 = R1BenchmarkRunner.percentile(
            java.util.List.of(1.0d, 3.0d, 2.0d, 4.0d, 5.0d),
            0.95d
        );
        assertEquals(5.0d, p95);
    }

    @Test
    void throughputIsComputedFromOperationCountAndDuration() {
        double throughput = R1BenchmarkRunner.throughputOpsPerSecond(250, 500_000_000L);
        assertEquals(500.0d, throughput);
    }

    @Test
    void runProducesFiniteMetricsAndExpectedSampleSizes() {
        R1BenchmarkRunner runner = new R1BenchmarkRunner();
        R1BenchmarkRunner.BenchmarkConfig config = new R1BenchmarkRunner.BenchmarkConfig(2, 2, 5, 20);

        R1BenchmarkRunner.BenchmarkResult result = runner.run(config);

        assertEquals(2, result.coldStartSamplesMillis().size());
        assertEquals(2, result.resetSamplesMillis().size());
        assertEquals(20, result.crudLatencySamplesMillis().size());
        assertTrue(result.coldStartMillis() >= 0.0d);
        assertTrue(result.resetMillis() >= 0.0d);
        assertTrue(result.crudP95LatencyMillis() >= 0.0d);
        assertTrue(Double.isFinite(result.throughputOpsPerSecond()));
        assertTrue(result.throughputOpsPerSecond() > 0.0d);
    }
}
