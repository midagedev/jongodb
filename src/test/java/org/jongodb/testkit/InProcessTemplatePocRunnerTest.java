package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class InProcessTemplatePocRunnerTest {
    @Test
    void decisionIsGoWhenPerformanceAndTraceGatesPass() {
        final InProcessTemplatePocRunner.BenchmarkSummary benchmark = new InProcessTemplatePocRunner.BenchmarkSummary(
                List.of(1.0d),
                List.of(2.0d),
                List.of(0.8d),
                List.of(1.2d),
                1.0d,
                2.0d,
                0.8d,
                1.2d,
                2000.0d,
                1000.0d,
                10,
                20);
        final InProcessTemplatePocRunner.TraceAnalysisSummary trace = new InProcessTemplatePocRunner.TraceAnalysisSummary(
                2,
                0,
                "command_failure",
                2,
                true);

        final InProcessTemplatePocRunner.Decision decision =
                InProcessTemplatePocRunner.evaluateDecision(benchmark, trace, 0.10d, 0.10d);

        assertEquals(InProcessTemplatePocRunner.DecisionStatus.GO, decision.status());
    }

    @Test
    void decisionIsNoGoWhenTraceGateFails() {
        final InProcessTemplatePocRunner.BenchmarkSummary benchmark = new InProcessTemplatePocRunner.BenchmarkSummary(
                List.of(1.0d),
                List.of(2.0d),
                List.of(0.8d),
                List.of(1.2d),
                1.0d,
                2.0d,
                0.8d,
                1.2d,
                2000.0d,
                1000.0d,
                10,
                20);
        final InProcessTemplatePocRunner.TraceAnalysisSummary trace = new InProcessTemplatePocRunner.TraceAnalysisSummary(
                1,
                1,
                "",
                0,
                false);

        final InProcessTemplatePocRunner.Decision decision =
                InProcessTemplatePocRunner.evaluateDecision(benchmark, trace, 0.10d, 0.10d);

        assertEquals(InProcessTemplatePocRunner.DecisionStatus.NO_GO, decision.status());
    }
}
