package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class R1FlakeRateEvaluatorTest {
    @Test
    void evaluateCountsFingerprintDiffsAndMissingScenarioIdsAsFlaky() {
        DifferentialReport baseline = new DifferentialReport(
            Instant.parse("2026-02-23T10:00:00Z"),
            "wire-left",
            "wire-right",
            List.of(
                DiffResult.match("scenario-1", "wire-left", "wire-right"),
                DiffResult.match("scenario-2", "wire-left", "wire-right")
            )
        );

        DifferentialReport rerunStable = new DifferentialReport(
            Instant.parse("2026-02-23T10:01:00Z"),
            "wire-left",
            "wire-right",
            List.of(
                DiffResult.match("scenario-1", "wire-left", "wire-right"),
                DiffResult.match("scenario-2", "wire-left", "wire-right")
            )
        );

        DifferentialReport rerunFlaky = new DifferentialReport(
            Instant.parse("2026-02-23T10:02:00Z"),
            "wire-left",
            "wire-right",
            List.of(
                DiffResult.mismatch(
                    "scenario-1",
                    "wire-left",
                    "wire-right",
                    List.of(new DiffEntry("$.ok", 1, 0, "value mismatch"))
                ),
                DiffResult.match("scenario-new", "wire-left", "wire-right")
            )
        );

        R1FlakeRateEvaluator evaluator = new R1FlakeRateEvaluator();
        R1FlakeRateEvaluator.FlakeSummary summary = evaluator.evaluate(
            baseline,
            List.of(rerunStable, rerunFlaky)
        );

        assertEquals(2, summary.runs());
        assertEquals(4, summary.observations());
        assertEquals(2, summary.flakyObservations());
        assertEquals(0.5d, summary.rate());
    }
}
