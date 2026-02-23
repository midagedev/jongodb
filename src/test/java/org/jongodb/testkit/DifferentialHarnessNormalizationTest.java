package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DifferentialHarnessNormalizationTest {
    @Test
    void ignoresEphemeralMetadataFieldsInSuccessComparison() {
        Scenario scenario = new Scenario("s1", "metadata normalization", List.of(new ScenarioCommand("find", Map.of())));
        DifferentialBackend left = new StaticBackend(
            "left",
            ScenarioOutcome.success(List.of(Map.of("ok", 1, "cursor", Map.of("id", 0))))
        );
        DifferentialBackend right = new StaticBackend(
            "right",
            ScenarioOutcome.success(
                List.of(
                    Map.of(
                        "ok",
                        1,
                        "cursor",
                        Map.of("id", 0),
                        "$clusterTime",
                        Map.of("clusterTime", "ts"),
                        "electionId",
                        "abc123",
                        "opTime",
                        Map.of("t", 1, "ts", "op-ts"),
                        "operationTime",
                        "op-ts"
                    )
                )
            )
        );

        DifferentialReport report = new DifferentialHarness(left, right).run(List.of(scenario));

        assertEquals(0, report.mismatchCount());
    }

    @Test
    void treatsFailureMessagesWithSameErrorCodeAsEquivalent() {
        Scenario scenario = new Scenario("s2", "error code normalization", List.of(new ScenarioCommand("insert", Map.of())));
        DifferentialBackend left = new StaticBackend(
            "left",
            ScenarioOutcome.failure(
                "command 'insert' failed at index 0: duplicate key (code=11000, codeName=DuplicateKey)"
            )
        );
        DifferentialBackend right = new StaticBackend(
            "right",
            ScenarioOutcome.failure(
                "command 'insert' failed at index 0: duplicate key collection detail differs (code=11000)"
            )
        );

        DifferentialReport report = new DifferentialHarness(left, right).run(List.of(scenario));

        assertEquals(0, report.mismatchCount());
    }

    private static final class StaticBackend implements DifferentialBackend {
        private final String name;
        private final ScenarioOutcome outcome;

        private StaticBackend(String name, ScenarioOutcome outcome) {
            this.name = name;
            this.outcome = outcome;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ScenarioOutcome execute(Scenario scenario) {
            return outcome;
        }
    }
}
