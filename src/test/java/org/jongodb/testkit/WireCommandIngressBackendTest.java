package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WireCommandIngressBackendTest {
    @Test
    void executeUsesScenarioSpecificDatabaseNameInFindCursorNamespace() {
        WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        Scenario scenario = new Scenario(
            "crud.insert-find",
            "insert and find",
            List.of(
                new ScenarioCommand(
                    "insert",
                    Map.of(
                        "collection",
                        "users",
                        "documents",
                        List.of(Map.of("_id", 1, "name", "alpha"))
                    )
                ),
                new ScenarioCommand("find", Map.of("collection", "users", "filter", Map.of("_id", 1)))
            )
        );

        ScenarioOutcome outcome = backend.execute(scenario);

        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));
        @SuppressWarnings("unchecked")
        Map<String, Object> cursor = (Map<String, Object>) outcome.commandResults().get(1).get("cursor");
        assertEquals(
            RealMongodBackend.scenarioDatabaseName("testkit", "crud.insert-find") + ".users",
            cursor.get("ns")
        );
    }

    @Test
    void executeSupportsDistinctCommandScenarios() {
        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final Scenario scenario = new Scenario(
                "crud.distinct",
                "insert and distinct",
                List.of(
                        new ScenarioCommand(
                                "insert",
                                Map.of(
                                        "collection",
                                        "users",
                                        "documents",
                                        List.of(
                                                Map.of("_id", 1, "tag", "a"),
                                                Map.of("_id", 2, "tag", "b"),
                                                Map.of("_id", 3, "tag", "a")))),
                        new ScenarioCommand(
                                "distinct",
                                Map.of("collection", "users", "key", "tag", "query", Map.of()))));

        final ScenarioOutcome outcome = backend.execute(scenario);

        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));
        @SuppressWarnings("unchecked")
        final Map<String, Object> distinctResult = outcome.commandResults().get(1);
        @SuppressWarnings("unchecked")
        final List<Object> values = (List<Object>) distinctResult.get("values");
        assertEquals(2, values.size());
        assertEquals("a", values.get(0));
        assertEquals("b", values.get(1));
    }
}
