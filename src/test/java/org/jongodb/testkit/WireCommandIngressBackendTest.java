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
}
