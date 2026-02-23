package org.jongodb.testkit;

/**
 * Adapter interface for any backend (mongod/jongodb/mock) used in differential tests.
 */
public interface DifferentialBackend {
    String name();

    ScenarioOutcome execute(Scenario scenario);
}
