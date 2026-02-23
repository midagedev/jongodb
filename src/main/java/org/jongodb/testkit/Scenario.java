package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Differential test scenario with deterministic command sequence.
 */
public final class Scenario {
    private final String id;
    private final String description;
    private final List<ScenarioCommand> commands;

    public Scenario(String id, String description, List<ScenarioCommand> commands) {
        this.id = requireText(id, "id");
        this.description = description == null ? "" : description.trim();
        this.commands = copyList(commands);
    }

    public String id() {
        return id;
    }

    public String description() {
        return description;
    }

    public List<ScenarioCommand> commands() {
        return commands;
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static List<ScenarioCommand> copyList(List<ScenarioCommand> source) {
        Objects.requireNonNull(source, "commands");
        if (source.isEmpty()) {
            throw new IllegalArgumentException("commands must not be empty");
        }
        return List.copyOf(new ArrayList<>(source));
    }
}
