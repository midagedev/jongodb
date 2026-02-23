package org.jongodb.testkit;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One logical command invocation within a scenario.
 */
public final class ScenarioCommand {
    private final String commandName;
    private final Map<String, Object> payload;

    public ScenarioCommand(String commandName, Map<String, Object> payload) {
        this.commandName = requireText(commandName, "commandName");
        this.payload = copyMap(payload);
    }

    public String commandName() {
        return commandName;
    }

    public Map<String, Object> payload() {
        return payload;
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static Map<String, Object> copyMap(Map<String, Object> source) {
        Objects.requireNonNull(source, "payload");
        Map<String, Object> copied = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = Objects.requireNonNull(entry.getKey(), "payload key");
            copied.put(key, entry.getValue());
        }
        return Collections.unmodifiableMap(copied);
    }
}
