package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Backend execution result for one scenario.
 */
public final class ScenarioOutcome {
    private final boolean success;
    private final List<Map<String, Object>> commandResults;
    private final String errorMessage;

    private ScenarioOutcome(boolean success, List<Map<String, Object>> commandResults, String errorMessage) {
        this.success = success;
        this.commandResults = normalizeCommandResults(commandResults);
        this.errorMessage = normalizeText(errorMessage);
        if (success && this.errorMessage != null) {
            throw new IllegalArgumentException("errorMessage must be null for success outcomes");
        }
        if (!success && this.errorMessage == null) {
            throw new IllegalArgumentException("errorMessage is required for failed outcomes");
        }
    }

    public static ScenarioOutcome success(List<Map<String, Object>> commandResults) {
        return new ScenarioOutcome(true, commandResults, null);
    }

    public static ScenarioOutcome failure(String errorMessage) {
        return new ScenarioOutcome(false, List.of(), errorMessage);
    }

    public boolean success() {
        return success;
    }

    public List<Map<String, Object>> commandResults() {
        return commandResults;
    }

    public Optional<String> errorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    private static List<Map<String, Object>> normalizeCommandResults(List<Map<String, Object>> commandResults) {
        Objects.requireNonNull(commandResults, "commandResults");
        List<Map<String, Object>> copied = new ArrayList<>(commandResults.size());
        for (Map<String, Object> result : commandResults) {
            Objects.requireNonNull(result, "commandResult");
            Map<String, Object> resultCopy = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : result.entrySet()) {
                String key = Objects.requireNonNull(entry.getKey(), "commandResult key");
                resultCopy.put(key, entry.getValue());
            }
            copied.add(Collections.unmodifiableMap(resultCopy));
        }
        return List.copyOf(copied);
    }

    private static String normalizeText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
