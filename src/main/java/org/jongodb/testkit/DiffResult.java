package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Differential comparison output for a single scenario.
 */
public final class DiffResult {
    private final String scenarioId;
    private final String leftBackend;
    private final String rightBackend;
    private final DiffStatus status;
    private final List<DiffEntry> entries;
    private final String errorMessage;

    private DiffResult(
        String scenarioId,
        String leftBackend,
        String rightBackend,
        DiffStatus status,
        List<DiffEntry> entries,
        String errorMessage
    ) {
        this.scenarioId = requireText(scenarioId, "scenarioId");
        this.leftBackend = requireText(leftBackend, "leftBackend");
        this.rightBackend = requireText(rightBackend, "rightBackend");
        this.status = Objects.requireNonNull(status, "status");
        this.entries = copyEntries(entries);
        this.errorMessage = normalize(errorMessage);
    }

    public static DiffResult match(String scenarioId, String leftBackend, String rightBackend) {
        return new DiffResult(scenarioId, leftBackend, rightBackend, DiffStatus.MATCH, List.of(), null);
    }

    public static DiffResult mismatch(
        String scenarioId,
        String leftBackend,
        String rightBackend,
        List<DiffEntry> entries
    ) {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty for mismatches");
        }
        return new DiffResult(scenarioId, leftBackend, rightBackend, DiffStatus.MISMATCH, entries, null);
    }

    public static DiffResult error(
        String scenarioId,
        String leftBackend,
        String rightBackend,
        String errorMessage
    ) {
        return new DiffResult(scenarioId, leftBackend, rightBackend, DiffStatus.ERROR, List.of(), errorMessage);
    }

    public String scenarioId() {
        return scenarioId;
    }

    public String leftBackend() {
        return leftBackend;
    }

    public String rightBackend() {
        return rightBackend;
    }

    public DiffStatus status() {
        return status;
    }

    public List<DiffEntry> entries() {
        return entries;
    }

    public Optional<String> errorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    private static List<DiffEntry> copyEntries(List<DiffEntry> source) {
        Objects.requireNonNull(source, "entries");
        return List.copyOf(new ArrayList<>(source));
    }

    private static String requireText(String value, String fieldName) {
        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
