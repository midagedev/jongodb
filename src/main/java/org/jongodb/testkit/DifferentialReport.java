package org.jongodb.testkit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Aggregate report for differential runs.
 */
public final class DifferentialReport {
    private final Instant generatedAt;
    private final String leftBackend;
    private final String rightBackend;
    private final List<DiffResult> results;

    public DifferentialReport(
        Instant generatedAt,
        String leftBackend,
        String rightBackend,
        List<DiffResult> results
    ) {
        this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
        this.leftBackend = requireText(leftBackend, "leftBackend");
        this.rightBackend = requireText(rightBackend, "rightBackend");
        this.results = copyResults(results);
    }

    public Instant generatedAt() {
        return generatedAt;
    }

    public String leftBackend() {
        return leftBackend;
    }

    public String rightBackend() {
        return rightBackend;
    }

    public List<DiffResult> results() {
        return results;
    }

    public int totalScenarios() {
        return results.size();
    }

    public int matchCount() {
        return countByStatus(DiffStatus.MATCH);
    }

    public int mismatchCount() {
        return countByStatus(DiffStatus.MISMATCH);
    }

    public int errorCount() {
        return countByStatus(DiffStatus.ERROR);
    }

    private int countByStatus(DiffStatus status) {
        int count = 0;
        for (DiffResult result : results) {
            if (result.status() == status) {
                count++;
            }
        }
        return count;
    }

    private static List<DiffResult> copyResults(List<DiffResult> source) {
        Objects.requireNonNull(source, "results");
        return List.copyOf(new ArrayList<>(source));
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
