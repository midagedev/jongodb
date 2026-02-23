package org.jongodb.testkit;

import java.util.Locale;
import java.util.Objects;

/**
 * Utility value for pass-rate calculations.
 */
public final class PassRate {
    private final int matchCount;
    private final int totalCount;

    private PassRate(int matchCount, int totalCount) {
        if (matchCount < 0) {
            throw new IllegalArgumentException("matchCount must be >= 0");
        }
        if (totalCount < 0) {
            throw new IllegalArgumentException("totalCount must be >= 0");
        }
        if (matchCount > totalCount) {
            throw new IllegalArgumentException("matchCount must be <= totalCount");
        }
        this.matchCount = matchCount;
        this.totalCount = totalCount;
    }

    public static PassRate from(DifferentialReport report) {
        Objects.requireNonNull(report, "report");
        return of(report.matchCount(), report.totalScenarios());
    }

    public static PassRate of(int matchCount, int totalCount) {
        return new PassRate(matchCount, totalCount);
    }

    public int matchCount() {
        return matchCount;
    }

    public int totalCount() {
        return totalCount;
    }

    /**
     * Ratio in [0.0, 1.0].
     */
    public double ratio() {
        if (totalCount == 0) {
            return 0.0d;
        }
        return (double) matchCount / (double) totalCount;
    }

    /**
     * Percentage in [0.0, 100.0].
     */
    public double percentage() {
        return ratio() * 100.0d;
    }

    public String formatted() {
        return String.format(Locale.ROOT, "%.2f%% (%d/%d)", percentage(), matchCount, totalCount);
    }
}
