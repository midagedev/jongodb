package org.jongodb.testkit;

import java.util.List;
import java.util.Objects;

/**
 * Aggregated fixture manifest validation errors.
 */
public final class FixtureManifestValidationException extends IllegalArgumentException {
    private final List<String> errors;

    public FixtureManifestValidationException(final List<String> errors) {
        super(formatMessage(errors));
        this.errors = List.copyOf(Objects.requireNonNull(errors, "errors"));
    }

    public List<String> errors() {
        return errors;
    }

    private static String formatMessage(final List<String> errors) {
        final List<String> normalized = List.copyOf(Objects.requireNonNull(errors, "errors"));
        if (normalized.isEmpty()) {
            return "fixture manifest validation failed";
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("fixture manifest validation failed (")
                .append(normalized.size())
                .append(" issue(s))");
        for (final String error : normalized) {
            sb.append('\n').append("- ").append(error);
        }
        return sb.toString();
    }
}
