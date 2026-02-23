package org.jongodb.testkit;

/**
 * One material difference detected between two backend outcomes.
 */
public final class DiffEntry {
    private final String path;
    private final Object leftValue;
    private final Object rightValue;
    private final String note;

    public DiffEntry(String path, Object leftValue, Object rightValue, String note) {
        this.path = requireText(path, "path");
        this.leftValue = leftValue;
        this.rightValue = rightValue;
        this.note = note == null ? "" : note.trim();
    }

    public String path() {
        return path;
    }

    public Object leftValue() {
        return leftValue;
    }

    public Object rightValue() {
        return rightValue;
    }

    public String note() {
        return note;
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
