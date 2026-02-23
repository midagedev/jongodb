package org.jongodb.engine;

import java.util.Objects;

/**
 * Identifier for a collection in a database.
 */
public final class Namespace {
    private final String database;
    private final String collection;

    public Namespace(String database, String collection) {
        this.database = requireNonBlank("database", database);
        this.collection = requireNonBlank("collection", collection);
    }

    public static Namespace of(String database, String collection) {
        return new Namespace(database, collection);
    }

    public String database() {
        return database;
    }

    public String collection() {
        return collection;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Namespace)) {
            return false;
        }
        Namespace that = (Namespace) other;
        return database.equals(that.database) && collection.equals(that.collection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, collection);
    }

    @Override
    public String toString() {
        return database + "." + collection;
    }

    private static String requireNonBlank(String fieldName, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
