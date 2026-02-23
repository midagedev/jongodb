package org.jongodb.engine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

final class UpdateApplier {
    private UpdateApplier() {}

    static ParsedUpdate parse(Document update) {
        Objects.requireNonNull(update, "update");
        if (update.isEmpty()) {
            throw new IllegalArgumentException("update must not be empty");
        }

        List<SetOperation> setOperations = new ArrayList<>();
        List<IncOperation> incrementOperations = new ArrayList<>();
        List<String> unsetOperations = new ArrayList<>();

        for (Map.Entry<String, Object> entry : update.entrySet()) {
            String operator = entry.getKey();
            Map<String, Object> definition = readDefinition(operator, entry.getValue());
            switch (operator) {
                case "$set":
                    for (Map.Entry<String, Object> setEntry : definition.entrySet()) {
                        setOperations.add(new SetOperation(setEntry.getKey(), setEntry.getValue()));
                    }
                    break;
                case "$inc":
                    for (Map.Entry<String, Object> incEntry : definition.entrySet()) {
                        Object delta = incEntry.getValue();
                        if (!(delta instanceof Number)) {
                            throw new IllegalArgumentException(
                                    "$inc value for '"
                                            + incEntry.getKey()
                                            + "' must be numeric");
                        }
                        incrementOperations.add(new IncOperation(incEntry.getKey(), (Number) delta));
                    }
                    break;
                case "$unset":
                    unsetOperations.addAll(definition.keySet());
                    break;
                default:
                    throw new IllegalArgumentException("unsupported update operator: " + operator);
            }
        }

        return new ParsedUpdate(setOperations, incrementOperations, unsetOperations);
    }

    static void validateApplicable(Document document, ParsedUpdate update) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(update, "update");

        for (SetOperation operation : update.setOperations()) {
            ensureWritablePath(document, operation.path());
        }
        for (IncOperation operation : update.incrementOperations()) {
            ensureWritablePath(document, operation.path());

            PathLookup target = lookupPath(document, operation.path());
            if (target.exists() && !(target.value() instanceof Number)) {
                throw new IllegalArgumentException(
                        "$inc target for '" + operation.path() + "' must be numeric");
            }
        }
    }

    static boolean apply(Document document, ParsedUpdate update) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(update, "update");

        boolean modified = false;

        for (SetOperation operation : update.setOperations()) {
            modified |= applySet(document, operation.path(), operation.value());
        }
        for (IncOperation operation : update.incrementOperations()) {
            modified |= applyIncrement(document, operation.path(), operation.delta());
        }
        for (String path : update.unsetOperations()) {
            modified |= applyUnset(document, path);
        }

        return modified;
    }

    private static Map<String, Object> readDefinition(String operator, Object rawDefinition) {
        if (!(rawDefinition instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(operator + " definition must be a document");
        }

        Map<String, Object> definition = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) rawDefinition).entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw new IllegalArgumentException(operator + " field path must be a string");
            }
            String fieldPath = (String) entry.getKey();
            parsePath(fieldPath);
            definition.put(fieldPath, entry.getValue());
        }
        return definition;
    }

    private static boolean applySet(Document document, String path, Object value) {
        Map<String, Object> parent = getOrCreateParent(document, path);
        String leaf = leaf(path);

        Object nextValue = DocumentCopies.copyAny(value);
        boolean hadLeaf = parent.containsKey(leaf);
        Object currentValue = parent.get(leaf);

        if (hadLeaf && valueEquals(currentValue, nextValue)) {
            return false;
        }

        parent.put(leaf, nextValue);
        return true;
    }

    private static boolean applyIncrement(Document document, String path, Number delta) {
        Map<String, Object> parent = getOrCreateParent(document, path);
        String leaf = leaf(path);

        if (!parent.containsKey(leaf)) {
            parent.put(leaf, delta);
            return true;
        }

        Object currentValue = parent.get(leaf);
        if (!(currentValue instanceof Number)) {
            throw new IllegalArgumentException("$inc target for '" + path + "' must be numeric");
        }

        Number updatedValue = add((Number) currentValue, delta);
        if (valueEquals(currentValue, updatedValue)) {
            return false;
        }

        parent.put(leaf, updatedValue);
        return true;
    }

    private static boolean applyUnset(Document document, String path) {
        Map<String, Object> parent = findParent(document, path);
        if (parent == null) {
            return false;
        }

        String leaf = leaf(path);
        if (!parent.containsKey(leaf)) {
            return false;
        }

        parent.remove(leaf);
        return true;
    }

    private static void ensureWritablePath(Document document, String path) {
        String[] segments = parsePath(path);
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) document;

        for (int i = 0; i < segments.length - 1; i++) {
            String segment = segments[i];
            if (!current.containsKey(segment)) {
                return;
            }

            Object next = current.get(segment);
            if (!(next instanceof Map<?, ?>)) {
                throw new IllegalArgumentException(
                        "cannot update path '"
                                + path
                                + "' through non-document segment '"
                                + segment
                                + "'");
            }

            current = castMap((Map<?, ?>) next);
        }
    }

    private static PathLookup lookupPath(Document document, String path) {
        String[] segments = parsePath(path);
        Object current = document;

        for (int i = 0; i < segments.length; i++) {
            if (!(current instanceof Map<?, ?>)) {
                return PathLookup.missing();
            }

            Map<?, ?> map = (Map<?, ?>) current;
            String segment = segments[i];
            if (!map.containsKey(segment)) {
                return PathLookup.missing();
            }

            Object value = map.get(segment);
            if (i == segments.length - 1) {
                return PathLookup.existing(value);
            }

            current = value;
        }

        return PathLookup.missing();
    }

    private static Map<String, Object> getOrCreateParent(Document document, String path) {
        String[] segments = parsePath(path);
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) document;

        for (int i = 0; i < segments.length - 1; i++) {
            String segment = segments[i];
            if (!current.containsKey(segment)) {
                Document created = new Document();
                current.put(segment, created);
                current = created;
                continue;
            }

            Object next = current.get(segment);
            if (!(next instanceof Map<?, ?>)) {
                throw new IllegalArgumentException(
                        "cannot update path '"
                                + path
                                + "' through non-document segment '"
                                + segment
                                + "'");
            }
            current = castMap((Map<?, ?>) next);
        }

        return current;
    }

    private static Map<String, Object> findParent(Document document, String path) {
        String[] segments = parsePath(path);
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) document;

        for (int i = 0; i < segments.length - 1; i++) {
            String segment = segments[i];
            if (!current.containsKey(segment)) {
                return null;
            }

            Object next = current.get(segment);
            if (!(next instanceof Map<?, ?>)) {
                return null;
            }

            current = castMap((Map<?, ?>) next);
        }

        return current;
    }

    private static String[] parsePath(String fieldPath) {
        if (fieldPath == null || fieldPath.isBlank()) {
            throw new IllegalArgumentException("field path must not be blank");
        }

        String[] segments = fieldPath.split("\\.");
        for (String segment : segments) {
            if (segment.isEmpty()) {
                throw new IllegalArgumentException("field path must not contain empty segments");
            }
        }
        return segments;
    }

    private static String leaf(String path) {
        String[] segments = parsePath(path);
        return segments[segments.length - 1];
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> value) {
        return (Map<String, Object>) value;
    }

    private static boolean valueEquals(Object left, Object right) {
        return Objects.deepEquals(left, right);
    }

    private static Number add(Number current, Number delta) {
        if (current instanceof BigDecimal) {
            return ((BigDecimal) current).add(toBigDecimal(delta));
        }
        if (current instanceof BigInteger) {
            return ((BigInteger) current).add(toBigInteger(delta));
        }

        if (isFloating(current) || isFloating(delta)) {
            if (current instanceof Float) {
                return current.floatValue() + delta.floatValue();
            }
            return current.doubleValue() + delta.doubleValue();
        }

        if (current instanceof Byte) {
            return (byte) (current.byteValue() + delta.byteValue());
        }
        if (current instanceof Short) {
            return (short) (current.shortValue() + delta.shortValue());
        }
        if (current instanceof Integer) {
            return current.intValue() + delta.intValue();
        }
        if (current instanceof Long) {
            return current.longValue() + delta.longValue();
        }

        return current.doubleValue() + delta.doubleValue();
    }

    private static boolean isFloating(Number value) {
        return value instanceof Float || value instanceof Double || value instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Number value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        return new BigDecimal(value.toString());
    }

    private static BigInteger toBigInteger(Number value) {
        try {
            if (value instanceof BigInteger) {
                return (BigInteger) value;
            }
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).toBigIntegerExact();
            }
            return new BigDecimal(value.toString()).toBigIntegerExact();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("$inc value must be integral for BigInteger target", exception);
        }
    }

    static final class ParsedUpdate {
        private final List<SetOperation> setOperations;
        private final List<IncOperation> incrementOperations;
        private final List<String> unsetOperations;

        ParsedUpdate(
                List<SetOperation> setOperations,
                List<IncOperation> incrementOperations,
                List<String> unsetOperations) {
            this.setOperations = List.copyOf(setOperations);
            this.incrementOperations = List.copyOf(incrementOperations);
            this.unsetOperations = List.copyOf(unsetOperations);
        }

        List<SetOperation> setOperations() {
            return Collections.unmodifiableList(setOperations);
        }

        List<IncOperation> incrementOperations() {
            return Collections.unmodifiableList(incrementOperations);
        }

        List<String> unsetOperations() {
            return Collections.unmodifiableList(unsetOperations);
        }
    }

    private record SetOperation(String path, Object value) {}

    private record IncOperation(String path, Number delta) {}

    private record PathLookup(boolean exists, Object value) {
        static PathLookup missing() {
            return new PathLookup(false, null);
        }

        static PathLookup existing(Object value) {
            return new PathLookup(true, value);
        }
    }
}
