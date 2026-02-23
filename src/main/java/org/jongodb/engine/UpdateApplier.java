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

    static ParsedUpdate parse(final Document update) {
        Objects.requireNonNull(update, "update");
        if (update.isEmpty()) {
            throw new IllegalArgumentException("update must not be empty");
        }

        final boolean operatorStyle = update.keySet().iterator().next().startsWith("$");
        for (final String key : update.keySet()) {
            if (key.startsWith("$") != operatorStyle) {
                throw new IllegalArgumentException(
                        "update must either be an operator document or a replacement document");
            }
        }

        if (!operatorStyle) {
            return ParsedUpdate.replacement(DocumentCopies.copy(update));
        }

        final List<SetOperation> setOperations = new ArrayList<>();
        final List<IncOperation> incrementOperations = new ArrayList<>();
        final List<String> unsetOperations = new ArrayList<>();

        for (final Map.Entry<String, Object> entry : update.entrySet()) {
            final String operator = entry.getKey();
            final Map<String, Object> definition = readDefinition(operator, entry.getValue());
            switch (operator) {
                case "$set":
                    for (final Map.Entry<String, Object> setEntry : definition.entrySet()) {
                        setOperations.add(new SetOperation(setEntry.getKey(), setEntry.getValue()));
                    }
                    break;
                case "$inc":
                    for (final Map.Entry<String, Object> incEntry : definition.entrySet()) {
                        final Object delta = incEntry.getValue();
                        if (!(delta instanceof Number)) {
                            throw new IllegalArgumentException(
                                    "$inc value for '" + incEntry.getKey() + "' must be numeric");
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

        return ParsedUpdate.operator(setOperations, incrementOperations, unsetOperations);
    }

    static void validateApplicable(final Document document, final ParsedUpdate update) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(update, "update");

        if (update.replacementStyle()) {
            validateReplacementApplicable(document, update.replacementDocument());
            return;
        }

        for (final SetOperation operation : update.setOperations()) {
            ensureWritablePath(document, operation.path());
        }
        for (final IncOperation operation : update.incrementOperations()) {
            ensureWritablePath(document, operation.path());

            final PathLookup target = lookupPath(document, operation.path());
            if (target.exists() && !(target.value() instanceof Number)) {
                throw new IllegalArgumentException(
                        "$inc target for '" + operation.path() + "' must be numeric");
            }
        }
    }

    static boolean apply(final Document document, final ParsedUpdate update) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(update, "update");

        if (update.replacementStyle()) {
            return applyReplacement(document, update.replacementDocument());
        }

        boolean modified = false;

        for (final SetOperation operation : update.setOperations()) {
            modified |= applySet(document, operation.path(), operation.value());
        }
        for (final IncOperation operation : update.incrementOperations()) {
            modified |= applyIncrement(document, operation.path(), operation.delta());
        }
        for (final String path : update.unsetOperations()) {
            modified |= applyUnset(document, path);
        }

        return modified;
    }

    private static void validateReplacementApplicable(final Document current, final Document replacement) {
        if (!current.containsKey("_id") || !replacement.containsKey("_id")) {
            return;
        }

        if (!valueEquals(current.get("_id"), replacement.get("_id"))) {
            throw new IllegalArgumentException("replacement update cannot change immutable field '_id'");
        }
    }

    private static boolean applyReplacement(final Document document, final Document replacement) {
        final Document next = DocumentCopies.copy(replacement);

        if (!next.containsKey("_id") && document.containsKey("_id")) {
            next.put("_id", DocumentCopies.copyAny(document.get("_id")));
        }

        if (document.containsKey("_id")
                && next.containsKey("_id")
                && !valueEquals(document.get("_id"), next.get("_id"))) {
            throw new IllegalArgumentException("replacement update cannot change immutable field '_id'");
        }

        if (valueEquals(document, next)) {
            return false;
        }

        document.clear();
        document.putAll(next);
        return true;
    }

    private static Map<String, Object> readDefinition(final String operator, final Object rawDefinition) {
        if (!(rawDefinition instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(operator + " definition must be a document");
        }

        final Map<String, Object> definition = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : ((Map<?, ?>) rawDefinition).entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw new IllegalArgumentException(operator + " field path must be a string");
            }
            final String fieldPath = (String) entry.getKey();
            parsePath(fieldPath);
            definition.put(fieldPath, entry.getValue());
        }
        return definition;
    }

    private static boolean applySet(final Document document, final String path, final Object value) {
        final Map<String, Object> parent = getOrCreateParent(document, path);
        final String leaf = leaf(path);

        final Object nextValue = DocumentCopies.copyAny(value);
        final boolean hadLeaf = parent.containsKey(leaf);
        final Object currentValue = parent.get(leaf);

        if (hadLeaf && valueEquals(currentValue, nextValue)) {
            return false;
        }

        parent.put(leaf, nextValue);
        return true;
    }

    private static boolean applyIncrement(final Document document, final String path, final Number delta) {
        final Map<String, Object> parent = getOrCreateParent(document, path);
        final String leaf = leaf(path);

        if (!parent.containsKey(leaf)) {
            parent.put(leaf, delta);
            return true;
        }

        final Object currentValue = parent.get(leaf);
        if (!(currentValue instanceof Number)) {
            throw new IllegalArgumentException("$inc target for '" + path + "' must be numeric");
        }

        final Number updatedValue = add((Number) currentValue, delta);
        if (valueEquals(currentValue, updatedValue)) {
            return false;
        }

        parent.put(leaf, updatedValue);
        return true;
    }

    private static boolean applyUnset(final Document document, final String path) {
        final Map<String, Object> parent = findParent(document, path);
        if (parent == null) {
            return false;
        }

        final String leaf = leaf(path);
        if (!parent.containsKey(leaf)) {
            return false;
        }

        parent.remove(leaf);
        return true;
    }

    private static void ensureWritablePath(final Document document, final String path) {
        final String[] segments = parsePath(path);
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) document;

        for (int i = 0; i < segments.length - 1; i++) {
            final String segment = segments[i];
            if (!current.containsKey(segment)) {
                return;
            }

            final Object next = current.get(segment);
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

    private static PathLookup lookupPath(final Document document, final String path) {
        final String[] segments = parsePath(path);
        Object current = document;

        for (int i = 0; i < segments.length; i++) {
            if (!(current instanceof Map<?, ?>)) {
                return PathLookup.missing();
            }

            final Map<?, ?> map = (Map<?, ?>) current;
            final String segment = segments[i];
            if (!map.containsKey(segment)) {
                return PathLookup.missing();
            }

            final Object value = map.get(segment);
            if (i == segments.length - 1) {
                return PathLookup.existing(value);
            }

            current = value;
        }

        return PathLookup.missing();
    }

    private static Map<String, Object> getOrCreateParent(final Document document, final String path) {
        final String[] segments = parsePath(path);
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) document;

        for (int i = 0; i < segments.length - 1; i++) {
            final String segment = segments[i];
            if (!current.containsKey(segment)) {
                final Document created = new Document();
                current.put(segment, created);
                current = created;
                continue;
            }

            final Object next = current.get(segment);
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

    private static Map<String, Object> findParent(final Document document, final String path) {
        final String[] segments = parsePath(path);
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) document;

        for (int i = 0; i < segments.length - 1; i++) {
            final String segment = segments[i];
            if (!current.containsKey(segment)) {
                return null;
            }

            final Object next = current.get(segment);
            if (!(next instanceof Map<?, ?>)) {
                return null;
            }

            current = castMap((Map<?, ?>) next);
        }

        return current;
    }

    private static String[] parsePath(final String fieldPath) {
        if (fieldPath == null || fieldPath.isBlank()) {
            throw new IllegalArgumentException("field path must not be blank");
        }

        final String[] segments = fieldPath.split("\\.");
        for (final String segment : segments) {
            if (segment.isEmpty()) {
                throw new IllegalArgumentException("field path must not contain empty segments");
            }
            if (isUnsupportedPositionalSegment(segment)) {
                throw new IllegalArgumentException(
                        "positional and array filter updates are not supported for path '" + fieldPath + "'");
            }
        }
        return segments;
    }

    private static boolean isUnsupportedPositionalSegment(final String segment) {
        if ("$".equals(segment) || "$[]".equals(segment)) {
            return true;
        }
        return segment.startsWith("$[") && segment.endsWith("]");
    }

    private static String leaf(final String path) {
        final String[] segments = parsePath(path);
        return segments[segments.length - 1];
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(final Map<?, ?> value) {
        return (Map<String, Object>) value;
    }

    private static boolean valueEquals(final Object left, final Object right) {
        return Objects.deepEquals(left, right);
    }

    private static Number add(final Number current, final Number delta) {
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

    private static boolean isFloating(final Number value) {
        return value instanceof Float || value instanceof Double || value instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(final Number value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        return new BigDecimal(value.toString());
    }

    private static BigInteger toBigInteger(final Number value) {
        try {
            if (value instanceof BigInteger) {
                return (BigInteger) value;
            }
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).toBigIntegerExact();
            }
            return new BigDecimal(value.toString()).toBigIntegerExact();
        } catch (final ArithmeticException exception) {
            throw new IllegalArgumentException("$inc value must be integral for BigInteger target", exception);
        }
    }

    static final class ParsedUpdate {
        private final List<SetOperation> setOperations;
        private final List<IncOperation> incrementOperations;
        private final List<String> unsetOperations;
        private final Document replacementDocument;

        private ParsedUpdate(
                final List<SetOperation> setOperations,
                final List<IncOperation> incrementOperations,
                final List<String> unsetOperations,
                final Document replacementDocument) {
            this.setOperations = List.copyOf(setOperations);
            this.incrementOperations = List.copyOf(incrementOperations);
            this.unsetOperations = List.copyOf(unsetOperations);
            this.replacementDocument = replacementDocument == null ? null : DocumentCopies.copy(replacementDocument);
        }

        static ParsedUpdate operator(
                final List<SetOperation> setOperations,
                final List<IncOperation> incrementOperations,
                final List<String> unsetOperations) {
            return new ParsedUpdate(setOperations, incrementOperations, unsetOperations, null);
        }

        static ParsedUpdate replacement(final Document replacementDocument) {
            return new ParsedUpdate(List.of(), List.of(), List.of(), replacementDocument);
        }

        boolean replacementStyle() {
            return replacementDocument != null;
        }

        Document replacementDocument() {
            return replacementDocument == null ? null : DocumentCopies.copy(replacementDocument);
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

        static PathLookup existing(final Object value) {
            return new PathLookup(true, value);
        }
    }
}
