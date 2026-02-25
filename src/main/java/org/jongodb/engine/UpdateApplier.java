package org.jongodb.engine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.bson.Document;

final class UpdateApplier {
    private UpdateApplier() {}

    static ParsedUpdate parse(final Document update) {
        return parse(update, List.of());
    }

    static ParsedUpdate parse(final Document update, final List<Document> arrayFilters) {
        Objects.requireNonNull(update, "update");
        if (update.isEmpty()) {
            throw new IllegalArgumentException("update must not be empty");
        }
        final ArrayFilterBindings parsedArrayFilters = ArrayFilterBindings.parse(arrayFilters);

        final boolean operatorStyle = update.keySet().iterator().next().startsWith("$");
        for (final String key : update.keySet()) {
            if (key.startsWith("$") != operatorStyle) {
                throw new IllegalArgumentException(
                        "update must either be an operator document or a replacement document");
            }
        }

        if (!operatorStyle) {
            if (!parsedArrayFilters.isEmpty()) {
                throw new IllegalArgumentException("arrayFilters is only supported for operator updates");
            }
            return ParsedUpdate.replacement(DocumentCopies.copy(update));
        }

        final List<SetOperation> setOperations = new ArrayList<>();
        final List<SetOnInsertOperation> setOnInsertOperations = new ArrayList<>();
        final List<IncOperation> incrementOperations = new ArrayList<>();
        final List<String> unsetOperations = new ArrayList<>();
        final List<AddToSetOperation> addToSetOperations = new ArrayList<>();

        for (final Map.Entry<String, Object> entry : update.entrySet()) {
            final String operator = entry.getKey();
            final Map<String, Object> definition = readDefinition(operator, entry.getValue(), parsedArrayFilters);
            switch (operator) {
                case "$set":
                    for (final Map.Entry<String, Object> setEntry : definition.entrySet()) {
                        setOperations.add(new SetOperation(setEntry.getKey(), setEntry.getValue()));
                    }
                    break;
                case "$setOnInsert":
                    for (final Map.Entry<String, Object> setOnInsertEntry : definition.entrySet()) {
                        setOnInsertOperations.add(
                                new SetOnInsertOperation(setOnInsertEntry.getKey(), setOnInsertEntry.getValue()));
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
                case "$addToSet":
                    for (final Map.Entry<String, Object> addToSetEntry : definition.entrySet()) {
                        addToSetOperations.add(
                                new AddToSetOperation(
                                        addToSetEntry.getKey(),
                                        parseAddToSetValues(addToSetEntry.getKey(), addToSetEntry.getValue())));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unsupported update operator: " + operator);
            }
        }

        return ParsedUpdate.operator(
                setOperations,
                setOnInsertOperations,
                incrementOperations,
                unsetOperations,
                addToSetOperations,
                parsedArrayFilters);
    }

    static void validateApplicable(final Document document, final ParsedUpdate update) {
        validateApplicable(document, update, false);
    }

    static void validateApplicableForUpsertInsert(final Document document, final ParsedUpdate update) {
        validateApplicable(document, update, true);
    }

    private static void validateApplicable(
            final Document document, final ParsedUpdate update, final boolean includeSetOnInsert) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(update, "update");

        if (update.replacementStyle()) {
            validateReplacementApplicable(document, update.replacementDocument());
            return;
        }

        for (final SetOperation operation : update.setOperations()) {
            ensureWritablePath(document, operation.path(), update.arrayFilterBindings());
        }
        if (includeSetOnInsert) {
            for (final SetOnInsertOperation operation : update.setOnInsertOperations()) {
                ensureWritablePath(document, operation.path(), update.arrayFilterBindings());
            }
        }
        for (final IncOperation operation : update.incrementOperations()) {
            ensureWritablePath(document, operation.path(), update.arrayFilterBindings());

            final PathLookup target = lookupPath(document, operation.path());
            if (target.exists() && !(target.value() instanceof Number)) {
                throw new IllegalArgumentException(
                        "$inc target for '" + operation.path() + "' must be numeric");
            }
        }
        for (final AddToSetOperation operation : update.addToSetOperations()) {
            ensureWritablePath(document, operation.path(), update.arrayFilterBindings());

            final PathLookup target = lookupPath(document, operation.path());
            if (target.exists() && !(target.value() instanceof List<?>)) {
                throw new IllegalArgumentException(
                        "$addToSet target for '" + operation.path() + "' must be an array");
            }
        }
    }

    static boolean apply(final Document document, final ParsedUpdate update) {
        return apply(document, update, false);
    }

    static boolean applyForUpsertInsert(final Document document, final ParsedUpdate update) {
        return apply(document, update, true);
    }

    private static boolean apply(
            final Document document, final ParsedUpdate update, final boolean includeSetOnInsert) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(update, "update");

        if (update.replacementStyle()) {
            return applyReplacement(document, update.replacementDocument());
        }

        boolean modified = false;

        for (final SetOperation operation : update.setOperations()) {
            modified |= applySet(document, operation.path(), operation.value(), update.arrayFilterBindings());
        }
        if (includeSetOnInsert) {
            for (final SetOnInsertOperation operation : update.setOnInsertOperations()) {
                modified |= applySet(document, operation.path(), operation.value(), update.arrayFilterBindings());
            }
        }
        for (final IncOperation operation : update.incrementOperations()) {
            modified |= applyIncrement(document, operation.path(), operation.delta());
        }
        for (final String path : update.unsetOperations()) {
            modified |= applyUnset(document, path, update.arrayFilterBindings());
        }
        for (final AddToSetOperation operation : update.addToSetOperations()) {
            modified |= applyAddToSet(document, operation.path(), operation.values());
        }

        return modified;
    }

    private static List<Object> parseAddToSetValues(final String path, final Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> mapValue) || !mapValue.containsKey("$each")) {
            return List.of(DocumentCopies.copyAny(rawValue));
        }

        if (mapValue.size() != 1) {
            throw new IllegalArgumentException("$addToSet for '" + path + "' only supports $each");
        }

        final Object eachValue = mapValue.get("$each");
        if (!(eachValue instanceof List<?> eachList)) {
            throw new IllegalArgumentException("$addToSet.$each for '" + path + "' must be an array");
        }

        final List<Object> values = new ArrayList<>(eachList.size());
        for (final Object item : eachList) {
            values.add(DocumentCopies.copyAny(item));
        }
        return List.copyOf(values);
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

    private static Map<String, Object> readDefinition(
            final String operator, final Object rawDefinition, final ArrayFilterBindings arrayFilterBindings) {
        if (!(rawDefinition instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(operator + " definition must be a document");
        }

        final Map<String, Object> definition = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : ((Map<?, ?>) rawDefinition).entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw new IllegalArgumentException(operator + " field path must be a string");
            }
            final String fieldPath = (String) entry.getKey();
            validateSupportedPathForOperator(fieldPath, operator, arrayFilterBindings);
            definition.put(fieldPath, entry.getValue());
        }
        return definition;
    }

    private static void validateSupportedPathForOperator(
            final String fieldPath, final String operator, final ArrayFilterBindings arrayFilterBindings) {
        final boolean arrayFilterPath = pathContainsArrayFilter(fieldPath);
        if (arrayFilterPath && !"$set".equals(operator) && !"$unset".equals(operator)) {
            throw new IllegalArgumentException(
                    "arrayFilters currently support only $set/$unset for path '" + fieldPath + "'");
        }
        parsePath(fieldPath, arrayFilterBindings.identifiers(), arrayFilterPath);
    }

    private static boolean applySet(
            final Document document,
            final String path,
            final Object value,
            final ArrayFilterBindings arrayFilterBindings) {
        if (pathContainsArrayFilter(path)) {
            return applySetWithArrayFilters(document, path, value, arrayFilterBindings);
        }
        return applySetWithoutArrayFilters(document, path, value);
    }

    private static boolean applySetWithoutArrayFilters(final Document document, final String path, final Object value) {
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

    private static boolean applySetWithArrayFilters(
            final Document document,
            final String path,
            final Object value,
            final ArrayFilterBindings arrayFilterBindings) {
        if (arrayFilterBindings.isEmpty()) {
            throw new IllegalArgumentException("arrayFilters must be specified for path '" + path + "'");
        }
        final String[] segments = parsePath(path, arrayFilterBindings.identifiers(), true);
        return applySetWithArrayFilters(document, path, segments, 0, value, arrayFilterBindings);
    }

    private static boolean applySetWithArrayFilters(
            final Object current,
            final String path,
            final String[] segments,
            final int segmentIndex,
            final Object value,
            final ArrayFilterBindings arrayFilterBindings) {
        final String segment = segments[segmentIndex];
        final boolean leaf = segmentIndex == segments.length - 1;

        if (isArrayFilterSegment(segment)) {
            if (!(current instanceof List<?> listValue)) {
                throw new IllegalArgumentException(
                        "cannot update path '" + path + "' through non-array segment '" + segment + "'");
            }
            final List<Object> list = castList(listValue);
            final Document arrayFilter = arrayFilterBindings.filterFor(arrayFilterIdentifier(segment));
            boolean modified = false;
            for (int i = 0; i < list.size(); i++) {
                final Object item = list.get(i);
                if (!matchesArrayFilter(item, arrayFilter)) {
                    continue;
                }
                if (leaf) {
                    final Object nextValue = DocumentCopies.copyAny(value);
                    if (valueEquals(item, nextValue)) {
                        continue;
                    }
                    list.set(i, nextValue);
                    modified = true;
                    continue;
                }
                modified |= applySetWithArrayFilters(item, path, segments, segmentIndex + 1, value, arrayFilterBindings);
            }
            return modified;
        }

        if (!(current instanceof Map<?, ?> mapValue)) {
            throw new IllegalArgumentException(
                    "cannot update path '" + path + "' through non-document segment '" + segment + "'");
        }

        final Map<String, Object> map = castMap(mapValue);
        if (leaf) {
            final Object nextValue = DocumentCopies.copyAny(value);
            if (map.containsKey(segment) && valueEquals(map.get(segment), nextValue)) {
                return false;
            }
            map.put(segment, nextValue);
            return true;
        }

        final String nextSegment = segments[segmentIndex + 1];
        if (!map.containsKey(segment)) {
            if (isArrayFilterSegment(nextSegment)) {
                return false;
            }
            final Document created = new Document();
            map.put(segment, created);
            return applySetWithArrayFilters(created, path, segments, segmentIndex + 1, value, arrayFilterBindings);
        }

        final Object next = map.get(segment);
        if (isArrayFilterSegment(nextSegment)) {
            if (!(next instanceof List<?>)) {
                throw new IllegalArgumentException(
                        "cannot update path '" + path + "' through non-array segment '" + segment + "'");
            }
            return applySetWithArrayFilters(next, path, segments, segmentIndex + 1, value, arrayFilterBindings);
        }
        if (!(next instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(
                    "cannot update path '" + path + "' through non-document segment '" + segment + "'");
        }
        return applySetWithArrayFilters(next, path, segments, segmentIndex + 1, value, arrayFilterBindings);
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

    private static boolean applyUnset(
            final Document document, final String path, final ArrayFilterBindings arrayFilterBindings) {
        if (pathContainsArrayFilter(path)) {
            return applyUnsetWithArrayFilters(document, path, arrayFilterBindings);
        }
        return applyUnsetWithoutArrayFilters(document, path);
    }

    private static boolean applyUnsetWithoutArrayFilters(final Document document, final String path) {
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

    private static boolean applyUnsetWithArrayFilters(
            final Document document, final String path, final ArrayFilterBindings arrayFilterBindings) {
        if (arrayFilterBindings.isEmpty()) {
            throw new IllegalArgumentException("arrayFilters must be specified for path '" + path + "'");
        }
        final String[] segments = parsePath(path, arrayFilterBindings.identifiers(), true);
        return applyUnsetWithArrayFilters(document, path, segments, 0, arrayFilterBindings);
    }

    private static boolean applyUnsetWithArrayFilters(
            final Object current,
            final String path,
            final String[] segments,
            final int segmentIndex,
            final ArrayFilterBindings arrayFilterBindings) {
        final String segment = segments[segmentIndex];
        final boolean leaf = segmentIndex == segments.length - 1;

        if (isArrayFilterSegment(segment)) {
            if (!(current instanceof List<?> listValue)) {
                return false;
            }
            final List<Object> list = castList(listValue);
            final Document arrayFilter = arrayFilterBindings.filterFor(arrayFilterIdentifier(segment));
            boolean modified = false;
            for (int i = 0; i < list.size(); i++) {
                final Object item = list.get(i);
                if (!matchesArrayFilter(item, arrayFilter)) {
                    continue;
                }
                if (leaf) {
                    if (item == null) {
                        continue;
                    }
                    list.set(i, null);
                    modified = true;
                    continue;
                }
                modified |= applyUnsetWithArrayFilters(item, path, segments, segmentIndex + 1, arrayFilterBindings);
            }
            return modified;
        }

        if (!(current instanceof Map<?, ?> mapValue)) {
            return false;
        }
        final Map<String, Object> map = castMap(mapValue);
        if (leaf) {
            if (!map.containsKey(segment)) {
                return false;
            }
            map.remove(segment);
            return true;
        }
        if (!map.containsKey(segment)) {
            return false;
        }

        final Object next = map.get(segment);
        return applyUnsetWithArrayFilters(next, path, segments, segmentIndex + 1, arrayFilterBindings);
    }

    private static boolean applyAddToSet(final Document document, final String path, final List<Object> values) {
        final Map<String, Object> parent = getOrCreateParent(document, path);
        final String leaf = leaf(path);

        final List<Object> targetList;
        if (!parent.containsKey(leaf)) {
            targetList = new ArrayList<>();
            parent.put(leaf, targetList);
        } else {
            final Object existing = parent.get(leaf);
            if (!(existing instanceof List<?> existingList)) {
                throw new IllegalArgumentException("$addToSet target for '" + path + "' must be an array");
            }
            targetList = castList(existingList);
        }

        boolean modified = false;
        for (final Object rawValue : values) {
            final Object copiedValue = DocumentCopies.copyAny(rawValue);
            if (!containsByMongoEquality(targetList, copiedValue)) {
                targetList.add(copiedValue);
                modified = true;
            }
        }
        return modified;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> castList(final List<?> source) {
        return (List<Object>) source;
    }

    private static boolean containsByMongoEquality(final List<Object> values, final Object candidate) {
        for (final Object value : values) {
            if (valueEquals(value, candidate)) {
                return true;
            }
        }
        return false;
    }

    private static void ensureWritablePath(
            final Document document, final String path, final ArrayFilterBindings arrayFilterBindings) {
        if (pathContainsArrayFilter(path)) {
            parsePath(path, arrayFilterBindings.identifiers(), true);
            return;
        }
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
        return parsePath(fieldPath, Set.of(), false);
    }

    private static String[] parsePath(
            final String fieldPath,
            final Set<String> arrayFilterIdentifiers,
            final boolean allowArrayFilterSegments) {
        if (fieldPath == null || fieldPath.isBlank()) {
            throw new IllegalArgumentException("field path must not be blank");
        }

        final String[] segments = fieldPath.split("\\.");
        for (final String segment : segments) {
            if (segment.isEmpty()) {
                throw new IllegalArgumentException("field path must not contain empty segments");
            }
            if ("$".equals(segment) || "$[]".equals(segment)) {
                throw new IllegalArgumentException(
                        "positional and array filter updates are not supported for path '" + fieldPath + "'");
            }
            if (!isArrayFilterSegment(segment)) {
                if (segment.contains("$[") || segment.contains("]")) {
                    throw new IllegalArgumentException(
                            "invalid array filter identifier segment in path '" + fieldPath + "'");
                }
                continue;
            }

            if (!allowArrayFilterSegments) {
                throw new IllegalArgumentException(
                        "positional and array filter updates are not supported for path '" + fieldPath + "'");
            }
            final String identifier = arrayFilterIdentifier(segment);
            if (identifier.isEmpty() || !isValidArrayFilterIdentifier(identifier)) {
                throw new IllegalArgumentException("invalid array filter identifier '" + identifier + "'");
            }
            if (arrayFilterIdentifiers.isEmpty()) {
                throw new IllegalArgumentException("arrayFilters must be specified for path '" + fieldPath + "'");
            }
            if (!arrayFilterIdentifiers.contains(identifier)) {
                throw new IllegalArgumentException("no array filter found for identifier '" + identifier + "'");
            }
        }
        return segments;
    }

    private static String leaf(final String path) {
        final String[] segments = parsePath(path);
        return segments[segments.length - 1];
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(final Map<?, ?> value) {
        return (Map<String, Object>) value;
    }

    private static boolean pathContainsArrayFilter(final String path) {
        if (path == null) {
            return false;
        }
        for (final String segment : path.split("\\.")) {
            if (isArrayFilterSegment(segment)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isArrayFilterSegment(final String segment) {
        return segment.startsWith("$[") && segment.endsWith("]");
    }

    private static String arrayFilterIdentifier(final String segment) {
        if (!isArrayFilterSegment(segment) || segment.length() <= 3) {
            return "";
        }
        return segment.substring(2, segment.length() - 1);
    }

    private static boolean isValidArrayFilterIdentifier(final String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }
        final char first = identifier.charAt(0);
        if (!(Character.isLetter(first) || first == '_')) {
            return false;
        }
        for (int i = 1; i < identifier.length(); i++) {
            final char c = identifier.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesArrayFilter(final Object candidate, final Document filter) {
        if (filter == null) {
            return false;
        }
        if (candidate instanceof Document documentCandidate) {
            return QueryMatcher.matches(documentCandidate, filter);
        }
        if (!(candidate instanceof Map<?, ?> mapCandidate)) {
            return false;
        }

        final Document wrappedCandidate = new Document();
        for (final Map.Entry<?, ?> entry : mapCandidate.entrySet()) {
            if (entry.getKey() instanceof String key) {
                wrappedCandidate.put(key, entry.getValue());
            }
        }
        return QueryMatcher.matches(wrappedCandidate, filter);
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
        private final List<SetOnInsertOperation> setOnInsertOperations;
        private final List<IncOperation> incrementOperations;
        private final List<String> unsetOperations;
        private final List<AddToSetOperation> addToSetOperations;
        private final Document replacementDocument;
        private final ArrayFilterBindings arrayFilterBindings;

        private ParsedUpdate(
                final List<SetOperation> setOperations,
                final List<SetOnInsertOperation> setOnInsertOperations,
                final List<IncOperation> incrementOperations,
                final List<String> unsetOperations,
                final List<AddToSetOperation> addToSetOperations,
                final Document replacementDocument,
                final ArrayFilterBindings arrayFilterBindings) {
            this.setOperations = List.copyOf(setOperations);
            this.setOnInsertOperations = List.copyOf(setOnInsertOperations);
            this.incrementOperations = List.copyOf(incrementOperations);
            this.unsetOperations = List.copyOf(unsetOperations);
            this.addToSetOperations = List.copyOf(addToSetOperations);
            this.replacementDocument = replacementDocument == null ? null : DocumentCopies.copy(replacementDocument);
            this.arrayFilterBindings = Objects.requireNonNull(arrayFilterBindings, "arrayFilterBindings");
        }

        static ParsedUpdate operator(
                final List<SetOperation> setOperations,
                final List<SetOnInsertOperation> setOnInsertOperations,
                final List<IncOperation> incrementOperations,
                final List<String> unsetOperations,
                final List<AddToSetOperation> addToSetOperations,
                final ArrayFilterBindings arrayFilterBindings) {
            return new ParsedUpdate(
                    setOperations,
                    setOnInsertOperations,
                    incrementOperations,
                    unsetOperations,
                    addToSetOperations,
                    null,
                    arrayFilterBindings);
        }

        static ParsedUpdate replacement(final Document replacementDocument) {
            return new ParsedUpdate(
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    replacementDocument,
                    ArrayFilterBindings.empty());
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

        List<SetOnInsertOperation> setOnInsertOperations() {
            return Collections.unmodifiableList(setOnInsertOperations);
        }

        List<IncOperation> incrementOperations() {
            return Collections.unmodifiableList(incrementOperations);
        }

        List<String> unsetOperations() {
            return Collections.unmodifiableList(unsetOperations);
        }

        List<AddToSetOperation> addToSetOperations() {
            return Collections.unmodifiableList(addToSetOperations);
        }

        ArrayFilterBindings arrayFilterBindings() {
            return arrayFilterBindings;
        }
    }

    private static final class ArrayFilterBindings {
        private static final ArrayFilterBindings EMPTY = new ArrayFilterBindings(Map.of());

        private final Map<String, Document> bindings;

        private ArrayFilterBindings(final Map<String, Document> bindings) {
            this.bindings = Map.copyOf(bindings);
        }

        static ArrayFilterBindings empty() {
            return EMPTY;
        }

        static ArrayFilterBindings parse(final List<Document> arrayFilters) {
            if (arrayFilters == null || arrayFilters.isEmpty()) {
                return EMPTY;
            }

            final Map<String, Document> parsed = new LinkedHashMap<>();
            for (final Document arrayFilter : arrayFilters) {
                if (arrayFilter == null || arrayFilter.isEmpty()) {
                    throw new IllegalArgumentException("arrayFilters entries must be non-empty documents");
                }

                String identifier = null;
                final Document normalizedFilter = new Document();
                for (final Map.Entry<String, Object> entry : arrayFilter.entrySet()) {
                    final String key = entry.getKey();
                    final int separator = key.indexOf('.');
                    if (separator <= 0 || separator == key.length() - 1) {
                        throw new IllegalArgumentException(
                                "arrayFilters entries must use '<identifier>.<fieldPath>' keys");
                    }

                    final String candidateIdentifier = key.substring(0, separator);
                    if (!isValidArrayFilterIdentifier(candidateIdentifier)) {
                        throw new IllegalArgumentException(
                                "invalid array filter identifier '" + candidateIdentifier + "'");
                    }
                    if (identifier == null) {
                        identifier = candidateIdentifier;
                    } else if (!identifier.equals(candidateIdentifier)) {
                        throw new IllegalArgumentException(
                                "arrayFilters entries must reference a single identifier");
                    }

                    final String fieldPath = key.substring(separator + 1);
                    parsePath(fieldPath);
                    normalizedFilter.put(fieldPath, DocumentCopies.copyAny(entry.getValue()));
                }

                if (identifier == null || normalizedFilter.isEmpty()) {
                    throw new IllegalArgumentException("arrayFilters entries must not be empty");
                }
                if (parsed.containsKey(identifier)) {
                    throw new IllegalArgumentException("duplicate array filter identifier '" + identifier + "'");
                }
                parsed.put(identifier, normalizedFilter);
            }
            return new ArrayFilterBindings(parsed);
        }

        boolean isEmpty() {
            return bindings.isEmpty();
        }

        Set<String> identifiers() {
            return bindings.keySet();
        }

        Document filterFor(final String identifier) {
            final Document filter = bindings.get(identifier);
            if (filter == null) {
                throw new IllegalArgumentException("no array filter found for identifier '" + identifier + "'");
            }
            return filter;
        }
    }

    private record SetOperation(String path, Object value) {}

    private record SetOnInsertOperation(String path, Object value) {}

    private record IncOperation(String path, Number delta) {}

    private record AddToSetOperation(String path, List<Object> values) {}

    private record PathLookup(boolean exists, Object value) {
        static PathLookup missing() {
            return new PathLookup(false, null);
        }

        static PathLookup existing(final Object value) {
            return new PathLookup(true, value);
        }
    }
}
