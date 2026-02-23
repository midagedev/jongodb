package org.jongodb.engine;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

final class QueryMatcher {
    private QueryMatcher() {}

    static boolean matches(Document document, Document filter) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(filter, "filter");

        for (Map.Entry<String, Object> criteria : filter.entrySet()) {
            Object actualValue = resolvePathValue(document, criteria.getKey());
            if (!matchesValue(actualValue, criteria.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static Object resolvePathValue(Document document, String key) {
        if (key == null || key.isEmpty()) {
            return null;
        }

        if (!key.contains(".")) {
            return document.get(key);
        }

        String[] segments = key.split("\\.");
        Object current = document;
        for (String segment : segments) {
            if (!(current instanceof Map<?, ?>)) {
                return null;
            }
            current = ((Map<?, ?>) current).get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static boolean matchesValue(Object actualValue, Object expectedValue) {
        if (actualValue instanceof List<?> && isSimpleScalar(expectedValue)) {
            for (Object item : (List<?>) actualValue) {
                if (valueEquals(item, expectedValue)) {
                    return true;
                }
            }
            return false;
        }
        return valueEquals(actualValue, expectedValue);
    }

    private static boolean isSimpleScalar(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            return false;
        }
        return !value.getClass().isArray();
    }

    private static boolean valueEquals(Object left, Object right) {
        return Objects.deepEquals(left, right);
    }
}
