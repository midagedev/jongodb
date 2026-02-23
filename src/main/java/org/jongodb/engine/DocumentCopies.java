package org.jongodb.engine;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

final class DocumentCopies {
    private DocumentCopies() {}

    static Document copy(Document source) {
        Objects.requireNonNull(source, "source");

        Document copy = new Document();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            copy.put(entry.getKey(), copyValue(entry.getValue()));
        }
        return copy;
    }

    private static Object copyValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Document) {
            return copy((Document) value);
        }
        if (value instanceof List<?>) {
            List<?> input = (List<?>) value;
            List<Object> output = new ArrayList<>(input.size());
            for (Object item : input) {
                output.add(copyValue(item));
            }
            return output;
        }
        if (value instanceof Map<?, ?>) {
            Map<?, ?> input = (Map<?, ?>) value;
            Map<Object, Object> output = new LinkedHashMap<>(input.size());
            for (Map.Entry<?, ?> entry : input.entrySet()) {
                output.put(entry.getKey(), copyValue(entry.getValue()));
            }
            return output;
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof Date) {
            return new Date(((Date) value).getTime());
        }
        if (value instanceof int[]) {
            return ((int[]) value).clone();
        }
        if (value instanceof long[]) {
            return ((long[]) value).clone();
        }
        if (value instanceof double[]) {
            return ((double[]) value).clone();
        }
        if (value instanceof float[]) {
            return ((float[]) value).clone();
        }
        if (value instanceof short[]) {
            return ((short[]) value).clone();
        }
        if (value instanceof char[]) {
            return ((char[]) value).clone();
        }
        if (value instanceof boolean[]) {
            return ((boolean[]) value).clone();
        }
        if (value instanceof Object[]) {
            Object[] input = (Object[]) value;
            Object[] output = new Object[input.length];
            for (int i = 0; i < input.length; i++) {
                output[i] = copyValue(input[i]);
            }
            return output;
        }
        return value;
    }
}
