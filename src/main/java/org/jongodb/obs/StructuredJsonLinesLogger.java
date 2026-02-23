package org.jongodb.obs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * JSON-lines logger intended for deterministic diagnostics.
 */
public final class StructuredJsonLinesLogger implements JsonLinesLogger {
    private final Writer writer;
    private final Clock clock;
    private final boolean autoFlush;
    private boolean closed;

    public StructuredJsonLinesLogger(OutputStream outputStream) {
        this(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), Clock.systemUTC(), true);
    }

    public StructuredJsonLinesLogger(OutputStream outputStream, Clock clock, boolean autoFlush) {
        this(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), clock, autoFlush);
    }

    public StructuredJsonLinesLogger(Writer writer, Clock clock, boolean autoFlush) {
        this.writer = Objects.requireNonNull(writer, "writer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.autoFlush = autoFlush;
        this.closed = false;
    }

    @Override
    public synchronized void log(
        String level,
        String message,
        CorrelationContext correlationContext,
        Map<String, ?> fields
    ) {
        ensureOpen();
        String safeLevel = normalizeLevel(level);
        String safeMessage = message == null ? "" : message;
        CorrelationContext safeCorrelation = Objects.requireNonNull(correlationContext, "correlationContext");
        Map<String, ?> safeFields = fields == null ? Map.of() : fields;

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("timestamp", Instant.now(clock).toString());
        event.put("level", safeLevel);
        event.put("message", safeMessage);
        event.putAll(safeCorrelation.asFields());
        for (Map.Entry<String, ?> entry : safeFields.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isBlank() || event.containsKey(key)) {
                continue;
            }
            event.put(key, entry.getValue());
        }

        writeLine(JsonEncoder.encode(event));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close logger writer", e);
        }
    }

    private void writeLine(String encoded) {
        try {
            writer.write(encoded);
            writer.write('\n');
            if (autoFlush) {
                writer.flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write log event", e);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("logger is already closed");
        }
    }

    private static String normalizeLevel(String level) {
        if (level == null || level.isBlank()) {
            return "INFO";
        }
        return level.trim().toUpperCase();
    }

    static final class JsonEncoder {
        private JsonEncoder() {
        }

        static String encode(Object value) {
            StringBuilder sb = new StringBuilder();
            appendValue(sb, value);
            return sb.toString();
        }

        @SuppressWarnings("unchecked")
        private static void appendValue(StringBuilder sb, Object value) {
            if (value == null) {
                sb.append("null");
                return;
            }
            if (value instanceof String s) {
                appendString(sb, s);
                return;
            }
            if (value instanceof Number || value instanceof Boolean) {
                sb.append(value);
                return;
            }
            if (value instanceof Map<?, ?> map) {
                appendObject(sb, (Map<Object, Object>) map);
                return;
            }
            if (value instanceof Collection<?> collection) {
                appendArray(sb, collection);
                return;
            }
            if (value.getClass().isArray()) {
                int length = Array.getLength(value);
                List<Object> boxed = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    boxed.add(Array.get(value, i));
                }
                appendArray(sb, boxed);
                return;
            }
            appendString(sb, String.valueOf(value));
        }

        private static void appendObject(StringBuilder sb, Map<Object, Object> map) {
            sb.append('{');
            boolean first = true;
            Map<String, Object> normalized = normalizeKeyMap(map);
            for (String key : sortedKeys(normalized)) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendString(sb, key);
                sb.append(':');
                appendValue(sb, normalized.get(key));
            }
            sb.append('}');
        }

        private static List<String> sortedKeys(Map<String, Object> map) {
            TreeSet<String> keys = new TreeSet<>();
            keys.addAll(map.keySet());
            return new ArrayList<>(keys);
        }

        private static void appendArray(StringBuilder sb, Collection<?> list) {
            sb.append('[');
            boolean first = true;
            for (Object item : list) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendValue(sb, item);
            }
            sb.append(']');
        }

        private static void appendString(StringBuilder sb, String value) {
            sb.append('"');
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '"' -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\b' -> sb.append("\\b");
                    case '\f' -> sb.append("\\f");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default -> {
                        if (c <= 0x1F) {
                            sb.append(String.format("\\u%04x", (int) c));
                        } else {
                            sb.append(c);
                        }
                    }
                }
            }
            sb.append('"');
        }

        private static Map<String, Object> normalizeKeyMap(Map<Object, Object> source) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<Object, Object> entry : source.entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return normalized;
        }
    }
}
