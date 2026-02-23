package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Renders differential reports in markdown and JSON.
 */
public final class DiffSummaryGenerator {
    public String toMarkdown(DifferentialReport report) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Differential Report\n\n");
        sb.append("- generatedAt: ").append(report.generatedAt()).append('\n');
        sb.append("- backends: ").append(report.leftBackend()).append(" vs ").append(report.rightBackend()).append('\n');
        sb.append("- total: ").append(report.totalScenarios()).append('\n');
        sb.append("- match: ").append(report.matchCount()).append('\n');
        sb.append("- mismatch: ").append(report.mismatchCount()).append('\n');
        sb.append("- error: ").append(report.errorCount()).append("\n\n");

        for (DiffResult result : report.results()) {
            sb.append("## ").append(result.scenarioId()).append(" (").append(result.status()).append(")\n");
            if (result.status() == DiffStatus.MATCH) {
                sb.append("- No material differences\n\n");
                continue;
            }
            if (result.status() == DiffStatus.ERROR) {
                sb.append("- Error: ").append(result.errorMessage().orElse("unknown error")).append("\n\n");
                continue;
            }
            for (DiffEntry entry : result.entries()) {
                sb.append("- `").append(entry.path()).append("`: ")
                    .append(stringify(entry.leftValue()))
                    .append(" <> ")
                    .append(stringify(entry.rightValue()));
                if (!entry.note().isEmpty()) {
                    sb.append(" (").append(entry.note()).append(')');
                }
                sb.append('\n');
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    public String toJson(DifferentialReport report) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", report.generatedAt().toString());
        root.put("leftBackend", report.leftBackend());
        root.put("rightBackend", report.rightBackend());

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total", report.totalScenarios());
        summary.put("match", report.matchCount());
        summary.put("mismatch", report.mismatchCount());
        summary.put("error", report.errorCount());
        root.put("summary", summary);

        List<Map<String, Object>> resultItems = new ArrayList<>();
        for (DiffResult result : report.results()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("scenarioId", result.scenarioId());
            item.put("status", result.status().name());
            item.put("leftBackend", result.leftBackend());
            item.put("rightBackend", result.rightBackend());
            item.put("errorMessage", result.errorMessage().orElse(null));

            List<Map<String, Object>> entryItems = new ArrayList<>();
            for (DiffEntry entry : result.entries()) {
                Map<String, Object> entryItem = new LinkedHashMap<>();
                entryItem.put("path", entry.path());
                entryItem.put("leftValue", entry.leftValue());
                entryItem.put("rightValue", entry.rightValue());
                entryItem.put("note", entry.note());
                entryItems.add(entryItem);
            }
            item.put("entries", entryItems);
            resultItems.add(item);
        }
        root.put("results", resultItems);
        return JsonEncoder.encode(root);
    }

    private static String stringify(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String s) {
            return "\"" + s + "\"";
        }
        return value.toString();
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
            appendString(sb, String.valueOf(value));
        }

        private static void appendObject(StringBuilder sb, Map<Object, Object> map) {
            sb.append('{');
            boolean first = true;
            Map<String, Object> normalized = normalizeKeyMap(map);
            TreeSet<String> keys = new TreeSet<>(normalized.keySet());
            for (String key : keys) {
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

        private static void appendArray(StringBuilder sb, Collection<?> values) {
            sb.append('[');
            boolean first = true;
            for (Object value : values) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendValue(sb, value);
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
