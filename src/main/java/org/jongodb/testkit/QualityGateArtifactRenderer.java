package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Renders quality gate reports as CI-friendly markdown and JSON artifacts.
 */
public final class QualityGateArtifactRenderer {
    public String toMarkdown(QualityGateReport report) {
        Objects.requireNonNull(report, "report");
        StringBuilder sb = new StringBuilder();
        sb.append("# Quality Gate Report\n\n");
        sb.append("- generatedAt: ").append(report.generatedAt()).append('\n');
        sb.append("- overall: ").append(report.overallPassed() ? "PASS" : "FAIL").append('\n');
        sb.append("- pass: ").append(report.passCount()).append('\n');
        sb.append("- fail: ").append(report.failCount()).append("\n\n");

        sb.append("## Metrics\n");
        appendMetricLine(sb, QualityGateMetric.COMPATIBILITY_PASS_RATE, report.metrics().compatibilityPassRate());
        appendMetricLine(sb, QualityGateMetric.FLAKE_RATE, report.metrics().flakeRate());
        appendMetricLine(sb, QualityGateMetric.P95_LATENCY_MILLIS, report.metrics().p95LatencyMillis());
        appendMetricLine(sb, QualityGateMetric.REPRO_TIME_P50_MINUTES, report.metrics().reproTimeP50Minutes());
        sb.append('\n');

        sb.append("## Gates\n");
        for (QualityGateResult gateResult : report.gateResults()) {
            sb.append("- ")
                .append(gateResult.gateId())
                .append(": ")
                .append(gateResult.status())
                .append(" (")
                .append(gateResult.metric().key())
                .append(' ')
                .append(formatMetricValue(gateResult.metric(), gateResult.measuredValue()))
                .append(' ')
                .append(gateResult.operator().symbol())
                .append(' ')
                .append(formatMetricValue(gateResult.metric(), gateResult.thresholdValue()))
                .append(")\n");
        }
        return sb.toString();
    }

    public String toJson(QualityGateReport report) {
        Objects.requireNonNull(report, "report");
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", report.generatedAt().toString());
        root.put("overallStatus", report.overallPassed() ? "PASS" : "FAIL");

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("pass", report.passCount());
        summary.put("fail", report.failCount());
        root.put("summary", summary);

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put(QualityGateMetric.COMPATIBILITY_PASS_RATE.key(), report.metrics().compatibilityPassRate());
        metrics.put(QualityGateMetric.FLAKE_RATE.key(), report.metrics().flakeRate());
        metrics.put(QualityGateMetric.P95_LATENCY_MILLIS.key(), report.metrics().p95LatencyMillis());
        metrics.put(QualityGateMetric.REPRO_TIME_P50_MINUTES.key(), report.metrics().reproTimeP50Minutes());
        root.put("metrics", metrics);

        List<Map<String, Object>> gates = new ArrayList<>();
        for (QualityGateResult gateResult : report.gateResults()) {
            Map<String, Object> gate = new LinkedHashMap<>();
            gate.put("gateId", gateResult.gateId());
            gate.put("metric", gateResult.metric().name());
            gate.put("metricKey", gateResult.metric().key());
            gate.put("measuredValue", gateResult.measuredValue());
            gate.put("operator", gateResult.operator().symbol());
            gate.put("thresholdValue", gateResult.thresholdValue());
            gate.put("status", gateResult.status().name());
            gates.add(gate);
        }
        root.put("gates", gates);

        return JsonEncoder.encode(root);
    }

    private static void appendMetricLine(StringBuilder sb, QualityGateMetric metric, double value) {
        sb.append("- ")
            .append(metric.key())
            .append(": ")
            .append(formatMetricValue(metric, value))
            .append('\n');
    }

    private static String formatMetricValue(QualityGateMetric metric, double value) {
        return switch (metric) {
            case COMPATIBILITY_PASS_RATE, FLAKE_RATE -> String.format(Locale.ROOT, "%.2f%%", value * 100.0d);
            case P95_LATENCY_MILLIS -> String.format(Locale.ROOT, "%.2fms", value);
            case REPRO_TIME_P50_MINUTES -> String.format(Locale.ROOT, "%.2fmin", value);
        };
    }

    static final class JsonEncoder {
        private JsonEncoder() {
        }

        static String encode(Object value) {
            StringBuilder sb = new StringBuilder();
            appendValue(sb, value);
            return sb.toString();
        }

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
                appendObject(sb, map);
                return;
            }
            if (value instanceof Collection<?> collection) {
                appendArray(sb, collection);
                return;
            }
            appendString(sb, String.valueOf(value));
        }

        private static void appendObject(StringBuilder sb, Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendString(sb, String.valueOf(entry.getKey()));
                sb.append(':');
                appendValue(sb, entry.getValue());
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
                            sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                        } else {
                            sb.append(c);
                        }
                    }
                }
            }
            sb.append('"');
        }
    }
}
