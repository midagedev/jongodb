package org.jongodb.testkit;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.bson.Document;

/**
 * Drift metric analyzer for fixture refresh outputs.
 */
final class FixtureDriftAnalyzer {
    static final String REPORT_JSON = "fixture-drift-report.json";
    static final String REPORT_MD = "fixture-drift-report.md";

    private FixtureDriftAnalyzer() {}

    static DriftReport analyze(
            final Map<String, List<Document>> baseline,
            final Map<String, List<Document>> candidate,
            final double warnThreshold,
            final double failThreshold) {
        final Set<String> union = new LinkedHashSet<>();
        union.addAll(baseline.keySet());
        union.addAll(candidate.keySet());

        final List<String> namespaces = new ArrayList<>(union);
        namespaces.sort(String::compareTo);

        final List<CollectionDrift> drifts = new ArrayList<>(namespaces.size());
        for (final String namespace : namespaces) {
            final List<Document> baselineDocs = baseline.getOrDefault(namespace, List.of());
            final List<Document> candidateDocs = candidate.getOrDefault(namespace, List.of());

            final double rowCountDelta = ratioDelta(candidateDocs.size(), baselineDocs.size());
            final Set<String> fieldSet = topLevelFieldUnion(baselineDocs, candidateDocs);
            final List<String> fields = new ArrayList<>(fieldSet);
            fields.sort(String::compareTo);

            final List<FieldDrift> fieldDrifts = new ArrayList<>(fields.size());
            for (final String field : fields) {
                final FieldStats before = fieldStats(baselineDocs, field);
                final FieldStats after = fieldStats(candidateDocs, field);
                final double nullRatioDelta = round4(Math.abs(after.nullRatio() - before.nullRatio()));
                final double cardinalityDelta = round4(ratioDelta(after.cardinality(), before.cardinality()));
                final double distributionDelta = round4(distributionDelta(before.distribution(), after.distribution()));

                fieldDrifts.add(new FieldDrift(
                        field,
                        before.nullRatio(),
                        after.nullRatio(),
                        nullRatioDelta,
                        before.cardinality(),
                        after.cardinality(),
                        cardinalityDelta,
                        distributionDelta));
            }

            final double avgNull = round4(average(fieldDrifts.stream().map(FieldDrift::nullRatioDelta).toList()));
            final double avgCardinality = round4(average(fieldDrifts.stream().map(FieldDrift::cardinalityDelta).toList()));
            final double maxDistribution = round4(max(fieldDrifts.stream().map(FieldDrift::distributionDelta).toList()));

            final double score = round4((rowCountDelta * 0.40)
                    + (avgNull * 0.20)
                    + (avgCardinality * 0.20)
                    + (maxDistribution * 0.20));

            final DriftStatus status = statusFor(score, warnThreshold, failThreshold);
            final List<FieldDrift> topFields = fieldDrifts.stream()
                    .sorted(Comparator
                            .comparing(FieldDrift::distributionDelta)
                            .thenComparing(FieldDrift::nullRatioDelta)
                            .thenComparing(FieldDrift::cardinalityDelta)
                            .reversed())
                    .limit(3)
                    .toList();

            drifts.add(new CollectionDrift(
                    namespace,
                    baselineDocs.size(),
                    candidateDocs.size(),
                    rowCountDelta,
                    avgNull,
                    avgCardinality,
                    maxDistribution,
                    score,
                    status,
                    topFields,
                    fieldDrifts));
        }

        final List<CollectionDrift> ranked = drifts.stream()
                .sorted(Comparator.comparing(CollectionDrift::score).reversed())
                .toList();

        return new DriftReport(
                warnThreshold,
                failThreshold,
                ranked,
                (int) ranked.stream().filter(item -> item.status() == DriftStatus.WARN).count(),
                (int) ranked.stream().filter(item -> item.status() == DriftStatus.FAIL).count());
    }

    private static Set<String> topLevelFieldUnion(
            final List<Document> baselineDocs,
            final List<Document> candidateDocs) {
        final Set<String> fields = new LinkedHashSet<>();
        for (final Document document : baselineDocs) {
            fields.addAll(document.keySet());
        }
        for (final Document document : candidateDocs) {
            fields.addAll(document.keySet());
        }
        return fields;
    }

    private static FieldStats fieldStats(final List<Document> docs, final String field) {
        final Map<String, Integer> distribution = new LinkedHashMap<>();
        int nullCount = 0;
        for (final Document document : docs) {
            if (!document.containsKey(field) || document.get(field) == null) {
                nullCount++;
                continue;
            }
            final String scalar = canonicalScalar(document.get(field));
            distribution.put(scalar, distribution.getOrDefault(scalar, 0) + 1);
        }

        final double nullRatio = docs.isEmpty() ? 0d : round4((double) nullCount / (double) docs.size());
        return new FieldStats(nullRatio, distribution.size(), Map.copyOf(distribution));
    }

    private static double distributionDelta(
            final Map<String, Integer> baseline,
            final Map<String, Integer> candidate) {
        final Set<String> union = new LinkedHashSet<>();
        union.addAll(baseline.keySet());
        union.addAll(candidate.keySet());

        final int baselineTotal = baseline.values().stream().mapToInt(Integer::intValue).sum();
        final int candidateTotal = candidate.values().stream().mapToInt(Integer::intValue).sum();
        if (baselineTotal == 0 && candidateTotal == 0) {
            return 0d;
        }

        double tvd = 0d;
        for (final String key : union) {
            final double before = baselineTotal == 0
                    ? 0d
                    : (double) baseline.getOrDefault(key, 0) / (double) baselineTotal;
            final double after = candidateTotal == 0
                    ? 0d
                    : (double) candidate.getOrDefault(key, 0) / (double) candidateTotal;
            tvd += Math.abs(before - after);
        }
        return tvd * 0.5d;
    }

    private static String canonicalScalar(final Object value) {
        if (value instanceof String stringValue) {
            return "str:" + stringValue;
        }
        if (value instanceof Number numberValue) {
            return "num:" + numberValue;
        }
        if (value instanceof Boolean booleanValue) {
            return "bool:" + booleanValue;
        }
        if (value instanceof Document documentValue) {
            return "doc:" + DiffSummaryGenerator.JsonEncoder.encode(canonicalizeValue(documentValue));
        }
        if (value instanceof List<?> listValue) {
            return "arr:" + DiffSummaryGenerator.JsonEncoder.encode(canonicalizeValue(listValue));
        }
        return "obj:" + String.valueOf(value);
    }

    private static Object canonicalizeValue(final Object value) {
        if (value instanceof Document document) {
            final Map<String, Object> sorted = new TreeMap<>();
            for (final Map.Entry<String, Object> entry : document.entrySet()) {
                sorted.put(entry.getKey(), canonicalizeValue(entry.getValue()));
            }
            final Map<String, Object> normalized = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : sorted.entrySet()) {
                normalized.put(entry.getKey(), entry.getValue());
            }
            return normalized;
        }
        if (value instanceof Map<?, ?> map) {
            final Map<String, Object> sorted = new TreeMap<>();
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() instanceof String key) {
                    sorted.put(key, canonicalizeValue(entry.getValue()));
                }
            }
            final Map<String, Object> normalized = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : sorted.entrySet()) {
                normalized.put(entry.getKey(), entry.getValue());
            }
            return normalized;
        }
        if (value instanceof List<?> list) {
            final List<Object> normalized = new ArrayList<>(list.size());
            for (final Object item : list) {
                normalized.add(canonicalizeValue(item));
            }
            return normalized;
        }
        return value;
    }

    private static double ratioDelta(final int current, final int baseline) {
        final int denominator = Math.max(1, baseline);
        return round4(Math.abs(current - baseline) / (double) denominator);
    }

    private static double average(final List<Double> values) {
        if (values.isEmpty()) {
            return 0d;
        }
        double sum = 0d;
        for (final double value : values) {
            sum += value;
        }
        return sum / (double) values.size();
    }

    private static double max(final List<Double> values) {
        if (values.isEmpty()) {
            return 0d;
        }
        double max = 0d;
        for (final double value : values) {
            if (value > max) {
                max = value;
            }
        }
        return max;
    }

    private static double round4(final double value) {
        return BigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_UP).doubleValue();
    }

    private static DriftStatus statusFor(
            final double score,
            final double warnThreshold,
            final double failThreshold) {
        if (score >= failThreshold) {
            return DriftStatus.FAIL;
        }
        if (score >= warnThreshold) {
            return DriftStatus.WARN;
        }
        return DriftStatus.OK;
    }

    enum DriftStatus {
        OK,
        WARN,
        FAIL
    }

    record DriftReport(
            double warnThreshold,
            double failThreshold,
            List<CollectionDrift> collections,
            int warningCollections,
            int failingCollections) {
        boolean hasFailures() {
            return failingCollections > 0;
        }

        String toJson() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("warnThreshold", warnThreshold);
            root.put("failThreshold", failThreshold);
            root.put("warningCollections", warningCollections);
            root.put("failingCollections", failingCollections);
            root.put("hasFailures", hasFailures());

            final List<Map<String, Object>> items = new ArrayList<>(collections.size());
            for (final CollectionDrift collection : collections) {
                items.add(collection.toMap());
            }
            root.put("collections", items);
            return DiffSummaryGenerator.JsonEncoder.encode(root);
        }

        String toMarkdown() {
            final StringBuilder sb = new StringBuilder();
            sb.append("# Fixture Drift Report\n\n");
            sb.append("- warnThreshold: ").append(warnThreshold).append("\n");
            sb.append("- failThreshold: ").append(failThreshold).append("\n");
            sb.append("- warningCollections: ").append(warningCollections).append("\n");
            sb.append("- failingCollections: ").append(failingCollections).append("\n\n");

            sb.append("| rank | namespace | score | status | rowDelta | nullDelta | cardDelta | distDelta |\n");
            sb.append("|---:|---|---:|---|---:|---:|---:|---:|\n");
            int rank = 1;
            for (final CollectionDrift item : collections) {
                sb.append("| ").append(rank++)
                        .append(" | ").append(item.namespace())
                        .append(" | ").append(item.score())
                        .append(" | ").append(item.status().name().toLowerCase(Locale.ROOT))
                        .append(" | ").append(item.rowCountDelta())
                        .append(" | ").append(item.avgNullRatioDelta())
                        .append(" | ").append(item.avgCardinalityDelta())
                        .append(" | ").append(item.maxDistributionDelta())
                        .append(" |\n");
            }

            for (final CollectionDrift item : collections) {
                if (item.topFields().isEmpty()) {
                    continue;
                }
                sb.append("\n## ").append(item.namespace()).append(" (Top Field Drift)\n");
                for (final FieldDrift field : item.topFields()) {
                    sb.append("- ").append(field.field())
                            .append(": nullDelta=").append(field.nullRatioDelta())
                            .append(", cardDelta=").append(field.cardinalityDelta())
                            .append(", distDelta=").append(field.distributionDelta())
                            .append("\n");
                }
            }
            return sb.toString();
        }
    }

    record CollectionDrift(
            String namespace,
            int baselineCount,
            int candidateCount,
            double rowCountDelta,
            double avgNullRatioDelta,
            double avgCardinalityDelta,
            double maxDistributionDelta,
            double score,
            DriftStatus status,
            List<FieldDrift> topFields,
            List<FieldDrift> fields) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("namespace", namespace);
            root.put("baselineCount", baselineCount);
            root.put("candidateCount", candidateCount);
            root.put("rowCountDelta", rowCountDelta);
            root.put("avgNullRatioDelta", avgNullRatioDelta);
            root.put("avgCardinalityDelta", avgCardinalityDelta);
            root.put("maxDistributionDelta", maxDistributionDelta);
            root.put("score", score);
            root.put("status", status.name().toLowerCase(Locale.ROOT));

            final List<Map<String, Object>> topFieldItems = new ArrayList<>(topFields.size());
            for (final FieldDrift item : topFields) {
                topFieldItems.add(item.toMap());
            }
            root.put("topFields", topFieldItems);

            final List<Map<String, Object>> fieldItems = new ArrayList<>(fields.size());
            for (final FieldDrift item : fields) {
                fieldItems.add(item.toMap());
            }
            root.put("fields", fieldItems);
            return root;
        }
    }

    record FieldDrift(
            String field,
            double baselineNullRatio,
            double candidateNullRatio,
            double nullRatioDelta,
            int baselineCardinality,
            int candidateCardinality,
            double cardinalityDelta,
            double distributionDelta) {
        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("field", field);
            root.put("baselineNullRatio", baselineNullRatio);
            root.put("candidateNullRatio", candidateNullRatio);
            root.put("nullRatioDelta", nullRatioDelta);
            root.put("baselineCardinality", baselineCardinality);
            root.put("candidateCardinality", candidateCardinality);
            root.put("cardinalityDelta", cardinalityDelta);
            root.put("distributionDelta", distributionDelta);
            return root;
        }
    }

    private record FieldStats(
            double nullRatio,
            int cardinality,
            Map<String, Integer> distribution) {}
}
