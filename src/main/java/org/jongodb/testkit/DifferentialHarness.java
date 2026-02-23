package org.jongodb.testkit;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs scenarios against two backends and computes structural diffs.
 */
public final class DifferentialHarness {
    private static final Pattern FAILURE_CODE_PATTERN = Pattern.compile(
        "\\(code=([-]?[0-9]+)(?:,\\s*codeName=([^\\)]+))?\\)\\s*$"
    );

    private final DifferentialBackend leftBackend;
    private final DifferentialBackend rightBackend;
    private final Clock clock;

    public DifferentialHarness(DifferentialBackend leftBackend, DifferentialBackend rightBackend) {
        this(leftBackend, rightBackend, Clock.systemUTC());
    }

    public DifferentialHarness(DifferentialBackend leftBackend, DifferentialBackend rightBackend, Clock clock) {
        this.leftBackend = Objects.requireNonNull(leftBackend, "leftBackend");
        this.rightBackend = Objects.requireNonNull(rightBackend, "rightBackend");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public DifferentialReport run(List<Scenario> scenarios) {
        Objects.requireNonNull(scenarios, "scenarios");
        List<DiffResult> results = new ArrayList<>(scenarios.size());
        for (Scenario scenario : scenarios) {
            results.add(runScenario(scenario));
        }
        return new DifferentialReport(
            clock.instant(),
            leftBackend.name(),
            rightBackend.name(),
            results
        );
    }

    public DiffResult runScenario(Scenario scenario) {
        Objects.requireNonNull(scenario, "scenario");
        ScenarioOutcome leftOutcome;
        ScenarioOutcome rightOutcome;
        try {
            leftOutcome = leftBackend.execute(scenario);
            rightOutcome = rightBackend.execute(scenario);
        } catch (Exception e) {
            return DiffResult.error(
                scenario.id(),
                leftBackend.name(),
                rightBackend.name(),
                e.getClass().getSimpleName() + ": " + e.getMessage()
            );
        }

        List<DiffEntry> entries = compareOutcomes(leftOutcome, rightOutcome);
        if (entries.isEmpty()) {
            return DiffResult.match(scenario.id(), leftBackend.name(), rightBackend.name());
        }
        return DiffResult.mismatch(scenario.id(), leftBackend.name(), rightBackend.name(), entries);
    }

    private static List<DiffEntry> compareOutcomes(ScenarioOutcome leftOutcome, ScenarioOutcome rightOutcome) {
        Objects.requireNonNull(leftOutcome, "leftOutcome");
        Objects.requireNonNull(rightOutcome, "rightOutcome");
        List<DiffEntry> entries = new ArrayList<>();
        compareValue("$.success", leftOutcome.success(), rightOutcome.success(), entries);
        if (leftOutcome.success() && rightOutcome.success()) {
            compareValue(
                "$.commandResults",
                normalizeForComparison(leftOutcome.commandResults()),
                normalizeForComparison(rightOutcome.commandResults()),
                entries
            );
            return entries;
        }
        if (!leftOutcome.success() && !rightOutcome.success()) {
            String leftMessage = leftOutcome.errorMessage().orElse(null);
            String rightMessage = rightOutcome.errorMessage().orElse(null);
            if (failureSemanticallyEqual(leftMessage, rightMessage)) {
                return entries;
            }
        }
        compareValue(
            "$.errorMessage",
            leftOutcome.errorMessage().orElse(null),
            rightOutcome.errorMessage().orElse(null),
            entries
        );
        return entries;
    }

    private static Object normalizeForComparison(Object value) {
        if (value instanceof Map<?, ?> valueMap) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
                String key = String.valueOf(entry.getKey());
                if (isEphemeralMetadataKey(key)) {
                    continue;
                }
                normalized.put(key, normalizeForComparison(entry.getValue()));
            }
            return normalized;
        }
        if (value instanceof List<?> valueList) {
            List<Object> normalized = new ArrayList<>(valueList.size());
            for (Object item : valueList) {
                normalized.add(normalizeForComparison(item));
            }
            return normalized;
        }
        return value;
    }

    private static boolean isEphemeralMetadataKey(String key) {
        return "$clusterTime".equals(key)
            || "operationTime".equals(key)
            || "electionId".equals(key)
            || "opTime".equals(key);
    }

    private static boolean failureSemanticallyEqual(String leftMessage, String rightMessage) {
        FailureSignature leftSignature = FailureSignature.parse(leftMessage);
        FailureSignature rightSignature = FailureSignature.parse(rightMessage);
        if (leftSignature.code != null && rightSignature.code != null) {
            return leftSignature.code.equals(rightSignature.code);
        }
        if (leftSignature.codeName != null && rightSignature.codeName != null) {
            return leftSignature.codeName.equals(rightSignature.codeName);
        }
        return Objects.equals(leftMessage, rightMessage);
    }

    private static final class FailureSignature {
        private final Integer code;
        private final String codeName;

        private FailureSignature(Integer code, String codeName) {
            this.code = code;
            this.codeName = codeName;
        }

        private static FailureSignature parse(String message) {
            if (message == null) {
                return new FailureSignature(null, null);
            }
            Matcher matcher = FAILURE_CODE_PATTERN.matcher(message);
            if (!matcher.find()) {
                return new FailureSignature(null, null);
            }
            Integer code = Integer.valueOf(matcher.group(1));
            String codeName = matcher.group(2);
            if (codeName != null) {
                codeName = codeName.trim();
                if (codeName.isEmpty()) {
                    codeName = null;
                }
            }
            return new FailureSignature(code, codeName);
        }
    }

    private static void compareValue(String path, Object left, Object right, List<DiffEntry> entries) {
        if (valuesEqual(left, right)) {
            return;
        }
        if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
            compareMap(path, leftMap, rightMap, entries);
            return;
        }
        if (left instanceof List<?> leftList && right instanceof List<?> rightList) {
            compareList(path, leftList, rightList, entries);
            return;
        }
        entries.add(new DiffEntry(path, left, right, "value mismatch"));
    }

    private static void compareMap(
        String path,
        Map<?, ?> leftMap,
        Map<?, ?> rightMap,
        List<DiffEntry> entries
    ) {
        Map<String, Object> leftNormalized = normalizeKeyMap(leftMap);
        Map<String, Object> rightNormalized = normalizeKeyMap(rightMap);
        TreeSet<String> keys = new TreeSet<>();
        keys.addAll(leftNormalized.keySet());
        keys.addAll(rightNormalized.keySet());
        for (String key : keys) {
            boolean hasLeft = leftNormalized.containsKey(key);
            boolean hasRight = rightNormalized.containsKey(key);
            if (!hasLeft || !hasRight) {
                entries.add(new DiffEntry(
                    path + "." + key,
                    leftNormalized.get(key),
                    rightNormalized.get(key),
                    "missing key"
                ));
                continue;
            }
            compareValue(path + "." + key, leftNormalized.get(key), rightNormalized.get(key), entries);
        }
    }

    private static void compareList(
        String path,
        List<?> leftList,
        List<?> rightList,
        List<DiffEntry> entries
    ) {
        if (leftList.size() != rightList.size()) {
            entries.add(new DiffEntry(path + ".length", leftList.size(), rightList.size(), "list size mismatch"));
        }
        int limit = Math.min(leftList.size(), rightList.size());
        for (int i = 0; i < limit; i++) {
            compareValue(path + "[" + i + "]", leftList.get(i), rightList.get(i), entries);
        }
    }

    private static boolean valuesEqual(Object left, Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return numericEquals(leftNumber, rightNumber);
        }
        return Objects.equals(left, right);
    }

    private static boolean numericEquals(Number left, Number right) {
        try {
            BigDecimal leftDecimal = new BigDecimal(left.toString());
            BigDecimal rightDecimal = new BigDecimal(right.toString());
            return leftDecimal.compareTo(rightDecimal) == 0;
        } catch (NumberFormatException ignored) {
            return Objects.equals(left, right);
        }
    }

    private static Map<String, Object> normalizeKeyMap(Map<?, ?> source) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return normalized;
    }
}
