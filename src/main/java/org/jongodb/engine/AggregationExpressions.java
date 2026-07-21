package org.jongodb.engine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

/** Expression evaluator used by aggregation stages and aggregation-pipeline updates. */
final class AggregationExpressions {
    private static final Object MISSING = new Object();
    private static final ZonedDateTime REFERENCE =
            ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    private AggregationExpressions() {}

    static Object evaluate(final Document source, final Object expression) {
        if (expression instanceof String pathExpression && pathExpression.startsWith("$")) {
            if ("$$ROOT".equals(pathExpression) || "$$CURRENT".equals(pathExpression)) {
                return DocumentCopies.copy(source);
            }
            if (pathExpression.startsWith("$$")) {
                throw new UnsupportedFeatureException(
                        "aggregation.expression.variable",
                        "unsupported aggregation variable: " + pathExpression);
            }
            return resolvePath(source, pathExpression.substring(1));
        }
        if (expression instanceof Map<?, ?> rawMap) {
            final Map<String, Object> map = stringMap(rawMap);
            if (map.size() == 1) {
                final Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                if (entry.getKey().startsWith("$")) {
                    return evaluateOperator(source, entry.getKey(), entry.getValue());
                }
            }
            final Document evaluated = new Document();
            for (final Map.Entry<String, Object> entry : map.entrySet()) {
                final Object value = evaluate(source, entry.getValue());
                if (!isMissing(value)) {
                    evaluated.put(entry.getKey(), DocumentCopies.copyAny(value));
                }
            }
            return evaluated;
        }
        if (expression instanceof List<?> list) {
            final List<Object> evaluated = new ArrayList<>(list.size());
            for (final Object item : list) {
                final Object value = evaluate(source, item);
                evaluated.add(isMissing(value) ? null : DocumentCopies.copyAny(value));
            }
            return evaluated;
        }
        return DocumentCopies.copyAny(expression);
    }

    static boolean isMissing(final Object value) {
        return value == MISSING;
    }

    static Object nullIfMissing(final Object value) {
        return isMissing(value) ? null : value;
    }

    static boolean truthy(final Object value) {
        if (value == null || isMissing(value)) {
            return false;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value instanceof Number numberValue) {
            return numberValue.doubleValue() != 0d && !Double.isNaN(numberValue.doubleValue());
        }
        return true;
    }

    private static Object evaluateOperator(
            final Document source, final String operator, final Object operand) {
        return switch (operator) {
            case "$literal" -> DocumentCopies.copyAny(operand);
            case "$cond" -> evaluateCond(source, operand);
            case "$ifNull" -> evaluateIfNull(source, operand);
            case "$type" -> typeName(evaluate(source, operand));
            case "$eq" -> compare(source, operand, Comparison.EQ);
            case "$ne" -> compare(source, operand, Comparison.NE);
            case "$gt" -> compare(source, operand, Comparison.GT);
            case "$gte" -> compare(source, operand, Comparison.GTE);
            case "$lt" -> compare(source, operand, Comparison.LT);
            case "$lte" -> compare(source, operand, Comparison.LTE);
            case "$cmp" -> compare(source, operand, Comparison.CMP);
            case "$and" -> evaluateAnd(source, operand);
            case "$or" -> evaluateOr(source, operand);
            case "$not" -> evaluateNot(source, operand);
            case "$max" -> evaluateMinMax(source, operand, true);
            case "$min" -> evaluateMinMax(source, operand, false);
            case "$add" -> evaluateArithmetic(source, operand, Arithmetic.ADD);
            case "$subtract" -> evaluateArithmetic(source, operand, Arithmetic.SUBTRACT);
            case "$multiply" -> evaluateArithmetic(source, operand, Arithmetic.MULTIPLY);
            case "$divide" -> evaluateArithmetic(source, operand, Arithmetic.DIVIDE);
            case "$mergeObjects" -> evaluateMergeObjects(source, operand);
            case "$dateTrunc" -> evaluateDateTrunc(source, operand);
            default -> throw new UnsupportedFeatureException(
                    "aggregation.expression." + operator,
                    "unsupported aggregation expression: " + operator);
        };
    }

    private static Object evaluateCond(final Document source, final Object operand) {
        final Object condition;
        final Object thenExpression;
        final Object elseExpression;
        if (operand instanceof List<?> list) {
            if (list.size() != 3) {
                throw new IllegalArgumentException("$cond array form requires exactly three arguments");
            }
            condition = list.get(0);
            thenExpression = list.get(1);
            elseExpression = list.get(2);
        } else if (operand instanceof Map<?, ?> map) {
            final Map<String, Object> definition = stringMap(map);
            if (!definition.keySet().equals(java.util.Set.of("if", "then", "else"))) {
                throw new IllegalArgumentException("$cond object form requires if, then, and else");
            }
            condition = definition.get("if");
            thenExpression = definition.get("then");
            elseExpression = definition.get("else");
        } else {
            throw new IllegalArgumentException("$cond requires an array or document argument");
        }
        return evaluate(source, truthy(evaluate(source, condition)) ? thenExpression : elseExpression);
    }

    private static Object evaluateIfNull(final Document source, final Object operand) {
        final List<?> arguments = requireArguments(operand, "$ifNull", 2, null);
        for (final Object argument : arguments) {
            final Object value = evaluate(source, argument);
            if (value != null && !isMissing(value)) {
                return value;
            }
        }
        return null;
    }

    private static Object compare(
            final Document source, final Object operand, final Comparison comparison) {
        final List<?> arguments = requireArguments(operand, comparison.operatorName(), 2, 2);
        final Object left = nullIfMissing(evaluate(source, arguments.get(0)));
        final Object right = nullIfMissing(evaluate(source, arguments.get(1)));
        final int compared = MongoValueComparator.compare(left, right);
        return switch (comparison) {
            case EQ -> compared == 0;
            case NE -> compared != 0;
            case GT -> compared > 0;
            case GTE -> compared >= 0;
            case LT -> compared < 0;
            case LTE -> compared <= 0;
            case CMP -> Integer.signum(compared);
        };
    }

    private static boolean evaluateAnd(final Document source, final Object operand) {
        for (final Object argument : requireArguments(operand, "$and", 0, null)) {
            if (!truthy(evaluate(source, argument))) {
                return false;
            }
        }
        return true;
    }

    private static boolean evaluateOr(final Document source, final Object operand) {
        for (final Object argument : requireArguments(operand, "$or", 0, null)) {
            if (truthy(evaluate(source, argument))) {
                return true;
            }
        }
        return false;
    }

    private static boolean evaluateNot(final Document source, final Object operand) {
        final List<?> arguments = requireArguments(operand, "$not", 1, 1);
        return !truthy(evaluate(source, arguments.get(0)));
    }

    private static Object evaluateMinMax(
            final Document source, final Object operand, final boolean maximum) {
        final List<Object> values = new ArrayList<>();
        if (operand instanceof List<?> arguments) {
            for (final Object argument : arguments) {
                collectMinMaxValue(values, evaluate(source, argument), false);
            }
        } else {
            collectMinMaxValue(values, evaluate(source, operand), true);
        }

        Object selected = MISSING;
        for (final Object value : values) {
            if (value == null || isMissing(value)) {
                continue;
            }
            if (isMissing(selected)) {
                selected = value;
                continue;
            }
            final int compared = MongoValueComparator.compare(value, selected);
            if ((maximum && compared > 0) || (!maximum && compared < 0)) {
                selected = value;
            }
        }
        return isMissing(selected) ? null : DocumentCopies.copyAny(selected);
    }

    private static void collectMinMaxValue(
            final List<Object> values, final Object value, final boolean traverseArray) {
        if (traverseArray && value instanceof List<?> list) {
            values.addAll(list);
            return;
        }
        values.add(value);
    }

    private static Object evaluateArithmetic(
            final Document source, final Object operand, final Arithmetic arithmetic) {
        final int minimum = arithmetic == Arithmetic.ADD || arithmetic == Arithmetic.MULTIPLY ? 1 : 2;
        final Integer exact = arithmetic == Arithmetic.SUBTRACT || arithmetic == Arithmetic.DIVIDE ? 2 : null;
        final List<?> arguments = requireArguments(operand, arithmetic.operatorName(), minimum, exact);
        final List<Number> numbers = new ArrayList<>(arguments.size());
        boolean floating = false;
        for (final Object argument : arguments) {
            final Object value = evaluate(source, argument);
            if (value == null || isMissing(value)) {
                return null;
            }
            if (!(value instanceof Number number)) {
                throw new IllegalArgumentException(arithmetic.operatorName() + " accepts numeric operands only");
            }
            floating |= number instanceof Float || number instanceof Double || number instanceof BigDecimal;
            numbers.add(number);
        }

        BigDecimal result = toBigDecimal(numbers.get(0));
        if (arithmetic == Arithmetic.ADD) {
            result = BigDecimal.ZERO;
        } else if (arithmetic == Arithmetic.MULTIPLY) {
            result = BigDecimal.ONE;
        }
        for (int index = arithmetic == Arithmetic.SUBTRACT || arithmetic == Arithmetic.DIVIDE ? 1 : 0;
                index < numbers.size();
                index++) {
            final BigDecimal value = toBigDecimal(numbers.get(index));
            result = switch (arithmetic) {
                case ADD -> result.add(value);
                case SUBTRACT -> result.subtract(value);
                case MULTIPLY -> result.multiply(value);
                case DIVIDE -> {
                    if (value.compareTo(BigDecimal.ZERO) == 0) {
                        throw new IllegalArgumentException("$divide cannot divide by zero");
                    }
                    yield result.divide(value, java.math.MathContext.DECIMAL128);
                }
            };
        }
        if (arithmetic == Arithmetic.DIVIDE || floating) {
            return result.doubleValue();
        }
        try {
            return result.longValueExact();
        } catch (final ArithmeticException ignored) {
            return result;
        }
    }

    private static Object evaluateMergeObjects(final Document source, final Object operand) {
        final List<?> expressions = operand instanceof List<?> list ? list : List.of(operand);
        final Document merged = new Document();
        for (final Object expression : expressions) {
            final Object value = evaluate(source, expression);
            if (value == null || isMissing(value)) {
                continue;
            }
            if (!(value instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("$mergeObjects operands must evaluate to documents or null");
            }
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("$mergeObjects document keys must be strings");
                }
                merged.put(key, DocumentCopies.copyAny(entry.getValue()));
            }
        }
        return merged;
    }

    private static Object evaluateDateTrunc(final Document source, final Object operand) {
        if (!(operand instanceof Map<?, ?> rawDefinition)) {
            throw new IllegalArgumentException("$dateTrunc requires a document argument");
        }
        final Map<String, Object> definition = stringMap(rawDefinition);
        for (final String key : definition.keySet()) {
            if (!java.util.Set.of("date", "unit", "binSize", "timezone", "startOfWeek").contains(key)) {
                throw new IllegalArgumentException("unsupported $dateTrunc option: " + key);
            }
        }
        if (!definition.containsKey("date") || !definition.containsKey("unit")) {
            throw new IllegalArgumentException("$dateTrunc requires date and unit");
        }

        final Object rawDate = evaluate(source, definition.get("date"));
        if (rawDate == null || isMissing(rawDate)) {
            return null;
        }
        if (!(rawDate instanceof Date)
                && !(rawDate instanceof Instant)
                && !(rawDate instanceof BsonTimestamp)
                && !(rawDate instanceof ObjectId)) {
            throw new IllegalArgumentException("$dateTrunc.date must evaluate to a date");
        }
        final Instant instant;
        if (rawDate instanceof Date date) {
            instant = date.toInstant();
        } else if (rawDate instanceof Instant instantValue) {
            instant = instantValue;
        } else if (rawDate instanceof BsonTimestamp timestamp) {
            instant = Instant.ofEpochSecond(Integer.toUnsignedLong(timestamp.getTime()));
        } else {
            instant = ((ObjectId) rawDate).getDate().toInstant();
        }

        final Object unitValue = evaluate(source, definition.get("unit"));
        if (unitValue == null || isMissing(unitValue)) {
            return null;
        }
        if (!(unitValue instanceof String unit)) {
            throw new IllegalArgumentException("$dateTrunc.unit must evaluate to a string");
        }
        final Object binSizeValue = definition.containsKey("binSize")
                ? evaluate(source, definition.get("binSize"))
                : 1;
        if (binSizeValue == null || isMissing(binSizeValue)) {
            return null;
        }
        final int binSize = readPositiveInt(binSizeValue, "$dateTrunc.binSize");
        final Object timezoneValue = definition.containsKey("timezone")
                ? evaluate(source, definition.get("timezone"))
                : "UTC";
        if (timezoneValue == null || isMissing(timezoneValue)) {
            return null;
        }
        if (!(timezoneValue instanceof String timezone)) {
            throw new IllegalArgumentException("$dateTrunc.timezone must evaluate to a string");
        }
        if (!isUtc(timezone)) {
            throw new UnsupportedFeatureException(
                    "aggregation.expression.$dateTrunc.timezone",
                    "$dateTrunc currently supports UTC timezone only: " + timezone);
        }
        final DayOfWeek startOfWeek;
        if ("week".equalsIgnoreCase(unit)) {
            final Object startOfWeekValue = definition.containsKey("startOfWeek")
                    ? evaluate(source, definition.get("startOfWeek"))
                    : "sunday";
            if (startOfWeekValue == null || isMissing(startOfWeekValue)) {
                return null;
            }
            startOfWeek = parseDayOfWeek(startOfWeekValue);
        } else {
            startOfWeek = DayOfWeek.SUNDAY;
        }

        return Date.from(truncateUtc(instant, unit.toLowerCase(java.util.Locale.ROOT), binSize, startOfWeek));
    }

    private static Instant truncateUtc(
            final Instant instant,
            final String unit,
            final int binSize,
            final DayOfWeek startOfWeek) {
        final ZonedDateTime value = instant.atZone(ZoneOffset.UTC);
        final ZonedDateTime truncated;
        switch (unit) {
            case "second", "minute", "hour", "day" -> {
                final long unitMillis = switch (unit) {
                    case "second" -> 1_000L;
                    case "minute" -> 60_000L;
                    case "hour" -> 3_600_000L;
                    default -> 86_400_000L;
                };
                final long widthMillis = Math.multiplyExact(unitMillis, binSize);
                final long elapsedMillis = value.toInstant().toEpochMilli() - REFERENCE.toInstant().toEpochMilli();
                truncated = REFERENCE.plus(
                        Math.floorDiv(elapsedMillis, widthMillis) * widthMillis,
                        ChronoUnit.MILLIS);
            }
            case "week" -> {
                final ZonedDateTime weekReference = REFERENCE.with(TemporalAdjusters.nextOrSame(startOfWeek));
                final long widthMillis = Math.multiplyExact(604_800_000L, binSize);
                final long elapsedMillis = value.toInstant().toEpochMilli()
                        - weekReference.toInstant().toEpochMilli();
                truncated = weekReference.plus(
                        Math.floorDiv(elapsedMillis, widthMillis) * widthMillis,
                        ChronoUnit.MILLIS);
            }
            case "month", "quarter", "year" -> {
                final int unitMonths = "month".equals(unit) ? 1 : "quarter".equals(unit) ? 3 : 12;
                final long elapsedMonths = ChronoUnit.MONTHS.between(
                        REFERENCE.toLocalDate().withDayOfMonth(1), value.toLocalDate().withDayOfMonth(1));
                final long width = (long) binSize * unitMonths;
                truncated = REFERENCE.plusMonths(Math.floorDiv(elapsedMonths, width) * width);
            }
            default -> throw new IllegalArgumentException("unsupported $dateTrunc unit: " + unit);
        }
        return truncated.toInstant();
    }

    private static DayOfWeek parseDayOfWeek(final Object value) {
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException("$dateTrunc.startOfWeek must evaluate to a string");
        }
        return switch (text.toLowerCase(java.util.Locale.ROOT)) {
            case "monday", "mon" -> DayOfWeek.MONDAY;
            case "tuesday", "tue" -> DayOfWeek.TUESDAY;
            case "wednesday", "wed" -> DayOfWeek.WEDNESDAY;
            case "thursday", "thu" -> DayOfWeek.THURSDAY;
            case "friday", "fri" -> DayOfWeek.FRIDAY;
            case "saturday", "sat" -> DayOfWeek.SATURDAY;
            case "sunday", "sun" -> DayOfWeek.SUNDAY;
            default -> throw new IllegalArgumentException("unsupported $dateTrunc.startOfWeek: " + text);
        };
    }

    private static boolean isUtc(final String timezone) {
        return "UTC".equalsIgnoreCase(timezone)
                || "GMT".equalsIgnoreCase(timezone)
                || "Etc/UTC".equalsIgnoreCase(timezone)
                || "Z".equalsIgnoreCase(timezone)
                || "+00:00".equals(timezone)
                || "-00:00".equals(timezone);
    }

    private static int readPositiveInt(final Object value, final String name) {
        if (!(value instanceof Number number)
                || !Double.isFinite(number.doubleValue())
                || Math.rint(number.doubleValue()) != number.doubleValue()
                || number.doubleValue() < 1
                || number.doubleValue() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be a positive integer");
        }
        return number.intValue();
    }

    private static Object resolvePath(final Object source, final String path) {
        if (path == null || path.isEmpty()) {
            return MISSING;
        }
        Object current = source;
        for (final String segment : path.split("\\.")) {
            if (!(current instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                return MISSING;
            }
            current = map.get(segment);
        }
        return DocumentCopies.copyAny(current);
    }

    private static String typeName(final Object value) {
        if (isMissing(value)) {
            return "missing";
        }
        if (value == null) {
            return "null";
        }
        if (value instanceof Double || value instanceof Float) {
            return "double";
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return "int";
        }
        if (value instanceof Long || value instanceof BigInteger) {
            return "long";
        }
        if (value instanceof Decimal128 || value instanceof BigDecimal) {
            return "decimal";
        }
        if (value instanceof String || value instanceof Character) {
            return "string";
        }
        if (value instanceof Map<?, ?>) {
            return "object";
        }
        if (value instanceof List<?> || value.getClass().isArray()) {
            return "array";
        }
        if (value instanceof Boolean) {
            return "bool";
        }
        if (value instanceof Date || value instanceof Instant) {
            return "date";
        }
        if (value instanceof ObjectId) {
            return "objectId";
        }
        if (value instanceof Binary || value instanceof byte[]) {
            return "binData";
        }
        if (value instanceof Pattern || value instanceof BsonRegularExpression) {
            return "regex";
        }
        return value.getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT);
    }

    private static List<?> requireArguments(
            final Object operand,
            final String operator,
            final int minimum,
            final Integer exact) {
        if (!(operand instanceof List<?> list)) {
            throw new IllegalArgumentException(operator + " requires an array argument");
        }
        if (exact != null && list.size() != exact) {
            throw new IllegalArgumentException(operator + " requires exactly " + exact + " arguments");
        }
        if (list.size() < minimum) {
            throw new IllegalArgumentException(operator + " requires at least " + minimum + " arguments");
        }
        return list;
    }

    private static Map<String, Object> stringMap(final Map<?, ?> rawMap) {
        final Map<String, Object> output = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("aggregation expression document keys must be strings");
            }
            output.put(key, entry.getValue());
        }
        return output;
    }

    private static BigDecimal toBigDecimal(final Number value) {
        if (value instanceof BigDecimal bigDecimal) {
            return bigDecimal;
        }
        if (value instanceof BigInteger bigInteger) {
            return new BigDecimal(bigInteger);
        }
        if (value instanceof Decimal128 decimal128) {
            return decimal128.bigDecimalValue();
        }
        return new BigDecimal(value.toString());
    }

    private enum Comparison {
        EQ("$eq"),
        NE("$ne"),
        GT("$gt"),
        GTE("$gte"),
        LT("$lt"),
        LTE("$lte"),
        CMP("$cmp");

        private final String operatorName;

        Comparison(final String operatorName) {
            this.operatorName = operatorName;
        }

        private String operatorName() {
            return operatorName;
        }
    }

    private enum Arithmetic {
        ADD("$add"),
        SUBTRACT("$subtract"),
        MULTIPLY("$multiply"),
        DIVIDE("$divide");

        private final String operatorName;

        Arithmetic(final String operatorName) {
            this.operatorName = operatorName;
        }

        private String operatorName() {
            return operatorName;
        }
    }
}
