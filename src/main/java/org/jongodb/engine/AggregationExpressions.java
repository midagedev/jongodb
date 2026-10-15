package org.jongodb.engine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntPredicate;
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
    private static final long DATE_TRUNC_REFERENCE_UTC_MILLIS = 946_684_800_000L;
    private static final String DEFAULT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%LZ";
    private static final String DATE_FORMAT_SPECIFIERS = "YmdHMSLjwUuUVGZzBb%";
    private static final String[] SHORT_MONTHS = {
        "", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    };
    private static final String[] FULL_MONTHS = {
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    };
    // $$NOW and $$CLUSTER_TIME hold one value per command: nested executions ($facet, $lookup) and the
    // per-document executions of a multi-document pipeline update reuse the outermost capture.
    private static final ThreadLocal<Long> COMMAND_NOW = new ThreadLocal<>();

    private AggregationExpressions() {}

    static Object evaluate(final Document source, final Object expression) {
        if (expression instanceof String pathExpression && pathExpression.startsWith("$")) {
            if ("$$ROOT".equals(pathExpression) || "$$CURRENT".equals(pathExpression)) {
                return DocumentCopies.copy(source);
            }
            if ("$$NOW".equals(pathExpression)) {
                return new Date(commandMillis());
            }
            if ("$$CLUSTER_TIME".equals(pathExpression)) {
                // Reference server on a standalone rejects this variable. Callers still need one
                // timestamp per execution, aligned with $$NOW, increment 1.
                final long millis = commandMillis();
                return new BsonTimestamp((int) Math.floorDiv(millis, 1000L), 1);
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
            case "$dateToString" -> evaluateDateToString(source, operand);
            case "$year", "$month", "$dayOfMonth", "$hour", "$minute", "$second", "$millisecond",
                    "$dayOfWeek", "$dayOfYear", "$week", "$isoWeek", "$isoWeekYear", "$isoDayOfWeek" ->
                    evaluateDatePart(source, operator, operand);
            case "$concat" -> evaluateConcat(source, operand);
            case "$toLower" -> evaluateCase(source, operator, operand, true);
            case "$toUpper" -> evaluateCase(source, operator, operand, false);
            case "$toString" -> evaluateToString(source, operand);
            case "$size" -> evaluateSize(source, operand);
            case "$substrCP" -> evaluateSubstrCp(source, operand);
            case "$split" -> evaluateSplit(source, operand);
            case "$strLenCP" -> evaluateStrLenCp(source, operand);
            case "$trim" -> evaluateTrim(source, operand);
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

        final String normalizedUnit = unit.toLowerCase(Locale.ROOT);
        if (!isUtc(timezone)) {
            return Date.from(truncateZoned(instant, normalizedUnit, binSize, resolveZone(timezone), startOfWeek));
        }
        return Date.from(truncateUtc(instant, normalizedUnit, binSize, startOfWeek));
    }

    private static Instant truncateUtc(
            final Instant instant,
            final String unit,
            final int binSize,
            final DayOfWeek startOfWeek) {
        final ZonedDateTime value = instant.atZone(ZoneOffset.UTC);
        final ZonedDateTime truncated;
        switch (unit) {
            case "millisecond", "second", "minute", "hour", "day" -> {
                final long unitMillis = switch (unit) {
                    case "millisecond" -> 1L;
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
                || "+00".equals(timezone)
                || "-00".equals(timezone)
                || "+0000".equals(timezone)
                || "-0000".equals(timezone)
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

    static boolean openCommandNow() {
        if (COMMAND_NOW.get() != null) {
            return false;
        }
        COMMAND_NOW.set(System.currentTimeMillis());
        return true;
    }

    static void closeCommandNow(final boolean opened) {
        if (opened) {
            COMMAND_NOW.remove();
        }
    }

    private static long commandMillis() {
        final Long now = COMMAND_NOW.get();
        return now == null ? System.currentTimeMillis() : now;
    }

    private static Object evaluateDateToString(final Document source, final Object operand) {
        if (!(operand instanceof Map<?, ?> rawDefinition)) {
            throw new IllegalArgumentException("$dateToString requires a document argument");
        }
        final Map<String, Object> definition = stringMap(rawDefinition);
        for (final String key : definition.keySet()) {
            if (!java.util.Set.of("date", "format", "timezone", "onNull").contains(key)) {
                throw new IllegalArgumentException("unsupported $dateToString option: " + key);
            }
        }
        if (!definition.containsKey("date")) {
            throw new IllegalArgumentException("$dateToString requires date");
        }
        final Object formatValue = definition.containsKey("format")
                ? evaluate(source, definition.get("format"))
                : DEFAULT_DATE_FORMAT;
        if (!(formatValue instanceof String format)) {
            throw new IllegalArgumentException("$dateToString.format must evaluate to a string");
        }
        validateDateFormat(format);
        final Object timezoneValue = definition.containsKey("timezone")
                ? evaluate(source, definition.get("timezone"))
                : "UTC";
        if (timezoneValue == null || isMissing(timezoneValue)) {
            return null;
        }
        if (!(timezoneValue instanceof String timezone)) {
            throw new IllegalArgumentException("$dateToString.timezone must evaluate to a string");
        }
        final ZoneId zone = resolveZone(timezone);
        final Object rawDate = evaluate(source, definition.get("date"));
        if (rawDate == null || isMissing(rawDate)) {
            if (!definition.containsKey("onNull")) {
                return null;
            }
            return evaluate(source, definition.get("onNull"));
        }
        return formatDate(format, coerceInstant(rawDate).atZone(zone));
    }

    private static Object evaluateDatePart(
            final Document source, final String operator, final Object operand) {
        final Object dateExpression;
        Object timezoneExpression = "UTC";
        if (operand instanceof List<?> list) {
            if (list.size() != 1) {
                throw new IllegalArgumentException(operator + " takes exactly 1 arguments");
            }
            dateExpression = list.get(0);
        } else if (operand instanceof Map<?, ?> rawDefinition && isDateOptionDocument(rawDefinition)) {
            final Map<String, Object> definition = stringMap(rawDefinition);
            for (final String key : definition.keySet()) {
                if (!"date".equals(key) && !"timezone".equals(key)) {
                    throw new IllegalArgumentException(
                            "unrecognized option to " + operator + ": \"" + key + "\"");
                }
            }
            if (!definition.containsKey("date")) {
                throw new IllegalArgumentException(operator + " requires date");
            }
            dateExpression = definition.get("date");
            if (definition.containsKey("timezone")) {
                timezoneExpression = definition.get("timezone");
            }
        } else {
            dateExpression = operand;
        }

        final Object timezoneValue = evaluate(source, timezoneExpression);
        if (timezoneValue == null || isMissing(timezoneValue)) {
            return null;
        }
        if (!(timezoneValue instanceof String timezone)) {
            throw new IllegalArgumentException(operator + ".timezone must evaluate to a string");
        }
        final ZoneId zone = resolveZone(timezone);
        final Object rawDate = evaluate(source, dateExpression);
        if (rawDate == null || isMissing(rawDate)) {
            return null;
        }
        final ZonedDateTime zoned = coerceInstant(rawDate).atZone(zone);
        final long part = switch (operator) {
            case "$year" -> zoned.getYear();
            case "$month" -> zoned.getMonthValue();
            case "$dayOfMonth" -> zoned.getDayOfMonth();
            case "$hour" -> zoned.getHour();
            case "$minute" -> zoned.getMinute();
            case "$second" -> zoned.getSecond();
            case "$millisecond" -> zoned.get(ChronoField.MILLI_OF_SECOND);
            case "$dayOfWeek" -> sundayBasedDayOfWeek(zoned);
            case "$dayOfYear" -> zoned.getDayOfYear();
            case "$week" -> sundayWeek(zoned);
            case "$isoWeek" -> zoned.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            case "$isoWeekYear" -> zoned.get(IsoFields.WEEK_BASED_YEAR);
            case "$isoDayOfWeek" -> zoned.get(ChronoField.DAY_OF_WEEK);
            default -> throw new IllegalArgumentException("unsupported date part: " + operator);
        };
        // $isoWeekYear is a long; every other date part is an int.
        if ("$isoWeekYear".equals(operator)) {
            return part;
        }
        return (int) part;
    }

    private static boolean isDateOptionDocument(final Map<?, ?> definition) {
        return definition.containsKey("date") || definition.containsKey("timezone");
    }

    private static Instant coerceInstant(final Object value) {
        if (value instanceof Date date) {
            return date.toInstant();
        }
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof BsonTimestamp timestamp) {
            return Instant.ofEpochSecond(Integer.toUnsignedLong(timestamp.getTime()));
        }
        if (value instanceof ObjectId objectId) {
            return objectId.getDate().toInstant();
        }
        throw new IllegalArgumentException("can't convert from BSON type " + typeName(value) + " to Date");
    }

    private static int sundayBasedDayOfWeek(final ZonedDateTime zoned) {
        return zoned.getDayOfWeek() == DayOfWeek.SUNDAY ? 1 : zoned.getDayOfWeek().getValue() + 1;
    }

    private static int sundayWeek(final ZonedDateTime zoned) {
        final int weekDay = sundayBasedDayOfWeek(zoned);
        final int nextSundayDayOfYear = zoned.getDayOfYear() - weekDay + 7;
        return nextSundayDayOfYear / 7;
    }

    private static void validateDateFormat(final String format) {
        for (int index = 0; index < format.length(); index++) {
            if (format.charAt(index) != '%') {
                continue;
            }
            if (index + 1 >= format.length()) {
                throw new IllegalArgumentException("Unmatched '%' at end of format string");
            }
            final char specifier = format.charAt(++index);
            if (DATE_FORMAT_SPECIFIERS.indexOf(specifier) < 0) {
                throw new IllegalArgumentException(
                        "Invalid format character '%" + specifier + "' in format string");
            }
        }
    }

    private static String formatDate(final String format, final ZonedDateTime zoned) {
        final StringBuilder formatted = new StringBuilder(format.length() + 16);
        for (int index = 0; index < format.length(); index++) {
            final char current = format.charAt(index);
            if (current != '%') {
                formatted.append(current);
                continue;
            }
            final char specifier = format.charAt(++index);
            switch (specifier) {
                case '%' -> formatted.append('%');
                case 'Y' -> formatted.append(pad(zoned.getYear(), 4));
                case 'm' -> formatted.append(pad(zoned.getMonthValue(), 2));
                case 'd' -> formatted.append(pad(zoned.getDayOfMonth(), 2));
                case 'H' -> formatted.append(pad(zoned.getHour(), 2));
                case 'M' -> formatted.append(pad(zoned.getMinute(), 2));
                case 'S' -> formatted.append(pad(zoned.getSecond(), 2));
                case 'L' -> formatted.append(pad(zoned.get(ChronoField.MILLI_OF_SECOND), 3));
                case 'j' -> formatted.append(pad(zoned.getDayOfYear(), 3));
                case 'w' -> formatted.append(sundayBasedDayOfWeek(zoned));
                case 'u' -> formatted.append(zoned.get(ChronoField.DAY_OF_WEEK));
                case 'U' -> formatted.append(pad(sundayWeek(zoned), 2));
                case 'V' -> formatted.append(pad(zoned.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR), 2));
                case 'G' -> formatted.append(pad(zoned.get(IsoFields.WEEK_BASED_YEAR), 4));
                case 'b' -> formatted.append(SHORT_MONTHS[zoned.getMonthValue()]);
                case 'B' -> formatted.append(FULL_MONTHS[zoned.getMonthValue()]);
                case 'z' -> formatted.append(formatOffset(zoned.getOffset()));
                case 'Z' -> formatted.append(zoned.getOffset().getTotalSeconds() / 60);
                default -> throw new IllegalArgumentException(
                        "Invalid format character '%" + specifier + "' in format string");
            }
        }
        return formatted.toString();
    }

    private static String pad(final int value, final int width) {
        final boolean negative = value < 0;
        final String digits = Integer.toString(Math.abs(value));
        final String padded = digits.length() >= width
                ? digits
                : "0".repeat(width - digits.length()) + digits;
        return negative ? "-" + padded : padded;
    }

    private static String formatOffset(final ZoneOffset offset) {
        final int totalSeconds = offset.getTotalSeconds();
        final int absolute = Math.abs(totalSeconds);
        final int hours = absolute / 3600;
        final int minutes = (absolute % 3600) / 60;
        return (totalSeconds < 0 ? "-" : "+") + pad(hours, 2) + pad(minutes, 2);
    }

    private static ZoneId resolveZone(final String timezone) {
        if (isUtc(timezone)) {
            return ZoneOffset.UTC;
        }
        final ZoneOffset offset = parseOffset(timezone);
        if (offset != null) {
            return offset;
        }
        try {
            return ZoneId.of(timezone);
        } catch (final DateTimeException exception) {
            throw new IllegalArgumentException("unrecognized time zone identifier: \"" + timezone + "\"");
        }
    }

    private static ZoneOffset parseOffset(final String text) {
        if (text.length() < 3 || (text.charAt(0) != '+' && text.charAt(0) != '-')) {
            return null;
        }
        final int sign = text.charAt(0) == '+' ? 1 : -1;
        try {
            if (text.length() == 3 && allDigits(text, 1, 3)) {
                return ZoneOffset.ofHoursMinutes(sign * Integer.parseInt(text.substring(1, 3)), 0);
            }
            if (text.length() == 5 && allDigits(text, 1, 5)) {
                final int packed = Integer.parseInt(text.substring(1, 5));
                return ZoneOffset.ofHoursMinutes(sign * (packed / 100), sign * (packed % 100));
            }
            if (text.length() == 6
                    && allDigits(text, 1, 3)
                    && text.charAt(3) == ':'
                    && allDigits(text, 4, 6)) {
                return ZoneOffset.ofHoursMinutes(
                        sign * Integer.parseInt(text.substring(1, 3)),
                        sign * Integer.parseInt(text.substring(4, 6)));
            }
        } catch (final DateTimeException exception) {
            throw new IllegalArgumentException("unrecognized time zone identifier: \"" + text + "\"");
        }
        return null;
    }

    private static boolean allDigits(final String text, final int from, final int to) {
        for (int index = from; index < to; index++) {
            if (!Character.isDigit(text.charAt(index))) {
                return false;
            }
        }
        return true;
    }

    // Sub-day bins are fixed widths from local 2000-01-01T00:00:00, so a DST transition does not
    // move the grid. Day and larger units follow the calendar in the zone.
    private static Instant truncateZoned(
            final Instant instant,
            final String unit,
            final int binSize,
            final ZoneId zone,
            final DayOfWeek startOfWeek) {
        final int offsetSeconds = zone.getRules().getOffset(Instant.ofEpochMilli(DATE_TRUNC_REFERENCE_UTC_MILLIS))
                .getTotalSeconds();
        final long referenceMillis = DATE_TRUNC_REFERENCE_UTC_MILLIS - offsetSeconds * 1000L;
        switch (unit) {
            case "millisecond", "second", "minute", "hour" -> {
                final long unitMillis = switch (unit) {
                    case "millisecond" -> 1L;
                    case "second" -> 1_000L;
                    case "minute" -> 60_000L;
                    default -> 3_600_000L;
                };
                final long width = Math.multiplyExact(unitMillis, binSize);
                final long elapsed = instant.toEpochMilli() - referenceMillis;
                return Instant.ofEpochMilli(referenceMillis + Math.floorDiv(elapsed, width) * width);
            }
            case "day", "week", "month", "quarter", "year" -> {
                final int weekAdjust = "week".equals(unit)
                        ? Math.floorMod(startOfWeek.getValue() - DayOfWeek.SATURDAY.getValue(), 7)
                        : 0;
                final LocalDate referenceDate = LocalDate.of(2000, 1, 1).plusDays(weekAdjust);
                final LocalDate localDate = instant.atZone(zone).toLocalDate();
                final long distance = switch (unit) {
                    case "day" -> ChronoUnit.DAYS.between(referenceDate, localDate);
                    case "week" -> weekDistance(referenceDate, localDate, startOfWeek);
                    case "month" -> ChronoUnit.MONTHS.between(
                            referenceDate.withDayOfMonth(1), localDate.withDayOfMonth(1));
                    case "quarter" -> quarterDistance(localDate);
                    default -> localDate.getYear() - 2000L;
                };
                final long lower = distance - Math.floorMod(distance, binSize);
                final LocalDate truncated = switch (unit) {
                    case "day" -> referenceDate.plusDays(lower);
                    case "week" -> referenceDate.plusWeeks(lower);
                    case "month" -> referenceDate.plusMonths(lower);
                    case "quarter" -> LocalDate.of(2000, 1, 1).plusMonths(lower * 3L);
                    default -> LocalDate.of(2000, 1, 1).plusYears(lower);
                };
                return truncated.atStartOfDay(zone).toInstant();
            }
            default -> throw new IllegalArgumentException("unsupported $dateTrunc unit: " + unit);
        }
    }

    private static long weekDistance(
            final LocalDate start, final LocalDate end, final DayOfWeek startOfWeek) {
        final long days = ChronoUnit.DAYS.between(start, end);
        return (days + dayIndex(start, startOfWeek) - dayIndex(end, startOfWeek)) / 7L;
    }

    private static int dayIndex(final LocalDate date, final DayOfWeek startOfWeek) {
        return Math.floorMod(date.getDayOfWeek().getValue() - startOfWeek.getValue(), 7);
    }

    private static long quarterDistance(final LocalDate date) {
        final int quarter = (date.getMonthValue() - 1) / 3;
        return quarter + (date.getYear() - 2000L) * 4L;
    }

    private static Object evaluateConcat(final Document source, final Object operand) {
        final List<?> arguments = requireArguments(operand, "$concat", 0, null);
        final StringBuilder concatenated = new StringBuilder();
        for (final Object argument : arguments) {
            final Object value = evaluate(source, argument);
            if (value == null || isMissing(value)) {
                return null;
            }
            if (!(value instanceof String text)) {
                throw new IllegalArgumentException(
                        "$concat only supports strings, not " + typeName(value));
            }
            concatenated.append(text);
        }
        return concatenated.toString();
    }

    private static Object evaluateCase(
            final Document source, final String operator, final Object operand, final boolean lower) {
        final Object value = evaluate(source, soleExpression(operand, operator));
        // Null and missing become an empty string, unlike $toString.
        if (value == null || isMissing(value)) {
            return "";
        }
        final String text = stringifyForCase(value);
        return lower ? text.toLowerCase(Locale.ROOT) : text.toUpperCase(Locale.ROOT);
    }

    private static Object evaluateToString(final Document source, final Object operand) {
        final Object value = evaluate(source, soleExpression(operand, "$toString"));
        if (value == null || isMissing(value)) {
            return null;
        }
        return stringifyForToString(value);
    }

    private static Object evaluateSize(final Document source, final Object operand) {
        final Object value = evaluate(source, soleExpression(operand, "$size"));
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("$size requires an array");
        }
        return list.size();
    }

    private static Object evaluateSubstrCp(final Document source, final Object operand) {
        final List<?> arguments = requireArguments(operand, "$substrCP", 3, 3);
        final Object value = evaluate(source, arguments.get(0));
        if (value == null || isMissing(value)) {
            return "";
        }
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException("$substrCP requires a string");
        }
        final int index = readNonNegativeInt(evaluate(source, arguments.get(1)), "$substrCP index");
        final int count = readNonNegativeInt(evaluate(source, arguments.get(2)), "$substrCP count");
        final int length = text.codePointCount(0, text.length());
        if (index >= length || count == 0) {
            return "";
        }
        final int end = count >= length - index ? length : index + count;
        return text.substring(text.offsetByCodePoints(0, index), text.offsetByCodePoints(0, end));
    }

    private static Object evaluateSplit(final Document source, final Object operand) {
        final List<?> arguments = requireArguments(operand, "$split", 2, 2);
        final Object value = evaluate(source, arguments.get(0));
        final Object delimiterValue = evaluate(source, arguments.get(1));
        if (value == null || isMissing(value) || delimiterValue == null || isMissing(delimiterValue)) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException("$split requires a string");
        }
        if (!(delimiterValue instanceof String delimiter)) {
            throw new IllegalArgumentException("$split delimiter must be a string");
        }
        if (delimiter.isEmpty()) {
            throw new IllegalArgumentException("$split requires a non-empty separator");
        }
        final List<String> parts = new ArrayList<>();
        int start = 0;
        while (true) {
            final int found = text.indexOf(delimiter, start);
            if (found < 0) {
                parts.add(text.substring(start));
                return parts;
            }
            parts.add(text.substring(start, found));
            start = found + delimiter.length();
        }
    }

    private static Object evaluateStrLenCp(final Document source, final Object operand) {
        final Object value = evaluate(source, soleExpression(operand, "$strLenCP"));
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException("$strLenCP requires a string");
        }
        return text.codePointCount(0, text.length());
    }

    private static Object evaluateTrim(final Document source, final Object operand) {
        if (!(operand instanceof Map<?, ?> rawDefinition)) {
            throw new IllegalArgumentException("$trim requires a document argument");
        }
        final Map<String, Object> definition = stringMap(rawDefinition);
        for (final String key : definition.keySet()) {
            if (!"input".equals(key) && !"chars".equals(key)) {
                throw new IllegalArgumentException("unsupported $trim option: " + key);
            }
        }
        if (!definition.containsKey("input")) {
            throw new IllegalArgumentException("$trim requires input");
        }
        final Object inputValue = evaluate(source, definition.get("input"));
        if (inputValue == null || isMissing(inputValue)) {
            return null;
        }
        if (!(inputValue instanceof String input)) {
            throw new IllegalArgumentException("can't convert from BSON type " + typeName(inputValue) + " to String");
        }
        final IntPredicate drop;
        if (!definition.containsKey("chars")) {
            drop = AggregationExpressions::isDefaultTrimCodePoint;
        } else {
            final Object charsValue = evaluate(source, definition.get("chars"));
            if (charsValue == null || isMissing(charsValue)) {
                return null;
            }
            if (!(charsValue instanceof String chars)) {
                throw new IllegalArgumentException("$trim.chars must evaluate to a string");
            }
            drop = codePoint -> containsCodePoint(chars, codePoint);
        }
        return trimCodePoints(input, drop);
    }

    // The default trim set includes NUL and U+0085 and excludes several Unicode White_Space
    // characters (U+2028, U+2029, U+202F, U+205F, U+3000, U+FEFF).
    private static boolean isDefaultTrimCodePoint(final int codePoint) {
        return codePoint == 0x0000
                || (codePoint >= 0x0009 && codePoint <= 0x000D)
                || codePoint == 0x0020
                || codePoint == 0x0085
                || codePoint == 0x00A0
                || codePoint == 0x1680
                || (codePoint >= 0x2000 && codePoint <= 0x200A);
    }

    private static boolean containsCodePoint(final String chars, final int codePoint) {
        int index = 0;
        while (index < chars.length()) {
            final int current = chars.codePointAt(index);
            if (current == codePoint) {
                return true;
            }
            index += Character.charCount(current);
        }
        return false;
    }

    private static String trimCodePoints(final String input, final IntPredicate drop) {
        int start = 0;
        int end = input.length();
        while (start < end) {
            final int codePoint = input.codePointAt(start);
            if (!drop.test(codePoint)) {
                break;
            }
            start += Character.charCount(codePoint);
        }
        while (end > start) {
            final int codePoint = input.codePointBefore(end);
            if (!drop.test(codePoint)) {
                break;
            }
            end -= Character.charCount(codePoint);
        }
        return input.substring(start, end);
    }

    private static Object soleExpression(final Object operand, final String operator) {
        if (operand instanceof List<?> list) {
            if (list.size() != 1) {
                throw new IllegalArgumentException(operator + " takes exactly 1 arguments");
            }
            return list.get(0);
        }
        return operand;
    }

    private static String stringifyForCase(final Object value) {
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof Date || value instanceof Instant || value instanceof BsonTimestamp) {
            return formatDate(DEFAULT_DATE_FORMAT, coerceInstant(value).atZone(ZoneOffset.UTC));
        }
        if (value instanceof Float || value instanceof Double) {
            return formatDouble(((Number) value).doubleValue(), 6, 6);
        }
        if (value instanceof Decimal128 decimal) {
            return decimal.bigDecimalValue().toPlainString();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Number number) {
            return number.toString();
        }
        throw new IllegalArgumentException("can't convert from BSON type " + typeName(value) + " to String");
    }

    private static String stringifyForToString(final Object value) {
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue ? "true" : "false";
        }
        if (value instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        if (value instanceof Date || value instanceof Instant || value instanceof BsonTimestamp) {
            return formatDate(DEFAULT_DATE_FORMAT, coerceInstant(value).atZone(ZoneOffset.UTC));
        }
        if (value instanceof Float || value instanceof Double) {
            return formatDouble(((Number) value).doubleValue(), null, 16);
        }
        if (value instanceof Decimal128 decimal) {
            return decimal.bigDecimalValue().toPlainString();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Number number) {
            return number.toString();
        }
        throw new IllegalArgumentException("can't convert from BSON type " + typeName(value) + " to String");
    }

    private static String formatDouble(
            final double value, final Integer significantDigits, final int scientificExponent) {
        if (Double.isNaN(value)) {
            return significantDigits == null ? "NaN" : "nan";
        }
        if (value == Double.POSITIVE_INFINITY) {
            return significantDigits == null ? "Infinity" : "inf";
        }
        if (value == Double.NEGATIVE_INFINITY) {
            return significantDigits == null ? "-Infinity" : "-inf";
        }
        final boolean negative = Double.doubleToRawLongBits(value) < 0;
        final double magnitude = Math.abs(value);
        if (magnitude == 0d) {
            return negative ? "-0" : "0";
        }
        BigDecimal decimal = new BigDecimal(Double.toString(magnitude));
        if (significantDigits != null) {
            decimal = decimal.round(new MathContext(significantDigits, RoundingMode.HALF_EVEN));
        }
        decimal = decimal.stripTrailingZeros();
        if (decimal.signum() == 0) {
            return negative ? "-0" : "0";
        }
        final int exponent = decimal.precision() - decimal.scale() - 1;
        final String body = exponent >= scientificExponent || exponent < -4
                ? scientific(decimal, exponent)
                : decimal.toPlainString();
        return negative ? "-" + body : body;
    }

    private static String scientific(final BigDecimal positive, final int exponent) {
        final String digits = positive.unscaledValue().toString();
        final String mantissa = digits.length() == 1
                ? digits
                : digits.charAt(0) + "." + digits.substring(1);
        final String absoluteExponent = Integer.toString(Math.abs(exponent));
        return mantissa + "e" + (exponent < 0 ? "-" : "+")
                + (absoluteExponent.length() < 2 ? "0" + absoluteExponent : absoluteExponent);
    }

    private static int readNonNegativeInt(final Object value, final String label) {
        if (!(value instanceof Number number) || !isIntegral(number)) {
            throw new IllegalArgumentException(label + " must be a non-negative integer");
        }
        final long exact;
        try {
            exact = integralLong(number);
        } catch (final ArithmeticException exception) {
            throw new IllegalArgumentException(label + " must be a non-negative integer");
        }
        if (exact < 0 || exact > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(label + " must be a non-negative integer");
        }
        return (int) exact;
    }

    private static boolean isIntegral(final Number number) {
        if (number instanceof Double doubleValue) {
            return Double.isFinite(doubleValue) && doubleValue == Math.rint(doubleValue);
        }
        if (number instanceof Float floatValue) {
            return Float.isFinite(floatValue) && floatValue == Math.rint(floatValue);
        }
        if (number instanceof BigDecimal decimal) {
            return decimal.stripTrailingZeros().scale() <= 0;
        }
        if (number instanceof Decimal128 decimal) {
            return decimal.bigDecimalValue().stripTrailingZeros().scale() <= 0;
        }
        return number instanceof Byte
                || number instanceof Short
                || number instanceof Integer
                || number instanceof Long
                || number instanceof BigInteger;
    }

    private static long integralLong(final Number number) {
        if (number instanceof BigDecimal decimal) {
            return decimal.longValueExact();
        }
        if (number instanceof Decimal128 decimal) {
            return decimal.bigDecimalValue().longValueExact();
        }
        if (number instanceof BigInteger bigInteger) {
            return bigInteger.longValueExact();
        }
        if (number instanceof Double || number instanceof Float) {
            return (long) number.doubleValue();
        }
        return number.longValue();
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
