package org.jongodb.engine;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

final class QueryMatcher {
    private QueryMatcher() {}

    static boolean matches(Document document, Document filter) {
        Objects.requireNonNull(document, "document");
        Objects.requireNonNull(filter, "filter");

        for (Map.Entry<String, Object> criteria : filter.entrySet()) {
            String key = criteria.getKey();
            Object expected = criteria.getValue();

            if (isTopLevelOperator(key)) {
                if (!matchesTopLevelOperator(document, key, expected)) {
                    return false;
                }
                continue;
            }

            PathResolution path = resolvePathValues(document, key);
            if (!matchesField(path, expected)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTopLevelOperator(String key) {
        return "$and".equals(key)
                || "$or".equals(key)
                || "$not".equals(key)
                || "$nor".equals(key)
                || "$expr".equals(key);
    }

    private static boolean matchesTopLevelOperator(Document document, String operator, Object definition) {
        switch (operator) {
            case "$and":
                for (Document clause : toDocumentList(operator, definition)) {
                    if (!matches(document, clause)) {
                        return false;
                    }
                }
                return true;
            case "$or":
                List<Document> anyClauses = toDocumentList(operator, definition);
                if (anyClauses.isEmpty()) {
                    return false;
                }
                for (Document clause : anyClauses) {
                    if (matches(document, clause)) {
                        return true;
                    }
                }
                return false;
            case "$nor":
                for (Document clause : toDocumentList(operator, definition)) {
                    if (matches(document, clause)) {
                        return false;
                    }
                }
                return true;
            case "$not":
                return !matches(document, toDocument(operator, definition));
            case "$expr":
                return matchesExpression(document, definition);
            default:
                throw new UnsupportedFeatureException(
                        "query.top_level_operator." + operator,
                        "unsupported query operator: " + operator);
        }
    }

    private static boolean matchesExpression(final Document document, final Object expression) {
        return expressionTruthiness(evaluateExpression(document, expression));
    }

    private static Object evaluateExpression(final Document document, final Object expression) {
        if (expression instanceof String pathExpression && pathExpression.startsWith("$")) {
            return resolveExpressionPath(document, pathExpression.substring(1));
        }
        if (expression instanceof Map<?, ?> rawMap) {
            final Map<String, Object> expressionMap = toStringKeyedMap(rawMap, "$expr");
            if (expressionMap.size() == 1) {
                final Map.Entry<String, Object> singleEntry = expressionMap.entrySet().iterator().next();
                if (singleEntry.getKey().startsWith("$")) {
                    return evaluateExpressionOperator(document, singleEntry.getKey(), singleEntry.getValue());
                }
            }

            final Map<String, Object> evaluatedDocument = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : expressionMap.entrySet()) {
                evaluatedDocument.put(entry.getKey(), evaluateExpression(document, entry.getValue()));
            }
            return evaluatedDocument;
        }
        if (expression instanceof List<?> expressionList) {
            final List<Object> evaluated = new ArrayList<>(expressionList.size());
            for (final Object item : expressionList) {
                evaluated.add(evaluateExpression(document, item));
            }
            return evaluated;
        }
        return expression;
    }

    private static Object evaluateExpressionOperator(
            final Document document, final String operator, final Object operand) {
        return switch (operator) {
            case "$eq" -> evaluateExpressionEquality(document, operand, true);
            case "$ne" -> evaluateExpressionEquality(document, operand, false);
            case "$gt" -> evaluateExpressionComparison(document, operand, ComparisonOperator.GT);
            case "$gte" -> evaluateExpressionComparison(document, operand, ComparisonOperator.GTE);
            case "$lt" -> evaluateExpressionComparison(document, operand, ComparisonOperator.LT);
            case "$lte" -> evaluateExpressionComparison(document, operand, ComparisonOperator.LTE);
            case "$and" -> evaluateExpressionAnd(document, operand);
            case "$or" -> evaluateExpressionOr(document, operand);
            case "$not" -> evaluateExpressionNot(document, operand);
            case "$literal" -> operand;
            default -> throw new UnsupportedFeatureException(
                    "query.expr_operator." + operator,
                    "unsupported $expr operator: " + operator);
        };
    }

    private static boolean evaluateExpressionEquality(
            final Document document, final Object operand, final boolean expectedEqual) {
        final List<Object> values = evaluateExpressionArguments(document, operand, 2, 2, "comparison");
        final boolean equals = valueEquals(values.get(0), values.get(1));
        return expectedEqual ? equals : !equals;
    }

    private static boolean evaluateExpressionComparison(
            final Document document, final Object operand, final ComparisonOperator operator) {
        final List<Object> values = evaluateExpressionArguments(document, operand, 2, 2, "comparison");
        final Integer compared = compareValues(values.get(0), values.get(1));
        if (compared == null) {
            return false;
        }
        return switch (operator) {
            case GT -> compared > 0;
            case GTE -> compared >= 0;
            case LT -> compared < 0;
            case LTE -> compared <= 0;
        };
    }

    private static boolean evaluateExpressionAnd(final Document document, final Object operand) {
        final List<Object> values = evaluateExpressionArguments(document, operand, 1, null, "$and");
        for (final Object value : values) {
            if (!expressionTruthiness(value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean evaluateExpressionOr(final Document document, final Object operand) {
        final List<Object> values = evaluateExpressionArguments(document, operand, 1, null, "$or");
        for (final Object value : values) {
            if (expressionTruthiness(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean evaluateExpressionNot(final Document document, final Object operand) {
        final List<Object> values = evaluateExpressionArguments(document, operand, 1, 1, "$not");
        return !expressionTruthiness(values.get(0));
    }

    private static List<Object> evaluateExpressionArguments(
            final Document document,
            final Object operand,
            final int minSize,
            final Integer exactSize,
            final String operatorName) {
        if (!(operand instanceof List<?> rawOperands)) {
            throw new IllegalArgumentException("$expr " + operatorName + " requires an array argument");
        }
        if (exactSize != null && rawOperands.size() != exactSize) {
            throw new IllegalArgumentException("$expr " + operatorName + " requires exactly " + exactSize + " arguments");
        }
        if (rawOperands.size() < minSize) {
            throw new IllegalArgumentException("$expr " + operatorName + " requires at least " + minSize + " arguments");
        }

        final List<Object> values = new ArrayList<>(rawOperands.size());
        for (final Object rawOperand : rawOperands) {
            values.add(evaluateExpression(document, rawOperand));
        }
        return values;
    }

    private static boolean expressionTruthiness(final Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value instanceof Number numberValue) {
            final double numeric = numberValue.doubleValue();
            return Double.isFinite(numeric) && numeric != 0d;
        }
        if (value instanceof CharSequence charSequence) {
            return charSequence.length() > 0;
        }
        return true;
    }

    private static Object resolveExpressionPath(final Document document, final String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }

        final String[] segments = path.split("\\.");
        Object current = document;
        for (final String segment : segments) {
            if (!(current instanceof Map<?, ?> mapValue) || !mapValue.containsKey(segment)) {
                return null;
            }
            current = mapValue.get(segment);
        }
        return current;
    }

    private static List<Document> toDocumentList(String operator, Object rawValue) {
        if (!(rawValue instanceof List<?> rawList)) {
            throw new IllegalArgumentException(operator + " requires an array of documents");
        }

        List<Document> documents = new ArrayList<>(rawList.size());
        for (Object item : rawList) {
            documents.add(toDocument(operator, item));
        }
        return documents;
    }

    private static Document toDocument(String operator, Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException(operator + " requires a document");
        }

        Document document = new Document();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("query key must be a string");
            }
            document.put(key, entry.getValue());
        }
        return document;
    }

    private static boolean matchesField(PathResolution path, Object expected) {
        if (isOperatorDocument(expected)) {
            return matchesOperatorDocument(path, toStringKeyedMap((Map<?, ?>) expected, "operator document"));
        }
        if (expected instanceof Pattern pattern) {
            return matchesRegex(path, pattern);
        }
        if (expected instanceof BsonRegularExpression bsonPattern) {
            return matchesRegex(path, compileRegex(bsonPattern, null));
        }
        return matchesEq(path, expected);
    }

    private static boolean matchesOperatorDocument(PathResolution path, Map<String, Object> operators) {
        if (operators.containsKey("$options") && !operators.containsKey("$regex")) {
            throw new IllegalArgumentException("$options requires $regex");
        }

        if (operators.containsKey("$regex")) {
            Pattern pattern = compileRegex(operators.get("$regex"), operators.get("$options"));
            if (!matchesRegex(path, pattern)) {
                return false;
            }
        }

        for (Map.Entry<String, Object> entry : operators.entrySet()) {
            String operator = entry.getKey();
            Object operand = entry.getValue();

            if ("$regex".equals(operator) || "$options".equals(operator)) {
                continue;
            }

            boolean matched;
            switch (operator) {
                case "$eq":
                    matched = matchesEq(path, operand);
                    break;
                case "$ne":
                    matched = matchesNe(path, operand);
                    break;
                case "$gt":
                    matched = matchesComparison(path, operand, ComparisonOperator.GT);
                    break;
                case "$gte":
                    matched = matchesComparison(path, operand, ComparisonOperator.GTE);
                    break;
                case "$lt":
                    matched = matchesComparison(path, operand, ComparisonOperator.LT);
                    break;
                case "$lte":
                    matched = matchesComparison(path, operand, ComparisonOperator.LTE);
                    break;
                case "$in":
                    matched = matchesIn(path, operand);
                    break;
                case "$nin":
                    matched = matchesNin(path, operand);
                    break;
                case "$exists":
                    matched = matchesExists(path, operand);
                    break;
                case "$type":
                    matched = matchesType(path, operand);
                    break;
                case "$size":
                    matched = matchesSize(path, operand);
                    break;
                case "$elemMatch":
                    matched = matchesElemMatch(path, operand);
                    break;
                case "$all":
                    matched = matchesAll(path, operand);
                    break;
                case "$not":
                    matched = matchesFieldNot(path, operand);
                    break;
                default:
                    throw new UnsupportedFeatureException(
                            "query.field_operator." + operator,
                            "unsupported query operator: " + operator);
            }

            if (!matched) {
                return false;
            }
        }

        return true;
    }

    private static PathResolution resolvePathValues(Document document, String key) {
        if (key == null || key.isEmpty()) {
            return PathResolution.missing();
        }

        String[] segments = key.split("\\.");
        List<Object> values = new ArrayList<>();
        collectPathValues(document, segments, 0, values);
        if (values.isEmpty()) {
            return PathResolution.missing();
        }
        return PathResolution.existing(values);
    }

    private static void collectPathValues(
            Object current, String[] segments, int segmentIndex, List<Object> values) {
        if (segmentIndex == segments.length) {
            values.add(current);
            return;
        }

        if (current instanceof Map<?, ?> map) {
            String segment = segments[segmentIndex];
            if (!map.containsKey(segment)) {
                return;
            }
            collectPathValues(map.get(segment), segments, segmentIndex + 1, values);
            return;
        }

        if (current instanceof List<?> list) {
            for (Object item : list) {
                collectPathValues(item, segments, segmentIndex, values);
            }
        }
    }

    private static boolean matchesEq(PathResolution path, Object expected) {
        if (!path.exists()) {
            return expected == null;
        }

        for (Object actual : path.values()) {
            if (matchesEqSingle(actual, expected)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesEqSingle(Object actual, Object expected) {
        if (expected instanceof Pattern pattern) {
            return matchesRegexCandidate(actual, pattern);
        }
        if (expected instanceof BsonRegularExpression bsonPattern) {
            return matchesRegexCandidate(actual, compileRegex(bsonPattern, null));
        }

        if (actual instanceof List<?> actualList && isSimpleScalar(expected)) {
            for (Object item : actualList) {
                if (valueEquals(item, expected)) {
                    return true;
                }
            }
            return false;
        }
        return valueEquals(actual, expected);
    }

    private static boolean matchesNe(PathResolution path, Object expected) {
        return !matchesEq(path, expected);
    }

    private static boolean matchesComparison(
            PathResolution path, Object expected, ComparisonOperator operator) {
        if (!path.exists()) {
            return false;
        }

        for (Object actual : path.values()) {
            if (matchesComparisonCandidate(actual, expected, operator)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesComparisonCandidate(
            Object actual, Object expected, ComparisonOperator operator) {
        if (actual instanceof List<?> list) {
            for (Object item : list) {
                if (matchesComparisonSingle(item, expected, operator)) {
                    return true;
                }
            }
            return false;
        }
        return matchesComparisonSingle(actual, expected, operator);
    }

    private static boolean matchesComparisonSingle(
            Object actual, Object expected, ComparisonOperator operator) {
        Integer comparison = compareValues(actual, expected);
        if (comparison == null) {
            return false;
        }

        return switch (operator) {
            case GT -> comparison > 0;
            case GTE -> comparison >= 0;
            case LT -> comparison < 0;
            case LTE -> comparison <= 0;
        };
    }

    private static Integer compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return null;
        }

        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return compareNumbers(leftNumber, rightNumber);
        }
        if (left instanceof String leftString && right instanceof String rightString) {
            return leftString.compareTo(rightString);
        }
        if (left.getClass().equals(right.getClass()) && left instanceof Comparable<?> comparableLeft) {
            @SuppressWarnings("unchecked")
            Comparable<Object> castComparable = (Comparable<Object>) comparableLeft;
            return castComparable.compareTo(right);
        }
        return null;
    }

    private static int compareNumbers(Number left, Number right) {
        if (isSpecialFloating(left) || isSpecialFloating(right)) {
            return Double.compare(left.doubleValue(), right.doubleValue());
        }

        try {
            return toBigDecimal(left).compareTo(toBigDecimal(right));
        } catch (NumberFormatException error) {
            return Double.compare(left.doubleValue(), right.doubleValue());
        }
    }

    private static boolean isSpecialFloating(Number value) {
        if (value instanceof Double doubleValue) {
            return Double.isNaN(doubleValue) || Double.isInfinite(doubleValue);
        }
        if (value instanceof Float floatValue) {
            return Float.isNaN(floatValue) || Float.isInfinite(floatValue);
        }
        return false;
    }

    private static BigDecimal toBigDecimal(Number value) {
        if (value instanceof BigDecimal bigDecimal) {
            return bigDecimal;
        }
        if (value instanceof BigInteger bigInteger) {
            return new BigDecimal(bigInteger);
        }
        if (value instanceof Decimal128 decimal128) {
            return decimal128.bigDecimalValue();
        }
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return BigDecimal.valueOf(value.longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            return BigDecimal.valueOf(value.doubleValue());
        }
        return new BigDecimal(value.toString());
    }

    private static boolean matchesIn(PathResolution path, Object operand) {
        List<Object> candidates = asOperandList("$in", operand);
        if (candidates.isEmpty()) {
            return false;
        }

        if (!path.exists()) {
            return containsNull(candidates);
        }

        for (Object candidate : candidates) {
            if (matchesEq(path, candidate)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesNin(PathResolution path, Object operand) {
        List<Object> candidates = asOperandList("$nin", operand);
        if (!path.exists()) {
            return true;
        }

        for (Object candidate : candidates) {
            if (matchesEq(path, candidate)) {
                return false;
            }
        }
        return true;
    }

    private static List<Object> asOperandList(String operator, Object operand) {
        if (operand instanceof List<?> listOperand) {
            return new ArrayList<>(listOperand);
        }
        if (operand != null && operand.getClass().isArray()) {
            int length = Array.getLength(operand);
            List<Object> values = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                values.add(Array.get(operand, i));
            }
            return values;
        }
        throw new IllegalArgumentException(operator + " requires an array");
    }

    private static boolean containsNull(List<Object> values) {
        for (Object value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesExists(PathResolution path, Object operand) {
        if (!(operand instanceof Boolean expected)) {
            throw new IllegalArgumentException("$exists requires a boolean");
        }
        return expected == path.exists();
    }

    private static boolean matchesType(PathResolution path, Object operand) {
        TypePredicate predicate = parseTypePredicate(operand);
        if (!path.exists()) {
            return false;
        }

        for (Object actual : path.values()) {
            if (predicate.matches(actual)) {
                return true;
            }
            if (actual instanceof List<?> list) {
                for (Object item : list) {
                    if (predicate.matches(item)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static TypePredicate parseTypePredicate(Object operand) {
        if (operand instanceof Number numberOperand) {
            return parseTypeCode(numberOperand.intValue());
        }
        if (operand instanceof String stringOperand) {
            String normalized = stringOperand.toLowerCase();
            return switch (normalized) {
                case "number" -> value -> value instanceof Number;
                case "double" -> value -> value instanceof Double || value instanceof Float;
                case "string" -> value -> value instanceof String;
                case "object" -> value -> value instanceof Map<?, ?> && !(value instanceof List<?>);
                case "array" -> value -> value instanceof List<?>;
                case "bool", "boolean" -> value -> value instanceof Boolean;
                case "date" -> value -> value instanceof java.util.Date || value instanceof Instant;
                case "null" -> value -> value == null;
                case "regex" -> value -> value instanceof Pattern || value instanceof BsonRegularExpression;
                case "int" -> value -> value instanceof Integer;
                case "long" -> value -> value instanceof Long;
                case "decimal" -> value -> value instanceof Decimal128 || value instanceof BigDecimal;
                case "objectid" -> value -> value instanceof ObjectId;
                default -> throw new IllegalArgumentException("unsupported $type value: " + operand);
            };
        }
        throw new IllegalArgumentException("$type requires a string alias or numeric code");
    }

    private static TypePredicate parseTypeCode(int code) {
        return switch (code) {
            case 1 -> value -> value instanceof Double || value instanceof Float;
            case 2 -> value -> value instanceof String;
            case 3 -> value -> value instanceof Map<?, ?> && !(value instanceof List<?>);
            case 4 -> value -> value instanceof List<?>;
            case 8 -> value -> value instanceof Boolean;
            case 9 -> value -> value instanceof java.util.Date || value instanceof Instant;
            case 10 -> value -> value == null;
            case 11 -> value -> value instanceof Pattern || value instanceof BsonRegularExpression;
            case 16 -> value -> value instanceof Integer;
            case 18 -> value -> value instanceof Long;
            case 19 -> value -> value instanceof Decimal128 || value instanceof BigDecimal;
            default -> throw new IllegalArgumentException("unsupported $type code: " + code);
        };
    }

    private static boolean matchesSize(PathResolution path, Object operand) {
        int expectedSize = parseNonNegativeInt("$size", operand);
        if (!path.exists()) {
            return false;
        }

        for (Object actual : path.values()) {
            if (actual instanceof List<?> list && list.size() == expectedSize) {
                return true;
            }
        }
        return false;
    }

    private static int parseNonNegativeInt(String operator, Object operand) {
        if (!(operand instanceof Number numberOperand)) {
            throw new IllegalArgumentException(operator + " requires a numeric value");
        }

        int value;
        try {
            value = toBigDecimal(numberOperand).intValueExact();
        } catch (ArithmeticException error) {
            throw new IllegalArgumentException(operator + " requires an integer value");
        }

        if (value < 0) {
            throw new IllegalArgumentException(operator + " requires a non-negative value");
        }
        return value;
    }

    private static boolean matchesElemMatch(PathResolution path, Object operand) {
        if (!(operand instanceof Map<?, ?> rawCriteria)) {
            throw new IllegalArgumentException("$elemMatch requires a document");
        }

        if (!path.exists()) {
            return false;
        }

        for (Object actual : path.values()) {
            if (!(actual instanceof List<?> list)) {
                continue;
            }

            for (Object element : list) {
                if (matchesElemMatchElement(element, rawCriteria)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean matchesElemMatchElement(Object element, Map<?, ?> rawCriteria) {
        if (isOperatorDocument(rawCriteria)) {
            return matchesOperatorDocument(
                    PathResolution.existing(Collections.singletonList(element)),
                    toStringKeyedMap(rawCriteria, "$elemMatch criteria"));
        }

        if (!(element instanceof Map<?, ?> elementMap)) {
            return false;
        }

        return matches(toDocument("$elemMatch element", elementMap), toDocument("$elemMatch", rawCriteria));
    }

    private static boolean matchesAll(PathResolution path, Object operand) {
        List<Object> requiredValues = asOperandList("$all", operand);
        if (!path.exists()) {
            return false;
        }

        for (Object actual : path.values()) {
            if (!(actual instanceof List<?> list)) {
                continue;
            }
            if (containsAllValues(list, requiredValues)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAllValues(List<?> actualValues, List<Object> requiredValues) {
        for (Object required : requiredValues) {
            if (!containsForAll(actualValues, required)) {
                return false;
            }
        }
        return true;
    }

    private static boolean containsForAll(List<?> actualValues, Object required) {
        if (required instanceof Map<?, ?> requiredMap
                && requiredMap.size() == 1
                && requiredMap.containsKey("$elemMatch")) {
            Object elemMatchOperand = requiredMap.get("$elemMatch");
            if (!(elemMatchOperand instanceof Map<?, ?> elemMatchCriteria)) {
                throw new IllegalArgumentException("$all $elemMatch requires a document");
            }
            for (Object actual : actualValues) {
                if (matchesElemMatchElement(actual, elemMatchCriteria)) {
                    return true;
                }
            }
            return false;
        }

        Pattern pattern;
        if (required instanceof Pattern rawPattern) {
            pattern = rawPattern;
        } else if (required instanceof BsonRegularExpression bsonPattern) {
            pattern = compileRegex(bsonPattern, null);
        } else {
            pattern = null;
        }
        if (pattern != null) {
            for (Object actual : actualValues) {
                if (matchesRegexCandidate(actual, pattern)) {
                    return true;
                }
            }
            return false;
        }

        for (Object actual : actualValues) {
            if (valueEquals(actual, required)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesFieldNot(PathResolution path, Object operand) {
        if (operand instanceof Map<?, ?> mapOperand && isOperatorDocument(mapOperand)) {
            return !matchesOperatorDocument(path, toStringKeyedMap(mapOperand, "$not"));
        }
        if (operand instanceof Pattern pattern) {
            return !matchesRegex(path, pattern);
        }
        if (operand instanceof BsonRegularExpression bsonPattern) {
            return !matchesRegex(path, compileRegex(bsonPattern, null));
        }
        if (operand instanceof String stringPattern) {
            return !matchesRegex(path, Pattern.compile(stringPattern));
        }
        throw new IllegalArgumentException("$not requires an operator document or regex");
    }

    private static boolean matchesRegex(PathResolution path, Pattern pattern) {
        if (!path.exists()) {
            return false;
        }

        for (Object actual : path.values()) {
            if (matchesRegexCandidate(actual, pattern)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesRegexCandidate(Object actual, Pattern pattern) {
        if (actual instanceof List<?> list) {
            for (Object item : list) {
                if (matchesRegexCandidate(item, pattern)) {
                    return true;
                }
            }
            return false;
        }

        if (!(actual instanceof CharSequence sequence)) {
            return false;
        }
        return pattern.matcher(sequence).find();
    }

    private static Pattern compileRegex(Object rawPattern, Object rawOptions) {
        int flags = parseRegexFlags(rawOptions);
        String expression;

        if (rawPattern instanceof Pattern pattern) {
            expression = pattern.pattern();
            flags |= pattern.flags();
        } else if (rawPattern instanceof BsonRegularExpression bsonPattern) {
            expression = bsonPattern.getPattern();
            flags |= parseRegexFlags(bsonPattern.getOptions());
        } else if (rawPattern instanceof String stringPattern) {
            expression = stringPattern;
        } else {
            throw new IllegalArgumentException("$regex requires a regex pattern");
        }

        return Pattern.compile(expression, flags);
    }

    private static int parseRegexFlags(Object rawOptions) {
        if (rawOptions == null) {
            return 0;
        }
        if (!(rawOptions instanceof String options)) {
            throw new IllegalArgumentException("$options must be a string");
        }

        int flags = 0;
        for (int i = 0; i < options.length(); i++) {
            switch (options.charAt(i)) {
                case 'i':
                    flags |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'm':
                    flags |= Pattern.MULTILINE;
                    break;
                case 's':
                    flags |= Pattern.DOTALL;
                    break;
                case 'x':
                    flags |= Pattern.COMMENTS;
                    break;
                case 'u':
                    flags |= Pattern.UNICODE_CASE;
                    break;
                default:
                    throw new IllegalArgumentException("unsupported regex option: " + options.charAt(i));
            }
        }
        return flags;
    }

    private static Map<String, Object> toStringKeyedMap(Map<?, ?> rawMap, String context) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException(context + " key must be a string");
            }
            result.put(key, entry.getValue());
        }
        return result;
    }

    private static boolean isOperatorDocument(Object value) {
        if (!(value instanceof Map<?, ?> mapValue) || mapValue.isEmpty()) {
            return false;
        }

        for (Object key : mapValue.keySet()) {
            if (!(key instanceof String keyString) || !keyString.startsWith("$")) {
                return false;
            }
        }
        return true;
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

    private enum ComparisonOperator {
        GT,
        GTE,
        LT,
        LTE
    }

    private interface TypePredicate {
        boolean matches(Object value);
    }

    private record PathResolution(boolean exists, List<Object> values) {
        private static PathResolution missing() {
            return new PathResolution(false, List.of());
        }

        private static PathResolution existing(List<Object> values) {
            return new PathResolution(true, values);
        }
    }
}
