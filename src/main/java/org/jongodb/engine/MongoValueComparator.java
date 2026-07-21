package org.jongodb.engine;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

/** MongoDB-style BSON value ordering shared by update and aggregation operators. */
final class MongoValueComparator {
    private MongoValueComparator() {}

    static int compare(final Object left, final Object right) {
        return compare(left, right, CollationSupport.Config.simple());
    }

    static int compare(
            final Object left,
            final Object right,
            final CollationSupport.Config collation) {
        if (left == right) {
            return 0;
        }

        final int leftRank = typeRank(left);
        final int rightRank = typeRank(right);
        if (leftRank != rightRank) {
            return Integer.compare(leftRank, rightRank);
        }

        if (left == null || left instanceof MinKey || left instanceof MaxKey) {
            return 0;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return compareNumbers(leftNumber, rightNumber);
        }
        if (left instanceof CharSequence leftText && right instanceof CharSequence rightText) {
            return collation.compareStrings(leftText.toString(), rightText.toString());
        }
        if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
            return compareMaps(leftMap, rightMap, collation);
        }
        if (isArrayLike(left) && isArrayLike(right)) {
            return compareLists(asList(left), asList(right), collation);
        }
        if (isBinary(left) && isBinary(right)) {
            return compareBytes(binaryBytes(left), binaryBytes(right));
        }
        if (left instanceof ObjectId leftObjectId && right instanceof ObjectId rightObjectId) {
            return compareBytes(leftObjectId.toByteArray(), rightObjectId.toByteArray());
        }
        if (left instanceof Boolean leftBoolean && right instanceof Boolean rightBoolean) {
            return Boolean.compare(leftBoolean, rightBoolean);
        }
        if (isDateLike(left) && isDateLike(right)) {
            return Long.compare(dateMillis(left), dateMillis(right));
        }
        if (left instanceof BsonTimestamp leftTimestamp && right instanceof BsonTimestamp rightTimestamp) {
            final int timeComparison = Integer.compare(leftTimestamp.getTime(), rightTimestamp.getTime());
            return timeComparison != 0
                    ? timeComparison
                    : Integer.compare(leftTimestamp.getInc(), rightTimestamp.getInc());
        }
        if (isRegex(left) && isRegex(right)) {
            return regexValue(left).compareTo(regexValue(right));
        }

        if (left.getClass().equals(right.getClass()) && left instanceof Comparable<?> comparable) {
            @SuppressWarnings("unchecked")
            final Comparable<Object> typed = (Comparable<Object>) comparable;
            return typed.compareTo(right);
        }
        return stableValue(left).compareTo(stableValue(right));
    }

    static boolean equals(final Object left, final Object right) {
        return typeRank(left) == typeRank(right) && compare(left, right) == 0;
    }

    static int hash(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number number) {
            if (isSpecialFloating(number)) {
                return Double.hashCode(number.doubleValue());
            }
            try {
                return toBigDecimal(number).stripTrailingZeros().hashCode();
            } catch (final ArithmeticException | NumberFormatException ignored) {
                return Double.hashCode(number.doubleValue());
            }
        }
        if (value instanceof Map<?, ?> map) {
            int hash = 1;
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                hash = 31 * hash + String.valueOf(entry.getKey()).hashCode();
                hash = 31 * hash + hash(entry.getValue());
            }
            return hash;
        }
        if (isArrayLike(value)) {
            int hash = 1;
            for (final Object item : asList(value)) {
                hash = 31 * hash + hash(item);
            }
            return hash;
        }
        if (isDateLike(value)) {
            return Long.hashCode(dateMillis(value));
        }
        if (isBinary(value)) {
            return java.util.Arrays.hashCode(binaryBytes(value));
        }
        return value.hashCode();
    }

    private static int compareNumbers(final Number left, final Number right) {
        if (isSpecialFloating(left) || isSpecialFloating(right)) {
            return Double.compare(left.doubleValue(), right.doubleValue());
        }
        try {
            return toBigDecimal(left).compareTo(toBigDecimal(right));
        } catch (final ArithmeticException | NumberFormatException ignored) {
            return Double.compare(left.doubleValue(), right.doubleValue());
        }
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
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(value.longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            return BigDecimal.valueOf(value.doubleValue());
        }
        return new BigDecimal(value.toString());
    }

    private static boolean isSpecialFloating(final Number value) {
        return (value instanceof Double doubleValue && !Double.isFinite(doubleValue))
                || (value instanceof Float floatValue && !Float.isFinite(floatValue));
    }

    private static int compareMaps(
            final Map<?, ?> left,
            final Map<?, ?> right,
            final CollationSupport.Config collation) {
        final Iterator<? extends Map.Entry<?, ?>> leftEntries = left.entrySet().iterator();
        final Iterator<? extends Map.Entry<?, ?>> rightEntries = right.entrySet().iterator();
        while (leftEntries.hasNext() && rightEntries.hasNext()) {
            final Map.Entry<?, ?> leftEntry = leftEntries.next();
            final Map.Entry<?, ?> rightEntry = rightEntries.next();
            final int typeComparison = Integer.compare(typeRank(leftEntry.getValue()), typeRank(rightEntry.getValue()));
            if (typeComparison != 0) {
                return typeComparison;
            }
            final int keyComparison = String.valueOf(leftEntry.getKey()).compareTo(String.valueOf(rightEntry.getKey()));
            if (keyComparison != 0) {
                return keyComparison;
            }
            final int valueComparison = compare(leftEntry.getValue(), rightEntry.getValue(), collation);
            if (valueComparison != 0) {
                return valueComparison;
            }
        }
        return Boolean.compare(leftEntries.hasNext(), rightEntries.hasNext());
    }

    private static int compareLists(
            final List<?> left,
            final List<?> right,
            final CollationSupport.Config collation) {
        final int commonSize = Math.min(left.size(), right.size());
        for (int index = 0; index < commonSize; index++) {
            final int comparison = compare(left.get(index), right.get(index), collation);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.size(), right.size());
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        final int commonLength = Math.min(left.length, right.length);
        for (int index = 0; index < commonLength; index++) {
            final int comparison = Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static boolean isArrayLike(final Object value) {
        return value instanceof List<?> || (value != null && value.getClass().isArray() && !(value instanceof byte[]));
    }

    private static List<Object> asList(final Object value) {
        if (value instanceof List<?> list) {
            return new ArrayList<>(list);
        }
        final int length = Array.getLength(value);
        final List<Object> output = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            output.add(Array.get(value, index));
        }
        return output;
    }

    private static boolean isBinary(final Object value) {
        return value instanceof Binary || value instanceof byte[];
    }

    private static byte[] binaryBytes(final Object value) {
        return value instanceof Binary binary ? binary.getData() : (byte[]) value;
    }

    private static boolean isDateLike(final Object value) {
        return value instanceof Date || value instanceof Instant;
    }

    private static long dateMillis(final Object value) {
        return value instanceof Date date ? date.getTime() : ((Instant) value).toEpochMilli();
    }

    private static boolean isRegex(final Object value) {
        return value instanceof Pattern || value instanceof BsonRegularExpression;
    }

    private static String regexValue(final Object value) {
        if (value instanceof Pattern pattern) {
            return pattern.pattern() + '\u0000' + pattern.flags();
        }
        final BsonRegularExpression expression = (BsonRegularExpression) value;
        return expression.getPattern() + '\u0000' + expression.getOptions();
    }

    private static int typeRank(final Object value) {
        if (value instanceof MinKey) {
            return 0;
        }
        if (value == null) {
            return 1;
        }
        if (value instanceof Number) {
            return 2;
        }
        if (value instanceof CharSequence || value instanceof Character) {
            return 3;
        }
        if (value instanceof Map<?, ?>) {
            return 4;
        }
        if (isArrayLike(value)) {
            return 5;
        }
        if (isBinary(value)) {
            return 6;
        }
        if (value instanceof ObjectId) {
            return 7;
        }
        if (value instanceof Boolean) {
            return 8;
        }
        if (isDateLike(value)) {
            return 9;
        }
        if (value instanceof BsonTimestamp) {
            return 10;
        }
        if (isRegex(value)) {
            return 11;
        }
        if (value instanceof MaxKey) {
            return 100;
        }
        return 50;
    }

    private static String stableValue(final Object value) {
        return value.getClass().getName() + ':' + value;
    }
}
