package org.jongodb.engine;

import java.lang.reflect.Array;
import java.text.Collator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

public final class CollationSupport {
    private static final Set<String> SUPPORTED_OPTIONS = Set.of("locale", "strength", "caseLevel");

    private CollationSupport() {}

    public static boolean isSupportedOption(final String optionName) {
        return SUPPORTED_OPTIONS.contains(optionName);
    }

    public static final class Config {
        private static final Config SIMPLE = new Config("simple", 3, false, true, null);

        private final String localeTag;
        private final int strength;
        private final boolean caseLevel;
        private final boolean simpleBinary;
        private final Collator collator;

        private Config(
                final String localeTag,
                final int strength,
                final boolean caseLevel,
                final boolean simpleBinary,
                final Collator collator) {
            this.localeTag = localeTag;
            this.strength = strength;
            this.caseLevel = caseLevel;
            this.simpleBinary = simpleBinary;
            this.collator = collator;
        }

        public static Config simple() {
            return SIMPLE;
        }

        public static Config fromBsonDocument(final BsonDocument collation) {
            if (collation == null || collation.isEmpty()) {
                return SIMPLE;
            }

            final String locale = readLocale(collation.get("locale"));
            final int strength = readStrength(collation.get("strength"));
            final boolean caseLevel = readCaseLevel(collation.get("caseLevel"));
            return create(locale, strength, caseLevel);
        }

        public static Config fromDocument(final Document collation) {
            if (collation == null || collation.isEmpty()) {
                return SIMPLE;
            }

            final String locale = readLocale(collation.get("locale"));
            final int strength = readStrength(collation.get("strength"));
            final boolean caseLevel = readCaseLevel(collation.get("caseLevel"));
            return create(locale, strength, caseLevel);
        }

        public String localeTag() {
            return localeTag;
        }

        public int strength() {
            return strength;
        }

        public boolean caseLevel() {
            return caseLevel;
        }

        public int compareStrings(final String left, final String right) {
            if (simpleBinary) {
                return left.compareTo(right);
            }
            final int compared = collator.compare(left, right);
            if (compared != 0) {
                return compared;
            }
            if (caseLevel && !left.equals(right)) {
                return left.compareTo(right);
            }
            return 0;
        }

        public boolean valuesEqual(final Object left, final Object right) {
            if (left == right) {
                return true;
            }
            if (left == null || right == null) {
                return false;
            }
            if (left instanceof String leftString && right instanceof String rightString) {
                return compareStrings(leftString, rightString) == 0;
            }
            if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
                if (leftMap.size() != rightMap.size()) {
                    return false;
                }
                for (final Map.Entry<?, ?> entry : leftMap.entrySet()) {
                    final Object key = entry.getKey();
                    if (!rightMap.containsKey(key)) {
                        return false;
                    }
                    if (!valuesEqual(entry.getValue(), rightMap.get(key))) {
                        return false;
                    }
                }
                return true;
            }
            if (left instanceof List<?> leftList && right instanceof List<?> rightList) {
                if (leftList.size() != rightList.size()) {
                    return false;
                }
                for (int i = 0; i < leftList.size(); i++) {
                    if (!valuesEqual(leftList.get(i), rightList.get(i))) {
                        return false;
                    }
                }
                return true;
            }
            if (left.getClass().isArray() && right.getClass().isArray()) {
                final int leftLength = Array.getLength(left);
                final int rightLength = Array.getLength(right);
                if (leftLength != rightLength) {
                    return false;
                }
                for (int i = 0; i < leftLength; i++) {
                    if (!valuesEqual(Array.get(left, i), Array.get(right, i))) {
                        return false;
                    }
                }
                return true;
            }
            return Objects.deepEquals(left, right);
        }

        private static Config create(final String locale, final int strength, final boolean caseLevel) {
            if ("simple".equalsIgnoreCase(locale)) {
                return SIMPLE;
            }

            final Locale parsedLocale = Locale.forLanguageTag(locale);
            final Collator collator = Collator.getInstance(parsedLocale);
            collator.setStrength(toCollatorStrength(strength));
            collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
            return new Config(parsedLocale.toLanguageTag(), strength, caseLevel, false, collator);
        }

        private static String readLocale(final Object localeValue) {
            if (localeValue == null) {
                return "simple";
            }
            if (localeValue instanceof BsonValue bsonValue) {
                if (!bsonValue.isString()) {
                    throw new IllegalArgumentException("collation.locale must be a string");
                }
                final String locale = bsonValue.asString().getValue();
                if (locale == null || locale.isBlank()) {
                    throw new IllegalArgumentException("collation.locale must not be blank");
                }
                return locale;
            }
            if (localeValue instanceof String locale) {
                if (locale.isBlank()) {
                    throw new IllegalArgumentException("collation.locale must not be blank");
                }
                return locale;
            }
            throw new IllegalArgumentException("collation.locale must be a string");
        }

        private static int readStrength(final Object strengthValue) {
            if (strengthValue == null) {
                return 3;
            }

            final Long parsed = parseIntegralLong(strengthValue);
            if (parsed == null) {
                throw new IllegalArgumentException("collation.strength must be an integer");
            }
            if (parsed < 1 || parsed > 4) {
                throw new IllegalArgumentException("collation.strength must be between 1 and 4");
            }
            return parsed.intValue();
        }

        private static boolean readCaseLevel(final Object caseLevelValue) {
            if (caseLevelValue == null) {
                return false;
            }
            if (caseLevelValue instanceof BsonValue bsonValue) {
                if (!bsonValue.isBoolean()) {
                    throw new IllegalArgumentException("collation.caseLevel must be a boolean");
                }
                return bsonValue.asBoolean().getValue();
            }
            if (caseLevelValue instanceof Boolean booleanValue) {
                return booleanValue;
            }
            throw new IllegalArgumentException("collation.caseLevel must be a boolean");
        }

        private static int toCollatorStrength(final int strength) {
            return switch (strength) {
                case 1 -> Collator.PRIMARY;
                case 2 -> Collator.SECONDARY;
                case 3 -> Collator.TERTIARY;
                case 4 -> Collator.IDENTICAL;
                default -> throw new IllegalArgumentException("collation.strength must be between 1 and 4");
            };
        }

        private static Long parseIntegralLong(final Object value) {
            if (value instanceof BsonValue bsonValue) {
                if (bsonValue.isInt32()) {
                    return (long) bsonValue.asInt32().getValue();
                }
                if (bsonValue.isInt64()) {
                    return bsonValue.asInt64().getValue();
                }
                if (bsonValue.isDouble()) {
                    final double doubleValue = bsonValue.asDouble().getValue();
                    if (!Double.isFinite(doubleValue) || Math.rint(doubleValue) != doubleValue) {
                        return null;
                    }
                    if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                        return null;
                    }
                    return (long) doubleValue;
                }
                return null;
            }

            if (value instanceof Number numberValue) {
                final double doubleValue = numberValue.doubleValue();
                if (!Double.isFinite(doubleValue) || Math.rint(doubleValue) != doubleValue) {
                    return null;
                }
                if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                    return null;
                }
                return (long) doubleValue;
            }
            return null;
        }
    }
}
