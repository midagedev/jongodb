package org.jongodb.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.bson.Document;

public final class AggregationPipeline {
    @FunctionalInterface
    public interface CollectionResolver {
        Iterable<Document> resolve(String collectionName);
    }

    private static final CollectionResolver UNSUPPORTED_COLLECTION_RESOLVER =
            collectionName -> {
                throw new UnsupportedFeatureException(
                        "aggregation.cross_collection_resolver",
                        "stage requires collection resolver: " + requireText(collectionName, "collectionName"));
            };

    private static final Set<String> GRAPH_LOOKUP_SUPPORTED_OPTIONS = Set.of(
            "from",
            "startWith",
            "connectFromField",
            "connectToField",
            "as",
            "maxDepth");

    private AggregationPipeline() {}

    public static List<Document> execute(final List<Document> source, final List<Document> pipeline) {
        return execute(source, pipeline, UNSUPPORTED_COLLECTION_RESOLVER, CollationSupport.Config.simple());
    }

    public static List<Document> execute(final Iterable<Document> source, final List<Document> pipeline) {
        return execute(source, pipeline, UNSUPPORTED_COLLECTION_RESOLVER, CollationSupport.Config.simple());
    }

    public static List<Document> execute(
            final List<Document> source,
            final List<Document> pipeline,
            final CollectionResolver collectionResolver) {
        return execute(source, pipeline, collectionResolver, CollationSupport.Config.simple());
    }

    public static List<Document> execute(
            final Iterable<Document> source,
            final List<Document> pipeline,
            final CollectionResolver collectionResolver) {
        return execute(source, pipeline, collectionResolver, CollationSupport.Config.simple());
    }

    public static List<Document> execute(
            final List<Document> source,
            final List<Document> pipeline,
            final CollectionResolver collectionResolver,
            final CollationSupport.Config collation) {
        return execute((Iterable<Document>) source, pipeline, collectionResolver, collation);
    }

    public static List<Document> execute(
            final Iterable<Document> source,
            final List<Document> pipeline,
            final CollectionResolver collectionResolver,
            final CollationSupport.Config collation) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(collectionResolver, "collectionResolver");
        Objects.requireNonNull(collation, "collation");

        List<Document> working = materializeDocuments(source, "source documents must not contain null");

        for (final Document stage : pipeline) {
            if (stage == null) {
                throw new IllegalArgumentException("pipeline stages must not contain null");
            }
            if (stage.size() != 1) {
                throw new IllegalArgumentException("each pipeline stage must contain exactly one field");
            }

            final String stageName = stage.keySet().iterator().next();
            final Object stageDefinition = stage.get(stageName);
            working = switch (stageName) {
                case "$match" -> applyMatch(working, stageDefinition, collation);
                case "$project" -> applyProject(working, stageDefinition);
                case "$group" -> applyGroup(working, stageDefinition);
                case "$sort" -> applySort(working, stageDefinition, collation);
                case "$limit" -> applyLimit(working, stageDefinition);
                case "$skip" -> applySkip(working, stageDefinition);
                case "$unwind" -> applyUnwind(working, stageDefinition);
                case "$count" -> applyCount(working, stageDefinition);
                case "$addFields" -> applyAddFields(working, stageDefinition);
                case "$set" -> applySet(working, stageDefinition);
                case "$unset" -> applyUnset(working, stageDefinition);
                case "$sortByCount" -> applySortByCount(working, stageDefinition, collation);
                case "$replaceRoot" -> applyReplaceRoot(working, stageDefinition);
                case "$replaceWith" -> applyReplaceWith(working, stageDefinition);
                case "$facet" -> applyFacet(working, stageDefinition, collectionResolver, collation);
                case "$lookup" -> applyLookup(working, stageDefinition, collectionResolver, collation);
                case "$graphLookup" -> applyGraphLookup(working, stageDefinition, collectionResolver);
                case "$unionWith" -> applyUnionWith(working, stageDefinition, collectionResolver, collation);
                case "$setWindowFields" -> applySetWindowFields(working, stageDefinition, collation);
                default -> throw new UnsupportedFeatureException(
                        "aggregation.stage." + stageName,
                        "unsupported aggregation stage: " + stageName);
            };
        }

        final List<Document> output = new ArrayList<>(working.size());
        for (final Document document : working) {
            output.add(DocumentCopies.copy(document));
        }
        return List.copyOf(output);
    }

    private static List<Document> applyMatch(
            final List<Document> input,
            final Object stageDefinition,
            final CollationSupport.Config collation) {
        final Document filter = requireDocument(stageDefinition, "$match stage requires a document");
        final List<Document> output = new ArrayList<>();
        for (final Document document : input) {
            if (QueryMatcher.matches(document, filter, collation)) {
                output.add(DocumentCopies.copy(document));
            }
        }
        return output;
    }

    private static List<Document> applyProject(final List<Document> input, final Object stageDefinition) {
        final Document projection = requireDocument(stageDefinition, "$project stage requires a document");
        if (projection.isEmpty()) {
            throw new IllegalArgumentException("$project stage must not be empty");
        }

        final ProjectionMode mode = projectionMode(projection);
        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            output.add(mode == ProjectionMode.EXCLUDE
                    ? applyExclusionProjection(source, projection)
                    : applyInclusionProjection(source, projection));
        }
        return output;
    }

    private static ProjectionMode projectionMode(final Document projection) {
        boolean hasIncludeOrExpression = false;
        boolean hasExclude = false;
        for (final Map.Entry<String, Object> entry : projection.entrySet()) {
            if ("_id".equals(entry.getKey())) {
                continue;
            }
            final ProjectionFlag flag = parseProjectionFlag(entry.getValue());
            if (flag == ProjectionFlag.EXCLUDE) {
                hasExclude = true;
            } else {
                hasIncludeOrExpression = true;
            }
        }

        if (hasIncludeOrExpression && hasExclude) {
            throw new IllegalArgumentException("$project cannot mix inclusion and exclusion");
        }
        return hasExclude ? ProjectionMode.EXCLUDE : ProjectionMode.INCLUDE;
    }

    private static Document applyInclusionProjection(final Document source, final Document projection) {
        final Document output = new Document();
        final boolean includeId = includeIdInProjection(projection, true);
        if (includeId && source.containsKey("_id")) {
            output.put("_id", DocumentCopies.copyAny(source.get("_id")));
        }

        for (final Map.Entry<String, Object> entry : projection.entrySet()) {
            final String field = entry.getKey();
            if ("_id".equals(field)) {
                continue;
            }

            final ProjectionFlag flag = parseProjectionFlag(entry.getValue());
            if (flag == ProjectionFlag.EXCLUDE) {
                throw new IllegalArgumentException("$project cannot mix inclusion and exclusion");
            }

            if (flag == ProjectionFlag.INCLUDE) {
                final PathValue pathValue = resolvePath(source, field);
                if (pathValue.present()) {
                    output.put(field, DocumentCopies.copyAny(pathValue.value()));
                }
                continue;
            }

            final Object evaluated = evaluateExpression(source, entry.getValue());
            if (!AggregationExpressions.isMissing(evaluated)) {
                output.put(field, evaluated);
            }
        }
        return output;
    }

    private static Document applyExclusionProjection(final Document source, final Document projection) {
        final Document output = DocumentCopies.copy(source);
        final boolean includeId = includeIdInProjection(projection, true);
        if (!includeId) {
            output.remove("_id");
        }

        for (final Map.Entry<String, Object> entry : projection.entrySet()) {
            final String field = entry.getKey();
            if ("_id".equals(field)) {
                continue;
            }

            final ProjectionFlag flag = parseProjectionFlag(entry.getValue());
            if (flag != ProjectionFlag.EXCLUDE) {
                throw new IllegalArgumentException("$project cannot mix exclusion and inclusion");
            }
            removePath(output, field);
        }
        return output;
    }

    private static boolean includeIdInProjection(final Document projection, final boolean defaultValue) {
        if (!projection.containsKey("_id")) {
            return defaultValue;
        }
        final ProjectionFlag flag = parseProjectionFlag(projection.get("_id"));
        if (flag == ProjectionFlag.EXPRESSION) {
            throw new IllegalArgumentException("$project _id must be 0 or 1");
        }
        return flag == ProjectionFlag.INCLUDE;
    }

    private static ProjectionFlag parseProjectionFlag(final Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue ? ProjectionFlag.INCLUDE : ProjectionFlag.EXCLUDE;
        }
        if (value instanceof Number numberValue) {
            final double numeric = numberValue.doubleValue();
            if (!Double.isFinite(numeric) || Math.rint(numeric) != numeric) {
                throw new IllegalArgumentException("$project numeric projections must be 0 or 1");
            }
            if (numeric == 0d) {
                return ProjectionFlag.EXCLUDE;
            }
            if (numeric == 1d) {
                return ProjectionFlag.INCLUDE;
            }
            throw new IllegalArgumentException("$project numeric projections must be 0 or 1");
        }
        return ProjectionFlag.EXPRESSION;
    }

    private static List<Document> applyGroup(final List<Document> input, final Object stageDefinition) {
        final Document groupDefinition = requireDocument(stageDefinition, "$group stage requires a document");
        if (!groupDefinition.containsKey("_id")) {
            throw new IllegalArgumentException("$group requires an _id field");
        }

        final Object idExpression = groupDefinition.get("_id");
        final List<GroupAccumulator> accumulators = parseAccumulators(groupDefinition);
        final Map<GroupKey, GroupBucket> grouped = new LinkedHashMap<>();
        for (final Document source : input) {
            final Object id = evaluateGroupId(source, idExpression);
            final GroupKey groupKey = new GroupKey(id);
            grouped.computeIfAbsent(groupKey, ignored -> new GroupBucket(id)).sources().add(source);
        }

        final List<Document> output = new ArrayList<>(grouped.size());
        for (final GroupBucket bucket : grouped.values()) {
            final Document aggregate = new Document("_id", DocumentCopies.copyAny(bucket.id()));
            for (final GroupAccumulator accumulator : accumulators) {
                aggregate.put(accumulator.outputField(), evaluateAccumulator(accumulator, bucket.sources()));
            }
            output.add(aggregate);
        }
        return List.copyOf(output);
    }

    private static List<GroupAccumulator> parseAccumulators(final Document groupDefinition) {
        final List<GroupAccumulator> accumulators = new ArrayList<>();
        for (final Map.Entry<String, Object> entry : groupDefinition.entrySet()) {
            if ("_id".equals(entry.getKey())) {
                continue;
            }

            final Document accumulatorDefinition =
                    requireDocument(entry.getValue(), "$group accumulator definitions must be documents");
            if (accumulatorDefinition.size() != 1) {
                throw new IllegalArgumentException("$group accumulator must have exactly one operator");
            }

            final String accumulatorName = accumulatorDefinition.keySet().iterator().next();
            final GroupAccumulatorOperator operator = switch (accumulatorName) {
                case "$sum" -> GroupAccumulatorOperator.SUM;
                case "$avg" -> GroupAccumulatorOperator.AVG;
                case "$min" -> GroupAccumulatorOperator.MIN;
                case "$max" -> GroupAccumulatorOperator.MAX;
                case "$first" -> GroupAccumulatorOperator.FIRST;
                case "$last" -> GroupAccumulatorOperator.LAST;
                case "$push" -> GroupAccumulatorOperator.PUSH;
                case "$addToSet" -> GroupAccumulatorOperator.ADD_TO_SET;
                case "$mergeObjects" -> GroupAccumulatorOperator.MERGE_OBJECTS;
                case "$percentile" -> GroupAccumulatorOperator.PERCENTILE;
                case "$median" -> GroupAccumulatorOperator.MEDIAN;
                case "$firstN" -> GroupAccumulatorOperator.FIRST_N;
                case "$lastN" -> GroupAccumulatorOperator.LAST_N;
                case "$minN" -> GroupAccumulatorOperator.MIN_N;
                case "$maxN" -> GroupAccumulatorOperator.MAX_N;
                case "$topN" -> GroupAccumulatorOperator.TOP_N;
                case "$bottomN" -> GroupAccumulatorOperator.BOTTOM_N;
                default -> throw new UnsupportedFeatureException(
                        "aggregation.group.accumulator." + accumulatorName,
                        "unsupported $group accumulator: " + accumulatorName);
            };
            accumulators.add(new GroupAccumulator(entry.getKey(), operator, accumulatorDefinition.get(accumulatorName)));
        }
        return List.copyOf(accumulators);
    }

    private static Object evaluateGroupExpression(final Document source, final Object expression) {
        return AggregationExpressions.nullIfMissing(evaluateExpression(source, expression));
    }

    private enum GroupAccumulatorOperator {
        SUM,
        AVG,
        MIN,
        MAX,
        FIRST,
        LAST,
        PUSH,
        ADD_TO_SET,
        MERGE_OBJECTS,
        PERCENTILE,
        MEDIAN,
        FIRST_N,
        LAST_N,
        MIN_N,
        MAX_N,
        TOP_N,
        BOTTOM_N
    }

    private record GroupAccumulator(String outputField, GroupAccumulatorOperator operator, Object expression) {}

    private static Object evaluateGroupId(final Document source, final Object expression) {
        return AggregationExpressions.nullIfMissing(evaluateExpression(source, expression));
    }

    private static Number addNumbers(final Object currentValue, final Number increment) {
        if (!(currentValue instanceof Number currentNumber)) {
            return normalizeNumber(increment.doubleValue());
        }
        return normalizeNumber(currentNumber.doubleValue() + increment.doubleValue());
    }

    private static boolean containsByMongoEquality(final List<Object> values, final Object candidate) {
        for (final Object value : values) {
            if (MongoValueComparator.equals(value, candidate)) {
                return true;
            }
        }
        return false;
    }

    private static Object evaluateAccumulator(
            final GroupAccumulator accumulator, final List<Document> sources) {
        return switch (accumulator.operator()) {
            case SUM -> accumulatorSum(sources, accumulator.expression());
            case AVG -> accumulatorAverage(sources, accumulator.expression());
            case MIN -> accumulatorMinMax(sources, accumulator.expression(), false);
            case MAX -> accumulatorMinMax(sources, accumulator.expression(), true);
            case FIRST -> sources.isEmpty()
                    ? null
                    : evaluateGroupExpression(sources.get(0), accumulator.expression());
            case LAST -> sources.isEmpty()
                    ? null
                    : evaluateGroupExpression(sources.get(sources.size() - 1), accumulator.expression());
            case PUSH -> accumulatorPush(sources, accumulator.expression(), false);
            case ADD_TO_SET -> accumulatorPush(sources, accumulator.expression(), true);
            case MERGE_OBJECTS -> accumulatorMergeObjects(sources, accumulator.expression());
            case PERCENTILE -> accumulatorPercentile(sources, accumulator.expression(), false);
            case MEDIAN -> accumulatorPercentile(sources, accumulator.expression(), true);
            case FIRST_N -> accumulatorPositionalN(sources, accumulator.expression(), false);
            case LAST_N -> accumulatorPositionalN(sources, accumulator.expression(), true);
            case MIN_N -> accumulatorMinMaxN(sources, accumulator.expression(), false);
            case MAX_N -> accumulatorMinMaxN(sources, accumulator.expression(), true);
            case TOP_N -> accumulatorTopBottomN(sources, accumulator.expression(), false);
            case BOTTOM_N -> accumulatorTopBottomN(sources, accumulator.expression(), true);
        };
    }

    private static Object accumulatorSum(final List<Document> sources, final Object expression) {
        Number sum = 0L;
        for (final Document source : sources) {
            final Object value = evaluateGroupExpression(source, expression);
            if (value instanceof Number number) {
                sum = addNumbers(sum, number);
            }
        }
        return sum;
    }

    private static Object accumulatorAverage(final List<Document> sources, final Object expression) {
        double sum = 0d;
        long count = 0L;
        for (final Document source : sources) {
            final Object value = evaluateGroupExpression(source, expression);
            if (value instanceof Number number) {
                sum += number.doubleValue();
                count++;
            }
        }
        return count == 0 ? null : sum / count;
    }

    private static Object accumulatorMinMax(
            final List<Document> sources, final Object expression, final boolean maximum) {
        Object selected = null;
        boolean found = false;
        for (final Document source : sources) {
            final Object value = evaluateExpression(source, expression);
            if (value == null || AggregationExpressions.isMissing(value)) {
                continue;
            }
            if (!found) {
                selected = value;
                found = true;
                continue;
            }
            final int comparison = MongoValueComparator.compare(value, selected);
            if ((maximum && comparison > 0) || (!maximum && comparison < 0)) {
                selected = value;
            }
        }
        return found ? DocumentCopies.copyAny(selected) : null;
    }

    private static Object accumulatorPush(
            final List<Document> sources, final Object expression, final boolean unique) {
        final List<Object> output = new ArrayList<>();
        for (final Document source : sources) {
            final Object value = evaluateGroupExpression(source, expression);
            if (!unique || !containsByMongoEquality(output, value)) {
                output.add(DocumentCopies.copyAny(value));
            }
        }
        return output;
    }

    private static Object accumulatorMergeObjects(
            final List<Document> sources, final Object expression) {
        final Document merged = new Document();
        for (final Document source : sources) {
            final Object value = evaluateGroupExpression(source, expression);
            if (value == null) {
                continue;
            }
            if (!(value instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("$mergeObjects accumulator requires document values");
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

    private static Object accumulatorPercentile(
            final List<Document> sources, final Object rawDefinition, final boolean median) {
        final Document definition = requireDocument(
                rawDefinition,
                median ? "$median requires a document" : "$percentile requires a document");
        final Set<String> supported = median ? Set.of("input", "method") : Set.of("input", "p", "method");
        if (!supported.containsAll(definition.keySet())
                || !definition.containsKey("input")
                || !definition.containsKey("method")
                || (!median && !definition.containsKey("p"))) {
            throw new IllegalArgumentException(
                    median
                            ? "$median requires input and method"
                            : "$percentile requires input, p, and method");
        }
        if (!"approximate".equals(definition.get("method"))) {
            throw new IllegalArgumentException("percentile method must be 'approximate'");
        }

        final List<Double> samples = new ArrayList<>();
        for (final Document source : sources) {
            final Object value = evaluateGroupExpression(source, definition.get("input"));
            if (value instanceof Number number && Double.isFinite(number.doubleValue())) {
                samples.add(number.doubleValue());
            }
        }
        samples.sort(Double::compare);
        if (median) {
            return exactPercentile(samples, 0.5d);
        }

        if (!(definition.get("p") instanceof List<?> rawPercentiles) || rawPercentiles.isEmpty()) {
            throw new IllegalArgumentException("$percentile.p must be a non-empty array");
        }
        final List<Object> percentiles = new ArrayList<>(rawPercentiles.size());
        for (final Object rawPercentile : rawPercentiles) {
            if (!(rawPercentile instanceof Number number)
                    || !Double.isFinite(number.doubleValue())
                    || number.doubleValue() < 0d
                    || number.doubleValue() > 1d) {
                throw new IllegalArgumentException("$percentile.p entries must be numbers in [0, 1]");
            }
            percentiles.add(exactPercentile(samples, number.doubleValue()));
        }
        return percentiles;
    }

    private static Object exactPercentile(final List<Double> sorted, final double percentile) {
        if (sorted.isEmpty()) {
            return null;
        }
        if (sorted.size() == 1) {
            return sorted.get(0);
        }
        final double rank = percentile * (sorted.size() - 1d);
        final int lower = (int) Math.floor(rank);
        final int upper = (int) Math.ceil(rank);
        if (lower == upper) {
            return sorted.get(lower);
        }
        final double fraction = rank - lower;
        return sorted.get(lower) + (sorted.get(upper) - sorted.get(lower)) * fraction;
    }

    private static Object accumulatorPositionalN(
            final List<Document> sources, final Object rawDefinition, final boolean fromEnd) {
        final Document definition = requireNDefinition(rawDefinition, fromEnd ? "$lastN" : "$firstN", Set.of("input", "n"));
        final int n = evaluateN(definition.get("n"), sources, fromEnd ? "$lastN" : "$firstN");
        final int start = fromEnd ? Math.max(0, sources.size() - n) : 0;
        final int end = fromEnd ? sources.size() : Math.min(sources.size(), n);
        final List<Object> output = new ArrayList<>(Math.max(0, end - start));
        for (int index = start; index < end; index++) {
            output.add(DocumentCopies.copyAny(evaluateGroupExpression(sources.get(index), definition.get("input"))));
        }
        return output;
    }

    private static Object accumulatorMinMaxN(
            final List<Document> sources, final Object rawDefinition, final boolean maximum) {
        final String operator = maximum ? "$maxN" : "$minN";
        final Document definition = requireNDefinition(rawDefinition, operator, Set.of("input", "n"));
        final int n = evaluateN(definition.get("n"), sources, operator);
        final List<Object> values = new ArrayList<>();
        for (final Document source : sources) {
            final Object value = evaluateGroupExpression(source, definition.get("input"));
            if (value != null) {
                values.add(value);
            }
        }
        values.sort((left, right) -> maximum
                ? MongoValueComparator.compare(right, left)
                : MongoValueComparator.compare(left, right));
        return new ArrayList<>(values.subList(0, Math.min(n, values.size())));
    }

    private static Object accumulatorTopBottomN(
            final List<Document> sources, final Object rawDefinition, final boolean bottom) {
        final String operator = bottom ? "$bottomN" : "$topN";
        final Document definition = requireNDefinition(rawDefinition, operator, Set.of("output", "sortBy", "n"));
        final Document sortBy = requireDocument(definition.get("sortBy"), operator + ".sortBy must be a document");
        if (sortBy.isEmpty()) {
            throw new IllegalArgumentException(operator + ".sortBy must not be empty");
        }
        final List<SortKey> sortKeys = new ArrayList<>();
        for (final Map.Entry<String, Object> entry : sortBy.entrySet()) {
            final int direction = parseSortDirection(entry.getValue());
            if (direction != 1 && direction != -1) {
                throw new IllegalArgumentException(operator + ".sortBy directions must be 1 or -1");
            }
            sortKeys.add(new SortKey(entry.getKey(), direction));
        }
        final int n = evaluateN(definition.get("n"), sources, operator);
        final List<Document> sorted = new ArrayList<>(sources);
        sorted.sort((left, right) -> compareSortDocuments(
                left, right, sortKeys, CollationSupport.Config.simple()));
        final List<Object> output = new ArrayList<>();
        final int start = bottom ? Math.max(0, sorted.size() - n) : 0;
        final int end = bottom ? sorted.size() : Math.min(n, sorted.size());
        for (int index = start; index < end; index++) {
            output.add(DocumentCopies.copyAny(evaluateGroupExpression(sorted.get(index), definition.get("output"))));
        }
        return output;
    }

    private static Document requireNDefinition(
            final Object rawDefinition, final String operator, final Set<String> requiredFields) {
        final Document definition = requireDocument(rawDefinition, operator + " requires a document");
        if (!definition.keySet().equals(requiredFields)) {
            throw new IllegalArgumentException(operator + " requires exactly " + requiredFields);
        }
        return definition;
    }

    private static int evaluateN(
            final Object expression, final List<Document> sources, final String operator) {
        final Document context = sources.isEmpty() ? new Document() : sources.get(0);
        final Object value = evaluateGroupExpression(context, expression);
        if (!(value instanceof Number number)
                || !Double.isFinite(number.doubleValue())
                || Math.rint(number.doubleValue()) != number.doubleValue()
                || number.doubleValue() < 1d
                || number.doubleValue() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(operator + ".n must evaluate to a positive integer");
        }
        return number.intValue();
    }

    private static final class GroupBucket {
        private final Object id;
        private final List<Document> sources = new ArrayList<>();

        private GroupBucket(final Object id) {
            this.id = DocumentCopies.copyAny(id);
        }

        private Object id() {
            return id;
        }

        private List<Document> sources() {
            return sources;
        }
    }

    private static Number normalizeNumber(final double value) {
        if (!Double.isFinite(value)) {
            return value;
        }
        if (Math.rint(value) == value
                && value >= Long.MIN_VALUE
                && value <= Long.MAX_VALUE) {
            return (long) value;
        }
        return value;
    }

    private static List<Document> applySort(
            final List<Document> input,
            final Object stageDefinition,
            final CollationSupport.Config collation) {
        final Document sortDefinition = requireDocument(stageDefinition, "$sort stage requires a document");
        if (sortDefinition.isEmpty()) {
            throw new IllegalArgumentException("$sort stage must not be empty");
        }

        final List<SortKey> sortKeys = new ArrayList<>(sortDefinition.size());
        for (final Map.Entry<String, Object> entry : sortDefinition.entrySet()) {
            final int direction = parseSortDirection(entry.getValue());
            if (direction != 1 && direction != -1) {
                throw new IllegalArgumentException("$sort directions must be 1 or -1");
            }
            sortKeys.add(new SortKey(entry.getKey(), direction));
        }

        final List<Document> sorted = new ArrayList<>(input);
        sorted.sort((left, right) -> compareSortDocuments(left, right, sortKeys, collation));
        return sorted;
    }

    private static List<Document> applySetWindowFields(
            final List<Document> input,
            final Object stageDefinition,
            final CollationSupport.Config collation) {
        final Document definition =
                requireDocument(stageDefinition, "$setWindowFields stage requires a document");
        if (!Set.of("partitionBy", "sortBy", "output").containsAll(definition.keySet())
                || !definition.containsKey("output")) {
            throw new IllegalArgumentException(
                    "$setWindowFields supports partitionBy, sortBy, and requires output");
        }
        final Document outputDefinition =
                requireDocument(definition.get("output"), "$setWindowFields.output must be a document");
        if (outputDefinition.isEmpty()) {
            throw new IllegalArgumentException("$setWindowFields.output must not be empty");
        }

        final List<SortKey> sortKeys = new ArrayList<>();
        if (definition.containsKey("sortBy")) {
            final Document sortDefinition =
                    requireDocument(definition.get("sortBy"), "$setWindowFields.sortBy must be a document");
            if (sortDefinition.isEmpty()) {
                throw new IllegalArgumentException("$setWindowFields.sortBy must not be empty");
            }
            for (final Map.Entry<String, Object> entry : sortDefinition.entrySet()) {
                final int direction = parseSortDirection(entry.getValue());
                if (direction != 1 && direction != -1) {
                    throw new IllegalArgumentException("$setWindowFields.sortBy directions must be 1 or -1");
                }
                sortKeys.add(new SortKey(entry.getKey(), direction));
            }
        }

        final Map<GroupKey, List<Document>> partitions = new LinkedHashMap<>();
        for (final Document source : input) {
            final Object partitionValue = definition.containsKey("partitionBy")
                    ? AggregationExpressions.nullIfMissing(evaluateExpression(source, definition.get("partitionBy")))
                    : null;
            partitions.computeIfAbsent(new GroupKey(partitionValue), ignored -> new ArrayList<>()).add(source);
        }

        final List<Document> output = new ArrayList<>(input.size());
        for (final List<Document> partition : partitions.values()) {
            if (!sortKeys.isEmpty()) {
                partition.sort((left, right) -> compareSortDocuments(left, right, sortKeys, collation));
            }
            applyWindowOutputs(partition, outputDefinition, sortKeys, collation, output);
        }
        return List.copyOf(output);
    }

    private static void applyWindowOutputs(
            final List<Document> partition,
            final Document outputDefinition,
            final List<SortKey> sortKeys,
            final CollationSupport.Config collation,
            final List<Document> output) {
        long rank = 1L;
        long denseRank = 1L;
        for (int index = 0; index < partition.size(); index++) {
            if (index > 0 && !sortKeys.isEmpty()) {
                final boolean changed = compareSortDocuments(
                                partition.get(index - 1), partition.get(index), sortKeys, collation)
                        != 0;
                if (changed) {
                    rank = index + 1L;
                    denseRank++;
                }
            }

            final Document expanded = DocumentCopies.copy(partition.get(index));
            for (final Map.Entry<String, Object> outputEntry : outputDefinition.entrySet()) {
                final String outputPath = requireText(outputEntry.getKey(), "$setWindowFields output field");
                final Document operatorDefinition = requireDocument(
                        outputEntry.getValue(), "$setWindowFields output definitions must be documents");
                if (operatorDefinition.size() != 1) {
                    throw new IllegalArgumentException(
                            "$setWindowFields output definition must contain exactly one operator");
                }
                final String operator = operatorDefinition.keySet().iterator().next();
                final Object value = switch (operator) {
                    case "$shift" -> evaluateShift(
                            partition, index, operatorDefinition.get(operator), sortKeys);
                    case "$documentNumber" -> {
                        requireEmptyWindowOperator(operatorDefinition.get(operator), "$documentNumber");
                        if (sortKeys.isEmpty()) {
                            throw new IllegalArgumentException("$documentNumber requires sortBy");
                        }
                        yield index + 1L;
                    }
                    case "$rank" -> {
                        requireEmptyWindowOperator(operatorDefinition.get(operator), "$rank");
                        if (sortKeys.isEmpty()) {
                            throw new IllegalArgumentException("$rank requires sortBy");
                        }
                        yield rank;
                    }
                    case "$denseRank" -> {
                        requireEmptyWindowOperator(operatorDefinition.get(operator), "$denseRank");
                        if (sortKeys.isEmpty()) {
                            throw new IllegalArgumentException("$denseRank requires sortBy");
                        }
                        yield denseRank;
                    }
                    default -> throw new UnsupportedFeatureException(
                            "aggregation.setWindowFields.operator." + operator,
                            "unsupported $setWindowFields operator: " + operator);
                };
                if (AggregationExpressions.isMissing(value)) {
                    removePath(expanded, outputPath);
                } else {
                    setPath(expanded, outputPath, value);
                }
            }
            output.add(expanded);
        }
    }

    private static Object evaluateShift(
            final List<Document> partition,
            final int index,
            final Object rawDefinition,
            final List<SortKey> sortKeys) {
        if (sortKeys.isEmpty()) {
            throw new IllegalArgumentException("$shift requires sortBy");
        }
        final Document definition = requireDocument(rawDefinition, "$shift requires a document");
        if (!Set.of("output", "by", "default").containsAll(definition.keySet())
                || !definition.containsKey("output")
                || !definition.containsKey("by")) {
            throw new IllegalArgumentException("$shift requires output and by, with optional default");
        }
        final Object byValue = definition.get("by");
        if (!(byValue instanceof Number number)
                || !Double.isFinite(number.doubleValue())
                || Math.rint(number.doubleValue()) != number.doubleValue()
                || number.doubleValue() < Integer.MIN_VALUE
                || number.doubleValue() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("$shift.by must be a non-zero integer constant");
        }
        final int targetIndex = index + number.intValue();
        if (targetIndex < 0 || targetIndex >= partition.size()) {
            if (!definition.containsKey("default")) {
                return null;
            }
            if (containsFieldReference(definition.get("default"))) {
                throw new IllegalArgumentException("$shift.default must be a constant expression");
            }
            return evaluateExpression(partition.get(index), definition.get("default"));
        }
        return evaluateExpression(partition.get(targetIndex), definition.get("output"));
    }

    private static void requireEmptyWindowOperator(final Object value, final String operator) {
        if (!(value instanceof Map<?, ?> map) || !map.isEmpty()) {
            throw new IllegalArgumentException(operator + " requires an empty document");
        }
    }

    private static boolean containsFieldReference(final Object value) {
        if (value instanceof String stringValue) {
            return stringValue.startsWith("$") && !stringValue.startsWith("$$");
        }
        if (value instanceof Map<?, ?> map) {
            for (final Object nested : map.values()) {
                if (containsFieldReference(nested)) {
                    return true;
                }
            }
        }
        if (value instanceof List<?> list) {
            for (final Object nested : list) {
                if (containsFieldReference(nested)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static int parseSortDirection(final Object value) {
        if (!(value instanceof Number numberValue)) {
            throw new IllegalArgumentException("$sort directions must be numeric");
        }
        final double numeric = numberValue.doubleValue();
        if (!Double.isFinite(numeric) || Math.rint(numeric) != numeric) {
            throw new IllegalArgumentException("$sort directions must be integers");
        }
        if (numeric < Integer.MIN_VALUE || numeric > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("$sort directions are out of range");
        }
        return (int) numeric;
    }

    private static int compareSortDocuments(
            final Document left,
            final Document right,
            final List<SortKey> sortKeys,
            final CollationSupport.Config collation) {
        for (final SortKey sortKey : sortKeys) {
            final Object leftValue = resolvePath(left, sortKey.field()).valueOrNull();
            final Object rightValue = resolvePath(right, sortKey.field()).valueOrNull();
            final int compared = compareSortValues(leftValue, rightValue, collation);
            if (compared != 0) {
                return sortKey.direction() == 1 ? compared : -compared;
            }
        }
        return 0;
    }

    private static int compareSortValues(
            final Object left, final Object right, final CollationSupport.Config collation) {
        return MongoValueComparator.compare(left, right, collation);
    }

    private static int sortTypeRank(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return 1;
        }
        if (value instanceof String) {
            return 2;
        }
        if (value instanceof Document || value instanceof Map<?, ?>) {
            return 3;
        }
        if (value instanceof List<?>) {
            return 4;
        }
        if (value instanceof Boolean) {
            return 5;
        }
        return 6;
    }

    private static String stableSortValue(final Object value) {
        if (value instanceof Document document) {
            return document.toJson();
        }
        if (value instanceof Map<?, ?> mapValue) {
            return copyMapToDocument(mapValue).toJson();
        }
        return value.getClass().getName() + ":" + String.valueOf(value);
    }

    private static List<Document> applyLimit(final List<Document> input, final Object stageDefinition) {
        final int limit = readNonNegativeInt(stageDefinition, "$limit must be a non-negative integer");
        if (limit == 0) {
            return List.of();
        }
        if (limit >= input.size()) {
            return List.copyOf(input);
        }
        return List.copyOf(new ArrayList<>(input.subList(0, limit)));
    }

    private static List<Document> applySkip(final List<Document> input, final Object stageDefinition) {
        final int skip = readNonNegativeInt(stageDefinition, "$skip must be a non-negative integer");
        if (skip <= 0) {
            return List.copyOf(input);
        }
        if (skip >= input.size()) {
            return List.of();
        }
        return List.copyOf(new ArrayList<>(input.subList(skip, input.size())));
    }

    private static int readNonNegativeInt(final Object value, final String errorMessage) {
        if (!(value instanceof Number numberValue)) {
            throw new IllegalArgumentException(errorMessage);
        }
        final double numeric = numberValue.doubleValue();
        if (!Double.isFinite(numeric) || Math.rint(numeric) != numeric) {
            throw new IllegalArgumentException(errorMessage);
        }
        if (numeric < 0 || numeric > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(errorMessage);
        }
        return (int) numeric;
    }

    private static List<Document> applyUnwind(final List<Document> input, final Object stageDefinition) {
        final UnwindSpec unwindSpec = parseUnwindSpec(stageDefinition);
        final List<Document> output = new ArrayList<>();
        for (final Document source : input) {
            final PathValue pathValue = resolvePath(source, unwindSpec.path());
            if (!pathValue.present() || pathValue.value() == null) {
                if (unwindSpec.preserveNullAndEmptyArrays()) {
                    final Document preserved = DocumentCopies.copy(source);
                    setUnwindArrayIndex(preserved, unwindSpec, null);
                    output.add(preserved);
                }
                continue;
            }

            if (pathValue.value() instanceof List<?> listValue) {
                if (listValue.isEmpty()) {
                    if (unwindSpec.preserveNullAndEmptyArrays()) {
                        final Document preserved = DocumentCopies.copy(source);
                        removePath(preserved, unwindSpec.path());
                        setUnwindArrayIndex(preserved, unwindSpec, null);
                        output.add(preserved);
                    }
                    continue;
                }

                for (int index = 0; index < listValue.size(); index++) {
                    final Object item = listValue.get(index);
                    final Document expanded = DocumentCopies.copy(source);
                    setPath(expanded, unwindSpec.path(), item);
                    setUnwindArrayIndex(expanded, unwindSpec, Long.valueOf(index));
                    output.add(expanded);
                }
                continue;
            }

            final Document expanded = DocumentCopies.copy(source);
            setPath(expanded, unwindSpec.path(), pathValue.value());
            setUnwindArrayIndex(expanded, unwindSpec, null);
            output.add(expanded);
        }
        return output;
    }

    private static void setUnwindArrayIndex(
            final Document target,
            final UnwindSpec unwindSpec,
            final Long arrayIndex) {
        if (unwindSpec.includeArrayIndex() != null) {
            setPath(target, unwindSpec.includeArrayIndex(), arrayIndex);
        }
    }

    private static UnwindSpec parseUnwindSpec(final Object stageDefinition) {
        if (stageDefinition instanceof String pathExpression) {
            return new UnwindSpec(unwindPath(pathExpression), false, null);
        }

        final Document unwindDocument = requireDocument(stageDefinition, "$unwind stage requires a string or document");
        final Object pathValue = unwindDocument.get("path");
        if (!(pathValue instanceof String pathExpression)) {
            throw new IllegalArgumentException("$unwind.path must be a string");
        }

        boolean preserveNullAndEmptyArrays = false;
        final Object preserveValue = unwindDocument.get("preserveNullAndEmptyArrays");
        if (preserveValue != null) {
            if (!(preserveValue instanceof Boolean preserve)) {
                throw new IllegalArgumentException("$unwind.preserveNullAndEmptyArrays must be a boolean");
            }
            preserveNullAndEmptyArrays = preserve;
        }
        String includeArrayIndex = null;
        if (unwindDocument.containsKey("includeArrayIndex")) {
            final Object includeArrayIndexValue = unwindDocument.get("includeArrayIndex");
            if (!(includeArrayIndexValue instanceof String includeArrayIndexName)
                    || includeArrayIndexName.isBlank()
                    || includeArrayIndexName.startsWith("$")) {
                throw new IllegalArgumentException(
                        "$unwind.includeArrayIndex must be a non-empty field name that does not start with '$'");
            }
            includeArrayIndex = includeArrayIndexName;
        }
        return new UnwindSpec(unwindPath(pathExpression), preserveNullAndEmptyArrays, includeArrayIndex);
    }

    private static String unwindPath(final String pathExpression) {
        if (pathExpression == null || pathExpression.length() < 2 || !pathExpression.startsWith("$")) {
            throw new IllegalArgumentException("$unwind path must be a non-empty string starting with '$'");
        }
        return pathExpression.substring(1);
    }

    private static List<Document> applyCount(final List<Document> input, final Object stageDefinition) {
        if (!(stageDefinition instanceof String fieldName)
                || fieldName.isBlank()
                || fieldName.startsWith("$")) {
            throw new IllegalArgumentException("$count stage requires a non-empty field name");
        }
        if (input.isEmpty()) {
            return List.of();
        }
        return List.of(new Document(fieldName, input.size()));
    }

    private static List<Document> applyAddFields(final List<Document> input, final Object stageDefinition) {
        return applyFieldAssignmentStage(input, stageDefinition, "$addFields");
    }

    private static List<Document> applySet(final List<Document> input, final Object stageDefinition) {
        return applyFieldAssignmentStage(input, stageDefinition, "$set");
    }

    private static List<Document> applyFieldAssignmentStage(
            final List<Document> input,
            final Object stageDefinition,
            final String stageName) {
        final Document assignments = requireDocument(stageDefinition, stageName + " stage requires a document");
        if (assignments.isEmpty()) {
            throw new IllegalArgumentException(stageName + " stage must not be empty");
        }

        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            final Document expanded = DocumentCopies.copy(source);
            for (final Map.Entry<String, Object> entry : assignments.entrySet()) {
                final String fieldName = requireText(entry.getKey(), stageName + " field");
                final Object value = evaluateExpression(source, entry.getValue());
                if (AggregationExpressions.isMissing(value)) {
                    removePath(expanded, fieldName);
                } else {
                    setPath(expanded, fieldName, value);
                }
            }
            output.add(expanded);
        }
        return List.copyOf(output);
    }

    private static List<Document> applyUnset(final List<Document> input, final Object stageDefinition) {
        final List<String> fieldPaths = parseUnsetFields(stageDefinition);
        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            final Document reduced = DocumentCopies.copy(source);
            for (final String fieldPath : fieldPaths) {
                removePath(reduced, fieldPath);
            }
            output.add(reduced);
        }
        return List.copyOf(output);
    }

    private static List<String> parseUnsetFields(final Object stageDefinition) {
        if (stageDefinition instanceof String fieldPath) {
            return List.of(requireText(fieldPath, "$unset field"));
        }

        if (stageDefinition instanceof List<?> rawFields) {
            if (rawFields.isEmpty()) {
                throw new IllegalArgumentException("$unset stage array must not be empty");
            }
            final List<String> fieldPaths = new ArrayList<>(rawFields.size());
            for (final Object rawField : rawFields) {
                if (!(rawField instanceof String fieldPath)) {
                    throw new IllegalArgumentException("$unset stage array entries must be strings");
                }
                fieldPaths.add(requireText(fieldPath, "$unset field"));
            }
            return List.copyOf(fieldPaths);
        }

        if (stageDefinition instanceof Map<?, ?> rawMap) {
            if (rawMap.isEmpty()) {
                throw new IllegalArgumentException("$unset stage document must not be empty");
            }
            final List<String> fieldPaths = new ArrayList<>(rawMap.size());
            for (final Object rawKey : rawMap.keySet()) {
                if (!(rawKey instanceof String fieldPath)) {
                    throw new IllegalArgumentException("$unset stage document keys must be strings");
                }
                fieldPaths.add(requireText(fieldPath, "$unset field"));
            }
            return List.copyOf(fieldPaths);
        }

        throw new IllegalArgumentException("$unset stage requires a string, array, or document");
    }

    private static List<Document> applySortByCount(
            final List<Document> input,
            final Object stageDefinition,
            final CollationSupport.Config collation) {
        final Map<GroupKey, CountBucket> buckets = new LinkedHashMap<>();
        for (final Document source : input) {
            final Object bucketValue = AggregationExpressions.nullIfMissing(
                    evaluateExpression(source, stageDefinition));
            final GroupKey bucketKey = new GroupKey(bucketValue);
            final CountBucket existing = buckets.get(bucketKey);
            if (existing == null) {
                buckets.put(bucketKey, new CountBucket(DocumentCopies.copyAny(bucketValue), 1L));
                continue;
            }
            existing.increment();
        }

        final List<Document> output = new ArrayList<>(buckets.size());
        for (final CountBucket bucket : buckets.values()) {
            output.add(new Document("_id", bucket.id()).append("count", bucket.count()));
        }

        output.sort((left, right) -> {
            final int countCompared = compareSortValues(right.get("count"), left.get("count"), collation);
            if (countCompared != 0) {
                return countCompared;
            }
            return compareSortValues(left.get("_id"), right.get("_id"), collation);
        });
        return List.copyOf(output);
    }

    private static List<Document> applyReplaceRoot(final List<Document> input, final Object stageDefinition) {
        final Object expression;
        if (stageDefinition instanceof String) {
            expression = stageDefinition;
        } else {
            final Document replaceDefinition =
                    requireDocument(stageDefinition, "$replaceRoot stage requires a document");
            if (replaceDefinition.size() != 1 || !replaceDefinition.containsKey("newRoot")) {
                throw new IllegalArgumentException("$replaceRoot stage requires a single newRoot field");
            }
            expression = replaceDefinition.get("newRoot");
        }
        return applyReplaceRootExpression(input, expression, "$replaceRoot newRoot must evaluate to a document");
    }

    private static List<Document> applyReplaceWith(final List<Document> input, final Object stageDefinition) {
        return applyReplaceRootExpression(input, stageDefinition, "$replaceWith expression must evaluate to a document");
    }

    private static List<Document> applyReplaceRootExpression(
            final List<Document> input,
            final Object expression,
            final String nonDocumentMessage) {
        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            final Object evaluated = evaluateExpression(source, expression);
            if (!(evaluated instanceof Map<?, ?> mapValue)) {
                throw new IllegalArgumentException(nonDocumentMessage);
            }
            output.add(copyMapToDocument(mapValue));
        }
        return List.copyOf(output);
    }

    private static List<Document> applyFacet(
            final List<Document> input,
            final Object stageDefinition,
            final CollectionResolver collectionResolver,
            final CollationSupport.Config collation) {
        final Document facetDefinition = requireDocument(stageDefinition, "$facet stage requires a document");
        if (facetDefinition.isEmpty()) {
            throw new IllegalArgumentException("$facet stage must not be empty");
        }

        final Document facetResult = new Document();
        for (final Map.Entry<String, Object> entry : facetDefinition.entrySet()) {
            final String facetName = requireText(entry.getKey(), "$facet key");
            final List<Document> facetPipeline = toPipelineList(entry.getValue(), "$facet pipeline must be an array");
            final List<Document> facetOutput =
                    execute(input, facetPipeline, collectionResolver, collation);
            facetResult.put(facetName, facetOutput);
        }
        return List.of(facetResult);
    }

    private static List<Document> applyLookup(
            final List<Document> input,
            final Object stageDefinition,
            final CollectionResolver collectionResolver,
            final CollationSupport.Config collation) {
        final Document lookupDefinition = requireDocument(stageDefinition, "$lookup stage requires a document");
        final String from = requireStringField(lookupDefinition, "from", "$lookup.from must be a string");
        final String as = requireStringField(lookupDefinition, "as", "$lookup.as must be a string");

        final String localField = optionalStringField(lookupDefinition, "localField", "$lookup.localField must be a string");
        final String foreignField = optionalStringField(lookupDefinition, "foreignField", "$lookup.foreignField must be a string");
        final List<Document> lookupPipeline =
                lookupDefinition.containsKey("pipeline")
                        ? toPipelineList(lookupDefinition.get("pipeline"), "$lookup.pipeline must be an array")
                        : List.of();
        final Document letDefinition =
                lookupDefinition.containsKey("let")
                        ? requireDocument(lookupDefinition.get("let"), "$lookup.let must be a document")
                        : new Document();

        final boolean hasLocalForeign = localField != null || foreignField != null;
        if (hasLocalForeign && (localField == null || foreignField == null)) {
            throw new IllegalArgumentException("$lookup localField and foreignField must be specified together");
        }
        if (!hasLocalForeign && lookupPipeline.isEmpty()) {
            throw new IllegalArgumentException("$lookup requires localField/foreignField or pipeline");
        }

        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            final List<Document> foreignSource = materializeDocuments(
                    collectionResolver.resolve(from),
                    "$lookup resolver returned null documents");
            List<Document> joined = foreignSource;

            if (hasLocalForeign) {
                final Object localValue = resolvePath(source, localField).valueOrNull();
                final List<Document> matched = new ArrayList<>();
                for (final Document foreignDocument : foreignSource) {
                    final Object foreignValue = resolvePath(foreignDocument, foreignField).valueOrNull();
                    if (lookupValueMatches(localValue, foreignValue)) {
                        matched.add(DocumentCopies.copy(foreignDocument));
                    }
                }
                joined = matched;
            }

            if (!lookupPipeline.isEmpty()) {
                final Map<String, Object> variables = evaluateLookupVariables(source, letDefinition);
                final List<Document> substitutedPipeline = substitutePipelineVariables(lookupPipeline, variables);
                joined = execute(joined, substitutedPipeline, collectionResolver, collation);
            }

            final Document expanded = DocumentCopies.copy(source);
            expanded.put(as, joined);
            output.add(expanded);
        }
        return List.copyOf(output);
    }

    private static List<Document> applyUnionWith(
            final List<Document> input,
            final Object stageDefinition,
            final CollectionResolver collectionResolver,
            final CollationSupport.Config collation) {
        final String collectionName;
        final List<Document> unionPipeline;
        if (stageDefinition instanceof String collectionValue) {
            collectionName = requireText(collectionValue, "$unionWith");
            unionPipeline = List.of();
        } else {
            final Document unionDefinition =
                    requireDocument(stageDefinition, "$unionWith stage requires a string or document");
            collectionName = requireStringField(unionDefinition, "coll", "$unionWith.coll must be a string");
            if (unionDefinition.containsKey("pipeline")) {
                unionPipeline = toPipelineList(unionDefinition.get("pipeline"), "$unionWith.pipeline must be an array");
            } else {
                unionPipeline = List.of();
            }
        }

        final List<Document> combined = materializeDocuments(input, "input must not contain null");
        List<Document> unionSource = materializeDocuments(
                collectionResolver.resolve(collectionName),
                "$unionWith resolver returned null documents");
        if (!unionPipeline.isEmpty()) {
            unionSource = execute(unionSource, unionPipeline, collectionResolver, collation);
        }
        for (final Document document : unionSource) {
            combined.add(DocumentCopies.copy(document));
        }
        return List.copyOf(combined);
    }

    private static List<Document> applyGraphLookup(
            final List<Document> input,
            final Object stageDefinition,
            final CollectionResolver collectionResolver) {
        final Document graphLookupDefinition = requireDocument(stageDefinition, "$graphLookup stage requires a document");
        for (final String option : graphLookupDefinition.keySet()) {
            if (!GRAPH_LOOKUP_SUPPORTED_OPTIONS.contains(option)) {
                throw new UnsupportedFeatureException(
                        "aggregation.graphLookup.option." + option,
                        "unsupported $graphLookup option: " + option);
            }
        }

        final String from = requireStringField(graphLookupDefinition, "from", "$graphLookup.from must be a string");
        if (!graphLookupDefinition.containsKey("startWith")) {
            throw new IllegalArgumentException("$graphLookup.startWith is required");
        }
        final Object startWithExpression = graphLookupDefinition.get("startWith");
        final String connectFromField = requireStringField(
                graphLookupDefinition,
                "connectFromField",
                "$graphLookup.connectFromField must be a string");
        final String connectToField = requireStringField(
                graphLookupDefinition,
                "connectToField",
                "$graphLookup.connectToField must be a string");
        final String as = requireStringField(graphLookupDefinition, "as", "$graphLookup.as must be a string");
        final int maxDepth = parseGraphLookupMaxDepth(graphLookupDefinition.get("maxDepth"));

        final List<Document> foreignSource = materializeDocuments(
                collectionResolver.resolve(from),
                "$graphLookup resolver returned null documents");
        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            final Deque<GraphLookupFrontier> frontier = new ArrayDeque<>();
            for (final Object seed : graphLookupValues(evaluateExpression(source, startWithExpression))) {
                frontier.addLast(new GraphLookupFrontier(seed, 0));
            }

            final Set<GroupKey> visited = new LinkedHashSet<>();
            final List<Document> graph = new ArrayList<>();
            while (!frontier.isEmpty()) {
                final GraphLookupFrontier current = frontier.removeFirst();
                for (final Document candidate : foreignSource) {
                    final Object connectToValue = resolvePath(candidate, connectToField).valueOrNull();
                    if (!lookupValueMatches(current.value(), connectToValue)) {
                        continue;
                    }

                    final GroupKey candidateKey = new GroupKey(candidate);
                    if (!visited.add(candidateKey)) {
                        continue;
                    }

                    graph.add(DocumentCopies.copy(candidate));
                    if (current.depth() >= maxDepth) {
                        continue;
                    }

                    final Object nextValues = resolvePath(candidate, connectFromField).valueOrNull();
                    for (final Object nextValue : graphLookupValues(nextValues)) {
                        frontier.addLast(new GraphLookupFrontier(nextValue, current.depth() + 1));
                    }
                }
            }

            final Document expanded = DocumentCopies.copy(source);
            expanded.put(as, graph);
            output.add(expanded);
        }
        return List.copyOf(output);
    }

    private static List<Object> graphLookupValues(final Object value) {
        if (value instanceof List<?> listValue) {
            final List<Object> values = new ArrayList<>(listValue.size());
            for (final Object item : listValue) {
                values.add(DocumentCopies.copyAny(item));
            }
            return Collections.unmodifiableList(values);
        }
        return Collections.singletonList(DocumentCopies.copyAny(value));
    }

    private static int parseGraphLookupMaxDepth(final Object value) {
        if (value == null) {
            return Integer.MAX_VALUE;
        }
        if (!(value instanceof Number numericValue)) {
            throw new IllegalArgumentException("$graphLookup.maxDepth must be a non-negative integer");
        }
        final double numeric = numericValue.doubleValue();
        if (!Double.isFinite(numeric)
                || Math.rint(numeric) != numeric
                || numeric < 0d
                || numeric > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("$graphLookup.maxDepth must be a non-negative integer");
        }
        return (int) numeric;
    }

    private static Object evaluateExpression(final Document source, final Object expression) {
        return AggregationExpressions.evaluate(source, expression);
    }

    private static PathValue resolvePath(final Object source, final String path) {
        if (path == null || path.isEmpty()) {
            return PathValue.missing();
        }

        final String[] segments = path.split("\\.");
        Object current = source;
        for (final String segment : segments) {
            if (!(current instanceof Map<?, ?> mapValue) || !mapValue.containsKey(segment)) {
                return PathValue.missing();
            }
            current = mapValue.get(segment);
        }
        return PathValue.present(current);
    }

    private static void removePath(final Document target, final String path) {
        if (path == null || path.isEmpty()) {
            return;
        }

        final String[] segments = path.split("\\.");
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) target;
        for (int index = 0; index < segments.length - 1; index++) {
            final Object next = current.get(segments[index]);
            if (!(next instanceof Map<?, ?> nextMap)) {
                return;
            }
            current = castStringMap(nextMap);
        }
        current.remove(segments[segments.length - 1]);
    }

    private static void setPath(final Document target, final String path, final Object value) {
        final String[] segments = path.split("\\.");
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) target;
        for (int index = 0; index < segments.length - 1; index++) {
            final String segment = segments[index];
            final Object existing = current.get(segment);
            if (!(existing instanceof Map<?, ?> existingMap)) {
                final Document created = new Document();
                current.put(segment, created);
                current = created;
                continue;
            }
            current = castStringMap(existingMap);
        }
        current.put(segments[segments.length - 1], DocumentCopies.copyAny(value));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castStringMap(final Map<?, ?> source) {
        return (Map<String, Object>) source;
    }

    private static Document requireDocument(final Object value, final String errorMessage) {
        if (!(value instanceof Map<?, ?> mapValue)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return copyMapToDocument(mapValue);
    }

    private static String requireStringField(
            final Document document,
            final String fieldName,
            final String errorMessage) {
        final Object value = document.get(fieldName);
        if (!(value instanceof String stringValue)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return requireText(stringValue, fieldName);
    }

    private static String optionalStringField(
            final Document document,
            final String fieldName,
            final String errorMessage) {
        if (!document.containsKey(fieldName)) {
            return null;
        }
        final Object value = document.get(fieldName);
        if (!(value instanceof String stringValue)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return requireText(stringValue, fieldName);
    }

    private static List<Document> toPipelineList(final Object value, final String errorMessage) {
        if (!(value instanceof List<?> rawList)) {
            throw new IllegalArgumentException(errorMessage);
        }
        final List<Document> pipeline = new ArrayList<>(rawList.size());
        for (final Object stage : rawList) {
            pipeline.add(requireDocument(stage, "pipeline stages must be documents"));
        }
        return List.copyOf(pipeline);
    }

    private static List<Document> materializeDocuments(final Iterable<Document> source, final String nullMessage) {
        Objects.requireNonNull(source, "source");
        final List<Document> materialized = source instanceof List<?> listSource
                ? new ArrayList<>(listSource.size())
                : new ArrayList<>();
        for (final Document document : source) {
            if (document == null) {
                throw new IllegalArgumentException(nullMessage);
            }
            materialized.add(document);
        }
        return materialized;
    }

    private static boolean lookupValueMatches(final Object localValue, final Object foreignValue) {
        if (localValue instanceof List<?> localList) {
            for (final Object candidate : localList) {
                if (Objects.deepEquals(candidate, foreignValue)) {
                    return true;
                }
            }
            return false;
        }
        if (foreignValue instanceof List<?> foreignList) {
            for (final Object candidate : foreignList) {
                if (Objects.deepEquals(localValue, candidate)) {
                    return true;
                }
            }
            return false;
        }
        return Objects.deepEquals(localValue, foreignValue);
    }

    private static Map<String, Object> evaluateLookupVariables(
            final Document source,
            final Document letDefinition) {
        if (letDefinition.isEmpty()) {
            return Map.of();
        }
        final Map<String, Object> variables = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : letDefinition.entrySet()) {
            final String variableName = requireText(entry.getKey(), "$lookup.let key");
            variables.put(variableName, evaluateExpression(source, entry.getValue()));
        }
        return Collections.unmodifiableMap(variables);
    }

    private static List<Document> substitutePipelineVariables(
            final List<Document> pipeline,
            final Map<String, Object> variables) {
        if (variables.isEmpty()) {
            return pipeline;
        }
        final List<Document> substituted = new ArrayList<>(pipeline.size());
        for (final Document stage : pipeline) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> stageMap = (Map<String, Object>) substituteVariables(stage, variables);
            substituted.add(copyMapToDocument(stageMap));
        }
        return List.copyOf(substituted);
    }

    private static Object substituteVariables(final Object value, final Map<String, Object> variables) {
        if (value instanceof String stringValue) {
            if (stringValue.startsWith("$$")) {
                final String variableName = stringValue.substring(2);
                if (variables.containsKey(variableName)) {
                    return DocumentCopies.copyAny(variables.get(variableName));
                }
            }
            return stringValue;
        }
        if (value instanceof Map<?, ?> mapValue) {
            final Map<String, Object> transformed = new LinkedHashMap<>();
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                transformed.put(key, substituteVariables(entry.getValue(), variables));
            }
            return transformed;
        }
        if (value instanceof List<?> listValue) {
            final List<Object> transformed = new ArrayList<>(listValue.size());
            for (final Object item : listValue) {
                transformed.add(substituteVariables(item, variables));
            }
            return transformed;
        }
        return DocumentCopies.copyAny(value);
    }

    private static String requireText(final String value, final String fieldName) {
        final String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static Document copyMapToDocument(final Map<?, ?> source) {
        final Document output = new Document();
        for (final Map.Entry<?, ?> entry : source.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("document keys must be strings");
            }
            output.put(key, DocumentCopies.copyAny(entry.getValue()));
        }
        return output;
    }

    private enum ProjectionMode {
        INCLUDE,
        EXCLUDE
    }

    private enum ProjectionFlag {
        INCLUDE,
        EXCLUDE,
        EXPRESSION
    }

    private record SortKey(String field, int direction) {}

    private record UnwindSpec(String path, boolean preserveNullAndEmptyArrays, String includeArrayIndex) {}

    private record PathValue(boolean present, Object value) {
        private static PathValue missing() {
            return new PathValue(false, null);
        }

        private static PathValue present(final Object value) {
            return new PathValue(true, value);
        }

        private Object valueOrNull() {
            return present ? value : null;
        }
    }

    private record GraphLookupFrontier(Object value, int depth) {}

    private static final class CountBucket {
        private final Object id;
        private long count;

        private CountBucket(final Object id, final long count) {
            this.id = id;
            this.count = count;
        }

        private Object id() {
            return id;
        }

        private long count() {
            return count;
        }

        private void increment() {
            count++;
        }
    }

    private static final class GroupKey {
        private final Object value;
        private final int hashCode;

        private GroupKey(final Object value) {
            this.value = DocumentCopies.copyAny(value);
            this.hashCode = MongoValueComparator.hash(this.value);
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof GroupKey that)) {
                return false;
            }
            return MongoValueComparator.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
