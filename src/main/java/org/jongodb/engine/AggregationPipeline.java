package org.jongodb.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

public final class AggregationPipeline {
    @FunctionalInterface
    public interface CollectionResolver {
        List<Document> resolve(String collectionName);
    }

    private static final CollectionResolver UNSUPPORTED_COLLECTION_RESOLVER =
            collectionName -> {
                throw new UnsupportedFeatureException(
                        "aggregation.cross_collection_resolver",
                        "stage requires collection resolver: " + requireText(collectionName, "collectionName"));
            };

    private AggregationPipeline() {}

    public static List<Document> execute(final List<Document> source, final List<Document> pipeline) {
        return execute(source, pipeline, UNSUPPORTED_COLLECTION_RESOLVER);
    }

    public static List<Document> execute(
            final List<Document> source,
            final List<Document> pipeline,
            final CollectionResolver collectionResolver) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(collectionResolver, "collectionResolver");

        List<Document> working = new ArrayList<>(source.size());
        for (final Document document : source) {
            if (document == null) {
                throw new IllegalArgumentException("source documents must not contain null");
            }
            working.add(DocumentCopies.copy(document));
        }

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
                case "$match" -> applyMatch(working, stageDefinition);
                case "$project" -> applyProject(working, stageDefinition);
                case "$group" -> applyGroup(working, stageDefinition);
                case "$sort" -> applySort(working, stageDefinition);
                case "$limit" -> applyLimit(working, stageDefinition);
                case "$skip" -> applySkip(working, stageDefinition);
                case "$unwind" -> applyUnwind(working, stageDefinition);
                case "$count" -> applyCount(working, stageDefinition);
                case "$addFields" -> applyAddFields(working, stageDefinition);
                case "$sortByCount" -> applySortByCount(working, stageDefinition);
                case "$replaceRoot" -> applyReplaceRoot(working, stageDefinition);
                case "$facet" -> applyFacet(working, stageDefinition, collectionResolver);
                case "$lookup" -> applyLookup(working, stageDefinition, collectionResolver);
                case "$unionWith" -> applyUnionWith(working, stageDefinition, collectionResolver);
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

    private static List<Document> applyMatch(final List<Document> input, final Object stageDefinition) {
        final Document filter = requireDocument(stageDefinition, "$match stage requires a document");
        final List<Document> output = new ArrayList<>();
        for (final Document document : input) {
            if (QueryMatcher.matches(document, filter)) {
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

            output.put(field, evaluateExpression(source, entry.getValue()));
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
        final Map<GroupKey, Document> grouped = new LinkedHashMap<>();
        for (final Document source : input) {
            final Object id = evaluateGroupId(source, idExpression);
            final GroupKey groupKey = new GroupKey(id);
            Document aggregate = grouped.get(groupKey);
            if (aggregate == null) {
                aggregate = new Document("_id", DocumentCopies.copyAny(id));
                for (final GroupAccumulator accumulator : accumulators) {
                    if (accumulator.operator() == GroupAccumulatorOperator.SUM) {
                        aggregate.put(accumulator.outputField(), 0L);
                    }
                }
                grouped.put(groupKey, aggregate);
            }

            for (final GroupAccumulator accumulator : accumulators) {
                if (accumulator.operator() == GroupAccumulatorOperator.SUM) {
                    final Number increment = accumulator.sumOperand(source);
                    aggregate.put(
                            accumulator.outputField(),
                            addNumbers(aggregate.get(accumulator.outputField()), increment));
                    continue;
                }

                if (accumulator.operator() == GroupAccumulatorOperator.FIRST
                        && !aggregate.containsKey(accumulator.outputField())) {
                    aggregate.put(accumulator.outputField(), accumulator.firstOperand(source));
                }
            }
        }

        return List.copyOf(grouped.values());
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
                case "$first" -> GroupAccumulatorOperator.FIRST;
                default -> throw new UnsupportedFeatureException(
                        "aggregation.group.accumulator." + accumulatorName,
                        "unsupported $group accumulator: " + accumulatorName);
            };
            accumulators.add(new GroupAccumulator(entry.getKey(), operator, accumulatorDefinition.get(accumulatorName)));
        }
        return List.copyOf(accumulators);
    }

    private static Object evaluateGroupExpression(final Document source, final Object expression) {
        if (expression instanceof String pathExpression && pathExpression.startsWith("$")) {
            if ("$$ROOT".equals(pathExpression) || "$$CURRENT".equals(pathExpression)) {
                return DocumentCopies.copy(source);
            }
            final PathValue pathValue = resolvePath(source, pathExpression.substring(1));
            return pathValue.present() ? DocumentCopies.copyAny(pathValue.value()) : null;
        }
        return DocumentCopies.copyAny(expression);
    }

    private static Number evaluateGroupSumOperand(final Document source, final Object expression) {
        final Object value = evaluateGroupExpression(source, expression);
        if (value instanceof Number numberValue) {
            return numberValue;
        }
        return 0L;
    }

    private enum GroupAccumulatorOperator {
        SUM,
        FIRST
    }

    private record GroupAccumulator(String outputField, GroupAccumulatorOperator operator, Object expression) {
        private Number sumOperand(final Document source) {
            return evaluateGroupSumOperand(source, expression);
        }

        private Object firstOperand(final Document source) {
            return evaluateGroupExpression(source, expression);
        }
    }

    private static Object evaluateGroupId(final Document source, final Object expression) {
        if (expression instanceof String pathExpression && pathExpression.startsWith("$")) {
            final PathValue pathValue = resolvePath(source, pathExpression.substring(1));
            return pathValue.present() ? DocumentCopies.copyAny(pathValue.value()) : null;
        }
        return DocumentCopies.copyAny(expression);
    }

    private static Number addNumbers(final Object currentValue, final Number increment) {
        if (!(currentValue instanceof Number currentNumber)) {
            return normalizeNumber(increment.doubleValue());
        }
        return normalizeNumber(currentNumber.doubleValue() + increment.doubleValue());
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

    private static List<Document> applySort(final List<Document> input, final Object stageDefinition) {
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
        sorted.sort((left, right) -> compareSortDocuments(left, right, sortKeys));
        return sorted;
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
            final Document left, final Document right, final List<SortKey> sortKeys) {
        for (final SortKey sortKey : sortKeys) {
            final Object leftValue = resolvePath(left, sortKey.field()).valueOrNull();
            final Object rightValue = resolvePath(right, sortKey.field()).valueOrNull();
            final int compared = compareSortValues(leftValue, rightValue);
            if (compared != 0) {
                return sortKey.direction() == 1 ? compared : -compared;
            }
        }
        return 0;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareSortValues(final Object left, final Object right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
        }
        if (left instanceof String leftString && right instanceof String rightString) {
            return leftString.compareTo(rightString);
        }
        if (left instanceof Boolean leftBoolean && right instanceof Boolean rightBoolean) {
            return Boolean.compare(leftBoolean, rightBoolean);
        }
        if (left.getClass().equals(right.getClass()) && left instanceof Comparable leftComparable) {
            return leftComparable.compareTo(right);
        }

        final int leftRank = sortTypeRank(left);
        final int rightRank = sortTypeRank(right);
        if (leftRank != rightRank) {
            return Integer.compare(leftRank, rightRank);
        }
        return stableSortValue(left).compareTo(stableSortValue(right));
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
                    output.add(DocumentCopies.copy(source));
                }
                continue;
            }

            if (pathValue.value() instanceof List<?> listValue) {
                if (listValue.isEmpty()) {
                    if (unwindSpec.preserveNullAndEmptyArrays()) {
                        output.add(DocumentCopies.copy(source));
                    }
                    continue;
                }

                for (final Object item : listValue) {
                    final Document expanded = DocumentCopies.copy(source);
                    setPath(expanded, unwindSpec.path(), item);
                    output.add(expanded);
                }
                continue;
            }

            final Document expanded = DocumentCopies.copy(source);
            setPath(expanded, unwindSpec.path(), pathValue.value());
            output.add(expanded);
        }
        return output;
    }

    private static UnwindSpec parseUnwindSpec(final Object stageDefinition) {
        if (stageDefinition instanceof String pathExpression) {
            return new UnwindSpec(unwindPath(pathExpression), false);
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
        if (unwindDocument.containsKey("includeArrayIndex")) {
            throw new UnsupportedFeatureException(
                    "aggregation.unwind.includeArrayIndex",
                    "$unwind includeArrayIndex is not supported yet");
        }
        return new UnwindSpec(unwindPath(pathExpression), preserveNullAndEmptyArrays);
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
        final Document addFields = requireDocument(stageDefinition, "$addFields stage requires a document");
        if (addFields.isEmpty()) {
            throw new IllegalArgumentException("$addFields stage must not be empty");
        }

        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            final Document expanded = DocumentCopies.copy(source);
            for (final Map.Entry<String, Object> entry : addFields.entrySet()) {
                final String fieldName = requireText(entry.getKey(), "$addFields field");
                setPath(expanded, fieldName, evaluateExpression(source, entry.getValue()));
            }
            output.add(expanded);
        }
        return List.copyOf(output);
    }

    private static List<Document> applySortByCount(final List<Document> input, final Object stageDefinition) {
        final Map<GroupKey, CountBucket> buckets = new LinkedHashMap<>();
        for (final Document source : input) {
            final Object bucketValue = evaluateExpression(source, stageDefinition);
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
            final int countCompared = compareSortValues(right.get("count"), left.get("count"));
            if (countCompared != 0) {
                return countCompared;
            }
            return compareSortValues(left.get("_id"), right.get("_id"));
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

        final List<Document> output = new ArrayList<>(input.size());
        for (final Document source : input) {
            final Object evaluated = evaluateExpression(source, expression);
            if (!(evaluated instanceof Map<?, ?> mapValue)) {
                throw new IllegalArgumentException("$replaceRoot newRoot must evaluate to a document");
            }
            output.add(copyMapToDocument(mapValue));
        }
        return List.copyOf(output);
    }

    private static List<Document> applyFacet(
            final List<Document> input,
            final Object stageDefinition,
            final CollectionResolver collectionResolver) {
        final Document facetDefinition = requireDocument(stageDefinition, "$facet stage requires a document");
        if (facetDefinition.isEmpty()) {
            throw new IllegalArgumentException("$facet stage must not be empty");
        }

        final Document facetResult = new Document();
        for (final Map.Entry<String, Object> entry : facetDefinition.entrySet()) {
            final String facetName = requireText(entry.getKey(), "$facet key");
            final List<Document> facetPipeline = toPipelineList(entry.getValue(), "$facet pipeline must be an array");
            final List<Document> facetOutput =
                    execute(input, facetPipeline, collectionResolver);
            facetResult.put(facetName, facetOutput);
        }
        return List.of(facetResult);
    }

    private static List<Document> applyLookup(
            final List<Document> input,
            final Object stageDefinition,
            final CollectionResolver collectionResolver) {
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
            final List<Document> foreignSource = copyDocuments(
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
                joined = execute(joined, substitutedPipeline, collectionResolver);
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
            final CollectionResolver collectionResolver) {
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

        final List<Document> combined = copyDocuments(input, "input must not contain null");
        List<Document> unionSource = copyDocuments(
                collectionResolver.resolve(collectionName),
                "$unionWith resolver returned null documents");
        if (!unionPipeline.isEmpty()) {
            unionSource = execute(unionSource, unionPipeline, collectionResolver);
        }
        for (final Document document : unionSource) {
            combined.add(DocumentCopies.copy(document));
        }
        return List.copyOf(combined);
    }

    private static Object evaluateExpression(final Document source, final Object expression) {
        if (expression instanceof String pathExpression && pathExpression.startsWith("$")) {
            if ("$$ROOT".equals(pathExpression) || "$$CURRENT".equals(pathExpression)) {
                return DocumentCopies.copy(source);
            }
            final PathValue pathValue = resolvePath(source, pathExpression.substring(1));
            return pathValue.present() ? DocumentCopies.copyAny(pathValue.value()) : null;
        }
        return DocumentCopies.copyAny(expression);
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

    private static List<Document> copyDocuments(final List<Document> source, final String nullMessage) {
        Objects.requireNonNull(source, "source");
        final List<Document> copied = new ArrayList<>(source.size());
        for (final Document document : source) {
            if (document == null) {
                throw new IllegalArgumentException(nullMessage);
            }
            copied.add(DocumentCopies.copy(document));
        }
        return copied;
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

    private record UnwindSpec(String path, boolean preserveNullAndEmptyArrays) {}

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
            this.hashCode = Arrays.deepHashCode(new Object[] {this.value});
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof GroupKey that)) {
                return false;
            }
            return Objects.deepEquals(value, that.value);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
