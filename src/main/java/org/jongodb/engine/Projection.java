package org.jongodb.engine;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;

/**
 * Find projection (inclusion, exclusion, {@code $slice}, {@code $elemMatch}, positional {@code $}).
 *
 * <p>{@code $slice} alone is an exclusion projection unless {@code _id: 1} is also present.
 * {@code $elemMatch} alone is an inclusion projection.
 */
public final class Projection {
    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();
    private static final DecoderContext DECODER_CONTEXT = DecoderContext.builder().build();

    private Projection() {}

    public static final class Failure extends IllegalArgumentException {
        private final int code;
        private final String codeName;

        public Failure(final int code, final String codeName, final String message) {
            super(message);
            this.code = code;
            this.codeName = codeName;
        }

        public int code() {
            return code;
        }

        public String codeName() {
            return codeName;
        }
    }

    public static Parsed parse(final BsonDocument specification) {
        if (specification == null || specification.isEmpty()) {
            return Parsed.identity();
        }
        return new Parser(specification).parse();
    }

    public static BsonDocument apply(
            final BsonDocument document, final Parsed parsed, final BsonDocument query) {
        if (document == null || parsed == null || parsed.identity) {
            return document;
        }
        final BsonDocument effectiveQuery = query == null ? new BsonDocument() : query;
        if (parsed.mode == Mode.INCLUSION) {
            final BsonDocument projected = new BsonDocument();
            if (parsed.includeId && document.containsKey("_id")) {
                projected.append("_id", copyValue(document.get("_id")));
            }
            projectChildren(document, parsed.root, projected, effectiveQuery, "");
            return projected;
        }
        final BsonDocument projected = (BsonDocument) copyValue(document);
        if (!parsed.includeId) {
            projected.remove("_id");
        }
        excludeChildren(projected, parsed.root, effectiveQuery, "");
        return projected;
    }

    public static final class Parsed {
        private final Mode mode;
        private final Node root;
        private final boolean includeId;
        private final boolean identity;

        private Parsed(final Mode mode, final Node root, final boolean includeId, final boolean identity) {
            this.mode = mode;
            this.root = root;
            this.includeId = includeId;
            this.identity = identity;
        }

        static Parsed identity() {
            return new Parsed(Mode.EXCLUSION, Node.intermediate(), true, true);
        }
    }

    private enum Mode {
        INCLUSION,
        EXCLUSION
    }

    private enum Kind {
        INTERMEDIATE,
        INCLUDE,
        EXCLUDE,
        SLICE,
        ELEM_MATCH,
        POSITIONAL
    }

    private static final class Node {
        private final Kind kind;
        private final Map<String, Node> children;
        private final Integer sliceSkip;
        private final int sliceLimit;
        private final BsonDocument elemMatch;
        private final String positionalPath;

        private Node(
                final Kind kind,
                final Map<String, Node> children,
                final Integer sliceSkip,
                final int sliceLimit,
                final BsonDocument elemMatch,
                final String positionalPath) {
            this.kind = kind;
            this.children = children;
            this.sliceSkip = sliceSkip;
            this.sliceLimit = sliceLimit;
            this.elemMatch = elemMatch;
            this.positionalPath = positionalPath;
        }

        private static Node intermediate() {
            return new Node(Kind.INTERMEDIATE, new LinkedHashMap<>(), null, 0, null, null);
        }

        private static Node include() {
            return new Node(Kind.INCLUDE, Map.of(), null, 0, null, null);
        }

        private static Node exclude() {
            return new Node(Kind.EXCLUDE, Map.of(), null, 0, null, null);
        }

        private static Node slice(final Integer skip, final int limit) {
            return new Node(Kind.SLICE, Map.of(), skip, limit, null, null);
        }

        private static Node elemMatch(final BsonDocument criteria) {
            return new Node(Kind.ELEM_MATCH, Map.of(), null, 0, criteria, null);
        }

        private static Node positional(final String path) {
            return new Node(Kind.POSITIONAL, Map.of(), null, 0, null, path);
        }
    }

    private static final class Parser {
        private final BsonDocument specification;
        private final Node root = Node.intermediate();
        private Mode type;
        private boolean hasSlice;
        private boolean hasElemMatch;
        private boolean hasPositional;
        private boolean idIncludedEntirely;
        private boolean idExcludedEntirely;
        private boolean idSpecified;

        private Parser(final BsonDocument specification) {
            this.specification = specification;
        }

        private Parsed parse() {
            for (final Map.Entry<String, BsonValue> entry : specification.entrySet()) {
                final String field = entry.getKey();
                idSpecified = idSpecified || "_id".equals(field) || field.startsWith("_id.");
                parseElement(field, entry.getValue(), root, true);
            }
            if (type == null) {
                type = computeType();
            }
            final boolean includeId = includeId(type);
            return new Parsed(type, root, includeId, false);
        }

        private Mode computeType() {
            if (hasSlice) {
                return idIncludedEntirely ? Mode.INCLUSION : Mode.EXCLUSION;
            }
            if (hasElemMatch || idIncludedEntirely) {
                return Mode.INCLUSION;
            }
            return Mode.EXCLUSION;
        }

        private boolean includeId(final Mode mode) {
            if (mode == Mode.INCLUSION) {
                if (idExcludedEntirely) {
                    return false;
                }
                return idIncludedEntirely || !idSpecified;
            }
            return !idExcludedEntirely;
        }

        private void parseElement(
                final String field, final BsonValue value, final Node parent, final boolean parentIsRoot) {
            if (field.contains(".$.") ) {
                throw new Failure(
                        31394,
                        "Location31394",
                        "As of 4.4, it's illegal to specify positional operator in the middle of a path."
                                + "Positional projection may only be used at the end, for example: a.b.$. "
                                + "If the query previously used a form like a.b.$.d, remove the parts following the '$' "
                                + "and the results will be equivalent.");
            }
            final boolean positional = isPositionalField(field);
            if (value != null && value.isDocument()) {
                if (positional) {
                    throw new Failure(
                            31271,
                            "Location31271",
                            "positional projection cannot be used with an expression or sub object");
                }
                parseSubObject(field, value.asDocument(), parent, parentIsRoot);
                return;
            }
            if (isInclusionOrExclusionValue(value)) {
                if (isTruthy(value)) {
                    parseInclusion(field, parent, parentIsRoot, positional);
                } else {
                    if (positional) {
                        throw new Failure(31395, "Location31395", "positional projection cannot be used with exclusion");
                    }
                    parseExclusion(field, parent, parentIsRoot);
                }
                return;
            }
            if (positional) {
                throw new Failure(31308, "Location31308", "positional projection cannot be used with a literal");
            }
            throw new Failure(
                    51271,
                    "Location51271",
                    "Bad projection specification, cannot use computed fields when parsing a spec in kBanComputedFields mode");
        }

        private void parseSubObject(
                final String field, final BsonDocument subObject, final Node parent, final boolean parentIsRoot) {
            if (subObject.isEmpty()) {
                throw new Failure(
                        51270,
                        "Location51270",
                        "An empty sub-projection is not a valid value. Found empty object at path");
            }
            if (subObject.size() == 1) {
                final String operator = subObject.getFirstKey();
                if (operator.startsWith("$")) {
                    if ("$slice".equals(operator)) {
                        parseSlice(field, subObject.get(operator), parent);
                        return;
                    }
                    if ("$elemMatch".equals(operator)) {
                        parseElemMatch(field, subObject.get(operator), parent, parentIsRoot);
                        return;
                    }
                    throw new Failure(31325, "Location31325", "Unknown expression " + operator);
                }
            }
            final Node container = descend(parent, splitPath(field));
            for (final Map.Entry<String, BsonValue> entry : subObject.entrySet()) {
                if (entry.getKey().startsWith("$")) {
                    throw new Failure(31325, "Location31325", "Unknown expression " + entry.getKey());
                }
                parseElement(entry.getKey(), entry.getValue(), container, false);
            }
        }

        private void parseSlice(final String field, final BsonValue argument, final Node parent) {
            final Integer skip;
            final int limit;
            if (argument != null && argument.isNumber()) {
                skip = null;
                limit = integralArgument(argument, "$slice expects an integral number, got ");
            } else if (argument != null && argument.isArray()) {
                final BsonArray pair = argument.asArray();
                if (pair.size() != 2) {
                    throw new Failure(31272, "Location31272", "$slice array argument should be of form [skip, limit]");
                }
                final BsonValue skipValue = pair.get(0);
                final BsonValue limitValue = pair.get(1);
                if (skipValue == null || !skipValue.isNumber()) {
                    throw new Failure(
                            31257,
                            "Location31257",
                            "$slice expects the skip argument to be a number, got " + bsonTypeName(skipValue));
                }
                if (limitValue == null || !limitValue.isNumber()) {
                    throw new Failure(
                            31258,
                            "Location31258",
                            "$slice expects the limit argument to be a number, got " + bsonTypeName(limitValue));
                }
                skip = integralArgument(skipValue, "$slice expects the skip argument to be an integral number, got ");
                limit = integralArgument(limitValue, "$slice expects the limit argument to be an integral number, got ");
                if (limit <= 0) {
                    throw new Failure(31259, "Location31259", "$slice limit must be positive, got " + limit);
                }
            } else {
                throw new Failure(31273, "Location31273", "$slice only supports numbers and [skip, limit] arrays");
            }
            place(parent, splitPath(field), Node.slice(skip, limit));
            hasSlice = true;
        }

        private void parseElemMatch(
                final String field, final BsonValue argument, final Node parent, final boolean parentIsRoot) {
            if (argument == null || !argument.isDocument()) {
                throw new Failure(
                        31274,
                        "Location31274",
                        "elemMatch: Invalid argument, object required, but got " + bsonTypeName(argument));
            }
            if (hasPositional) {
                throw new Failure(31255, "Location31255", "Cannot specify positional operator and $elemMatch.");
            }
            final String[] segments = splitPath(field);
            if (segments.length != 1 || !parentIsRoot) {
                throw new Failure(31275, "Location31275", "Cannot use $elemMatch projection on a nested field.");
            }
            place(parent, segments, Node.elemMatch(argument.asDocument()));
            hasElemMatch = true;
        }

        private void parseInclusion(
                final String field, final Node parent, final boolean parentIsRoot, final boolean positional) {
            if (positional) {
                if (hasPositional) {
                    throw new Failure(31276, "Location31276", "Cannot specify more than one positional projection per query.");
                }
                if (hasElemMatch) {
                    throw new Failure(31256, "Location31256", "Cannot specify positional operator and $elemMatch.");
                }
                if (".$".equals(field)) {
                    throw new Failure(5392900, "Location5392900", "Projection on field .$ is invalid");
                }
                final String path = field.substring(0, field.length() - 2);
                if (path.isEmpty()) {
                    throw new Failure(2, "BadValue", "Projection path is empty");
                }
                place(parent, splitPath(path), Node.positional(path));
                hasPositional = true;
            } else if ("_id".equals(field) && parentIsRoot) {
                idIncludedEntirely = true;
            } else {
                place(parent, splitPath(field), Node.include());
            }
            if (!("_id".equals(field) && parentIsRoot)) {
                if (type == Mode.EXCLUSION) {
                    throw new Failure(
                            31253,
                            "Location31253",
                            "Cannot do inclusion on field " + field + " in exclusion projection");
                }
                type = Mode.INCLUSION;
            }
        }

        private void parseExclusion(final String field, final Node parent, final boolean parentIsRoot) {
            if ("_id".equals(field) && parentIsRoot) {
                idExcludedEntirely = true;
                return;
            }
            if (type == Mode.INCLUSION) {
                throw new Failure(
                        31254,
                        "Location31254",
                        "Cannot do exclusion on field " + field + " in inclusion projection");
            }
            type = Mode.EXCLUSION;
            place(parent, splitPath(field), Node.exclude());
        }

        private Node descend(final Node parent, final String[] segments) {
            Node current = parent;
            for (final String segment : segments) {
                Node child = current.children.get(segment);
                if (child == null) {
                    child = Node.intermediate();
                    current.children.put(segment, child);
                } else if (child.kind != Kind.INTERMEDIATE) {
                    throw pathCollision(segments);
                }
                current = child;
            }
            return current;
        }

        private void place(final Node parent, final String[] segments, final Node leaf) {
            if (segments.length == 0) {
                throw new Failure(2, "BadValue", "Projection path is empty");
            }
            Node current = parent;
            for (int index = 0; index < segments.length - 1; index++) {
                final String segment = segments[index];
                final Node child = current.children.get(segment);
                if (child == null) {
                    final Node created = Node.intermediate();
                    current.children.put(segment, created);
                    current = created;
                    continue;
                }
                if (child.kind != Kind.INTERMEDIATE) {
                    throw pathCollision(segments);
                }
                current = child;
            }
            final String leafName = segments[segments.length - 1];
            if (current.children.containsKey(leafName)) {
                throw new Failure(31250, "Location31250", "Path collision at " + String.join(".", segments));
            }
            current.children.put(leafName, leaf);
        }

        private Failure pathCollision(final String[] segments) {
            final String full = String.join(".", segments);
            final String tail = segments.length <= 1 ? "" : String.join(".", Arrays.copyOfRange(segments, 1, segments.length));
            return new Failure(31249, "Location31249", "Path collision at " + full + " remaining portion " + tail);
        }
    }

    private static void projectChildren(
            final BsonDocument source,
            final Node node,
            final BsonDocument target,
            final BsonDocument query,
            final String path) {
        for (final Map.Entry<String, Node> entry : node.children.entrySet()) {
            final String field = entry.getKey();
            if (!source.containsKey(field)) {
                continue;
            }
            final String childPath = path.isEmpty() ? field : path + "." + field;
            final BsonValue projected = projectValue(source.get(field), entry.getValue(), query, childPath);
            if (projected != null) {
                target.append(field, projected);
            }
        }
    }

    private static BsonValue projectValue(
            final BsonValue value, final Node node, final BsonDocument query, final String path) {
        return switch (node.kind) {
            case INCLUDE -> copyValue(value);
            case SLICE -> sliceValue(value, node);
            case ELEM_MATCH -> projectElemMatch(value, node.elemMatch);
            case POSITIONAL -> projectPositional(value, node.positionalPath, query);
            case INTERMEDIATE -> projectIntermediate(value, node, query, path);
            case EXCLUDE -> null;
        };
    }

    private static BsonValue projectIntermediate(
            final BsonValue value, final Node node, final BsonDocument query, final String path) {
        if (value != null && value.isDocument()) {
            final BsonDocument projected = new BsonDocument();
            projectChildren(value.asDocument(), node, projected, query, path);
            return projected;
        }
        if (value != null && value.isArray()) {
            final BsonArray projected = new BsonArray();
            for (final BsonValue element : value.asArray()) {
                if (element != null && element.isDocument()) {
                    projected.add(projectIntermediate(element, node, query, path));
                } else if (element != null && element.isArray()) {
                    final BsonValue nested = projectIntermediate(element, node, query, path);
                    projected.add(nested == null ? new BsonArray() : nested);
                } else {
                    projected.add(copyValue(element));
                }
            }
            return projected;
        }
        return null;
    }

    private static void excludeChildren(
            final BsonDocument document, final Node node, final BsonDocument query, final String path) {
        for (final Map.Entry<String, Node> entry : node.children.entrySet()) {
            final String field = entry.getKey();
            if (!document.containsKey(field)) {
                continue;
            }
            final Node child = entry.getValue();
            final String childPath = path.isEmpty() ? field : path + "." + field;
            switch (child.kind) {
                case EXCLUDE -> document.remove(field);
                case SLICE -> document.put(field, sliceValue(document.get(field), child));
                case ELEM_MATCH -> {
                    final BsonValue matched = projectElemMatch(document.get(field), child.elemMatch);
                    if (matched == null) {
                        document.remove(field);
                    } else {
                        document.put(field, matched);
                    }
                }
                case POSITIONAL -> document.put(field, projectPositional(document.get(field), child.positionalPath, query));
                case INTERMEDIATE -> excludeNested(document.get(field), child, query, childPath);
                case INCLUDE -> {
                    // Inclusion leaves are not stored on an exclusion projection.
                }
                default -> {
                }
            }
        }
    }

    private static void excludeNested(final BsonValue value, final Node node, final BsonDocument query, final String path) {
        if (value != null && value.isDocument()) {
            excludeChildren(value.asDocument(), node, query, path);
            return;
        }
        if (value != null && value.isArray()) {
            for (final BsonValue element : value.asArray()) {
                if (element != null && (element.isDocument() || element.isArray())) {
                    excludeNested(element, node, query, path);
                }
            }
        }
    }

    private static BsonValue sliceValue(final BsonValue value, final Node node) {
        if (value == null || !value.isArray()) {
            return copyValue(value);
        }
        return sliceArray(value.asArray(), node.sliceSkip, node.sliceLimit);
    }

    private static BsonArray sliceArray(final BsonArray array, final Integer skip, final int limit) {
        final int size = array.size();
        final int start;
        final int count;
        if (skip == null) {
            if (limit >= 0) {
                start = 0;
                count = limit;
            } else {
                count = -limit;
                start = Math.max(0, size - count);
            }
        } else if (skip >= 0) {
            start = skip;
            count = limit;
        } else {
            start = Math.max(0, size + skip);
            count = limit;
        }
        final BsonArray sliced = new BsonArray();
        for (int index = start; index < size && sliced.size() < count; index++) {
            sliced.add(copyValue(array.get(index)));
        }
        return sliced;
    }

    private static BsonValue projectElemMatch(final BsonValue value, final BsonDocument criteria) {
        if (value == null || !value.isArray()) {
            return copyValue(value);
        }
        for (final BsonValue element : value.asArray()) {
            if (elementSatisfies(element, criteria)) {
                final BsonArray matched = new BsonArray();
                matched.add(copyValue(element));
                return matched;
            }
        }
        return null;
    }

    private static BsonValue projectPositional(final BsonValue value, final String path, final BsonDocument query) {
        if (value == null || !value.isArray()) {
            return copyValue(value);
        }
        final BsonDocument predicate = firstPredicate(query, path);
        if (predicate != null) {
            final String predicateKey = predicate.getFirstKey();
            final BsonValue predicateValue = predicate.get(predicateKey);
            final BsonArray array = value.asArray();
            for (final BsonValue element : array) {
                if (positionalElementMatches(element, path, predicateKey, predicateValue)) {
                    final BsonArray matched = new BsonArray();
                    matched.add(copyValue(element));
                    return matched;
                }
            }
        }
        throw new Failure(
                51246,
                "Location51246",
                "positional operator '.$' couldn't find a matching element in the array");
    }

    private static boolean positionalElementMatches(
            final BsonValue element, final String path, final String predicateKey, final BsonValue predicateValue) {
        if (predicateKey.equals(path)) {
            if (predicateValue != null
                    && predicateValue.isDocument()
                    && predicateValue.asDocument().size() == 1
                    && predicateValue.asDocument().containsKey("$elemMatch")
                    && predicateValue.asDocument().get("$elemMatch").isDocument()) {
                return elementSatisfies(element, predicateValue.asDocument().getDocument("$elemMatch"));
            }
            return QueryMatcher.matches(
                    new Document("v", toJava(element)), new Document("v", toJava(predicateValue)));
        }
        if (!predicateKey.startsWith(path + ".") || element == null || !element.isDocument()) {
            return false;
        }
        final String suffix = predicateKey.substring(path.length() + 1);
        return QueryMatcher.matches(
                toDocument(element.asDocument()), new Document(suffix, toJava(predicateValue)));
    }

    private static boolean elementSatisfies(final BsonValue element, final BsonDocument criteria) {
        if (criteria == null || criteria.isEmpty()) {
            return true;
        }
        final boolean operatorOnly = isOperatorOnly(criteria);
        final boolean logical = criteria.containsKey("$and") || criteria.containsKey("$or") || criteria.containsKey("$nor");
        if (element != null && element.isDocument() && (!operatorOnly || logical)) {
            return QueryMatcher.matches(toDocument(element.asDocument()), toDocument(criteria));
        }
        if (operatorOnly && !logical) {
            return QueryMatcher.matches(new Document("v", toJava(element)), new Document("v", toJava(criteria)));
        }
        return false;
    }

    private static boolean isOperatorOnly(final BsonDocument document) {
        if (document.isEmpty()) {
            return false;
        }
        for (final String key : document.keySet()) {
            if (!key.startsWith("$")) {
                return false;
            }
        }
        return true;
    }

    private static BsonDocument firstPredicate(final BsonDocument query, final String path) {
        if (query == null) {
            return null;
        }
        for (final Map.Entry<String, BsonValue> entry : query.entrySet()) {
            final String key = entry.getKey();
            final BsonValue value = entry.getValue();
            if ("$and".equals(key) || "$or".equals(key) || "$nor".equals(key)) {
                if (value != null && value.isArray()) {
                    for (final BsonValue clause : value.asArray()) {
                        if (clause != null && clause.isDocument()) {
                            final BsonDocument found = firstPredicate(clause.asDocument(), path);
                            if (found != null) {
                                return found;
                            }
                        }
                    }
                }
                continue;
            }
            if (key.equals(path) || key.startsWith(path + ".")) {
                return new BsonDocument(key, value);
            }
        }
        return null;
    }

    private static boolean isPositionalField(final String field) {
        return ".$".equals(field) || field.endsWith(".$");
    }

    private static boolean isInclusionOrExclusionValue(final BsonValue value) {
        return value != null && (value.isBoolean() || value.isNumber());
    }

    private static boolean isTruthy(final BsonValue value) {
        if (value.isBoolean()) {
            return value.asBoolean().getValue();
        }
        if (value.isDecimal128()) {
            try {
                return value.asDecimal128().getValue().bigDecimalValue().signum() != 0;
            } catch (final ArithmeticException exception) {
                return true;
            }
        }
        return value.asNumber().doubleValue() != 0d;
    }

    private static int integralArgument(final BsonValue value, final String messagePrefix) {
        try {
            if (value.isInt32()) {
                return value.asInt32().getValue();
            }
            if (value.isInt64()) {
                final long longValue = value.asInt64().getValue();
                if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                    throw new ArithmeticException("out of int range");
                }
                return (int) longValue;
            }
            if (value.isDouble()) {
                final double doubleValue = value.asDouble().getValue();
                if (!Double.isFinite(doubleValue) || doubleValue != Math.rint(doubleValue)) {
                    throw new ArithmeticException("not integral");
                }
                if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                    throw new ArithmeticException("out of int range");
                }
                return (int) doubleValue;
            }
            if (value.isDecimal128()) {
                return value.asDecimal128().getValue().bigDecimalValue().intValueExact();
            }
        } catch (final ArithmeticException | NumberFormatException exception) {
            throw new Failure(31109, "Location31109", messagePrefix + bsonTypeName(value));
        }
        throw new Failure(31109, "Location31109", messagePrefix + bsonTypeName(value));
    }

    private static String[] splitPath(final String field) {
        if (field == null || field.isEmpty()) {
            throw new Failure(2, "BadValue", "Projection path is empty");
        }
        final String[] segments = field.split("\\.", -1);
        for (final String segment : segments) {
            if (segment.isEmpty()) {
                throw new Failure(2, "BadValue", "Projection path is empty");
            }
        }
        return segments;
    }

    private static String bsonTypeName(final BsonValue value) {
        return value == null ? "null" : value.getBsonType().toString();
    }

    private static BsonValue copyValue(final BsonValue value) {
        if (value == null) {
            return null;
        }
        if (value.isDocument()) {
            final BsonDocument copy = new BsonDocument();
            for (final Map.Entry<String, BsonValue> entry : value.asDocument().entrySet()) {
                copy.append(entry.getKey(), copyValue(entry.getValue()));
            }
            return copy;
        }
        if (value.isArray()) {
            final BsonArray copy = new BsonArray();
            for (final BsonValue item : value.asArray()) {
                copy.add(copyValue(item));
            }
            return copy;
        }
        return value;
    }

    private static Object toJava(final BsonValue value) {
        if (value == null || value.isNull()) {
            return null;
        }
        return DOCUMENT_CODEC
                .decode(new BsonDocumentReader(new BsonDocument("v", value)), DECODER_CONTEXT)
                .get("v");
    }

    private static Document toDocument(final BsonDocument value) {
        if (value.isEmpty()) {
            return new Document();
        }
        return DOCUMENT_CODEC.decode(new BsonDocumentReader(value), DECODER_CONTEXT);
    }
}
