package org.jongodb.command;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

final class UpdateArrayFiltersSubset {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private UpdateArrayFiltersSubset() {}

    static ParseResult parse(final BsonValue value) {
        if (value == null) {
            return ParseResult.ok(ParsedArrayFilters.empty());
        }
        if (!value.isArray()) {
            return ParseResult.error(CommandErrors.typeMismatch("arrayFilters must be an array"));
        }

        final BsonArray rawFilters = value.asArray();
        if (rawFilters.isEmpty()) {
            return ParseResult.error(CommandErrors.badValue("arrayFilters must not be empty"));
        }

        final List<BsonDocument> filters = new ArrayList<>(rawFilters.size());
        final Set<String> identifiers = new LinkedHashSet<>();
        for (int index = 0; index < rawFilters.size(); index++) {
            final BsonValue item = rawFilters.get(index);
            if (!item.isDocument()) {
                return ParseResult.error(CommandErrors.typeMismatch("arrayFilters entries must be documents"));
            }

            final BsonDocument filter = item.asDocument();
            if (filter.isEmpty()) {
                return ParseResult.error(CommandErrors.badValue("arrayFilters entries must not be empty"));
            }

            final String identifier = extractIdentifier(filter);
            if (identifier == null) {
                return ParseResult.error(CommandErrors.badValue(
                        "arrayFilters entries must use '<identifier>.<fieldPath>' keys in the supported subset"));
            }
            if (!identifiers.add(identifier)) {
                return ParseResult.error(
                        CommandErrors.badValue("duplicate array filter identifier '" + identifier + "'"));
            }
            filters.add(filter.clone());
        }

        return ParseResult.ok(new ParsedArrayFilters(filters, identifiers));
    }

    static BsonDocument validateUpdatePaths(
            final BsonDocument updateDocument, final ParsedArrayFilters parsedArrayFilters) {
        Objects.requireNonNull(updateDocument, "updateDocument");
        Objects.requireNonNull(parsedArrayFilters, "parsedArrayFilters");

        final Set<String> usedIdentifiers = new LinkedHashSet<>();
        for (final String operator : updateDocument.keySet()) {
            final BsonValue operatorDefinition = updateDocument.get(operator);
            if (operatorDefinition == null || !operatorDefinition.isDocument()) {
                continue;
            }

            for (final String path : operatorDefinition.asDocument().keySet()) {
                final PathAnalysis analysis = analyzeUpdatePath(path);
                if (analysis.error() != null) {
                    return CommandErrors.badValue(analysis.error());
                }
                if (analysis.unsupportedPositionalPath()) {
                    return CommandErrors.badValue(
                            "positional and array filter updates are not supported for path '" + path + "'");
                }
                if (analysis.identifiers().isEmpty()) {
                    continue;
                }

                if (parsedArrayFilters.isEmpty()) {
                    return CommandErrors.badValue("arrayFilters must be specified for path '" + path + "'");
                }
                if (!"$set".equals(operator) && !"$unset".equals(operator)) {
                    return CommandErrors.badValue(
                            "arrayFilters currently support only $set/$unset for path '" + path + "'");
                }

                for (final String identifier : analysis.identifiers()) {
                    if (!parsedArrayFilters.identifiers().contains(identifier)) {
                        return CommandErrors.badValue("no array filter found for identifier '" + identifier + "'");
                    }
                    usedIdentifiers.add(identifier);
                }
            }
        }

        if (!parsedArrayFilters.isEmpty()) {
            if (usedIdentifiers.isEmpty()) {
                return CommandErrors.badValue("arrayFilters specified but no array filter identifier used in update paths");
            }
            for (final String identifier : parsedArrayFilters.identifiers()) {
                if (!usedIdentifiers.contains(identifier)) {
                    return CommandErrors.badValue(
                            "array filter identifier '" + identifier + "' was not used in update paths");
                }
            }
        }

        return null;
    }

    private static String extractIdentifier(final BsonDocument filterDocument) {
        String identifier = null;
        for (final String key : filterDocument.keySet()) {
            if (key.startsWith("$")) {
                return null;
            }
            final int separator = key.indexOf('.');
            if (separator <= 0 || separator == key.length() - 1) {
                return null;
            }

            final String candidateIdentifier = key.substring(0, separator);
            if (!isValidIdentifier(candidateIdentifier)) {
                return null;
            }
            if (!isValidFieldPath(key.substring(separator + 1))) {
                return null;
            }
            if (identifier == null) {
                identifier = candidateIdentifier;
            } else if (!identifier.equals(candidateIdentifier)) {
                return null;
            }
        }
        return identifier;
    }

    private static PathAnalysis analyzeUpdatePath(final String path) {
        if (path == null || path.isBlank()) {
            return PathAnalysis.error("field path must not be blank");
        }

        final String[] segments = path.split("\\.");
        final Set<String> identifiers = new LinkedHashSet<>();
        for (final String segment : segments) {
            if (segment.isEmpty()) {
                return PathAnalysis.error("field path must not contain empty segments");
            }
            if ("$".equals(segment) || "$[]".equals(segment)) {
                return PathAnalysis.unsupportedPositional();
            }

            if (!segment.startsWith("$[") && !segment.endsWith("]")) {
                continue;
            }
            if (!segment.startsWith("$[") || !segment.endsWith("]") || segment.length() <= 3) {
                return PathAnalysis.error("invalid array filter identifier segment in path '" + path + "'");
            }

            final String identifier = segment.substring(2, segment.length() - 1);
            if (!isValidIdentifier(identifier)) {
                return PathAnalysis.error("invalid array filter identifier '" + identifier + "'");
            }
            identifiers.add(identifier);
        }

        return new PathAnalysis(List.copyOf(identifiers), false, null);
    }

    private static boolean isValidIdentifier(final String value) {
        return IDENTIFIER_PATTERN.matcher(value).matches();
    }

    private static boolean isValidFieldPath(final String path) {
        final String[] segments = path.split("\\.");
        for (final String segment : segments) {
            if (segment.isEmpty() || "$".equals(segment) || "$[]".equals(segment)) {
                return false;
            }
            if (segment.startsWith("$[")) {
                return false;
            }
        }
        return true;
    }

    static final class ParseResult {
        private final ParsedArrayFilters parsed;
        private final BsonDocument error;

        private ParseResult(final ParsedArrayFilters parsed, final BsonDocument error) {
            this.parsed = parsed;
            this.error = error;
        }

        static ParseResult ok(final ParsedArrayFilters parsed) {
            return new ParseResult(parsed, null);
        }

        static ParseResult error(final BsonDocument error) {
            return new ParseResult(ParsedArrayFilters.empty(), Objects.requireNonNull(error, "error"));
        }

        ParsedArrayFilters parsed() {
            return parsed;
        }

        BsonDocument error() {
            return error;
        }
    }

    static final class ParsedArrayFilters {
        private static final ParsedArrayFilters EMPTY = new ParsedArrayFilters(List.of(), Set.of());

        private final List<BsonDocument> filters;
        private final Set<String> identifiers;

        ParsedArrayFilters(final List<BsonDocument> filters, final Set<String> identifiers) {
            this.filters = copyFilters(filters);
            this.identifiers = Set.copyOf(identifiers);
        }

        static ParsedArrayFilters empty() {
            return EMPTY;
        }

        List<BsonDocument> filters() {
            return filters;
        }

        Set<String> identifiers() {
            return identifiers;
        }

        boolean isEmpty() {
            return filters.isEmpty();
        }

        BsonArray toBsonArray() {
            final BsonArray array = new BsonArray();
            for (final BsonDocument filter : filters) {
                array.add(filter.clone());
            }
            return array;
        }

        private static List<BsonDocument> copyFilters(final List<BsonDocument> filters) {
            if (filters == null || filters.isEmpty()) {
                return List.of();
            }
            final List<BsonDocument> copied = new ArrayList<>(filters.size());
            for (final BsonDocument filter : filters) {
                copied.add(Objects.requireNonNull(filter, "arrayFilters entries must not be null").clone());
            }
            return List.copyOf(copied);
        }
    }

    private record PathAnalysis(List<String> identifiers, boolean unsupportedPositionalPath, String error) {
        static PathAnalysis unsupportedPositional() {
            return new PathAnalysis(List.of(), true, null);
        }

        static PathAnalysis error(final String message) {
            return new PathAnalysis(List.of(), false, Objects.requireNonNull(message, "message"));
        }
    }
}
