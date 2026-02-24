package org.jongodb.testkit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.bson.Document;
import org.yaml.snakeyaml.Yaml;

/**
 * Imports a subset of MongoDB Unified Test Format (UTF) into differential harness scenarios.
 */
public final class UnifiedSpecImporter {
    private static final String EXT_JSON = ".json";
    private static final String EXT_YAML = ".yaml";
    private static final String EXT_YML = ".yml";

    private final Yaml yaml;

    public UnifiedSpecImporter() {
        this.yaml = new Yaml();
    }

    public ImportResult importCorpus(final Path specRoot) throws IOException {
        Objects.requireNonNull(specRoot, "specRoot");
        if (!Files.exists(specRoot)) {
            throw new IllegalArgumentException("specRoot does not exist: " + specRoot);
        }
        if (!Files.isDirectory(specRoot)) {
            throw new IllegalArgumentException("specRoot must be a directory: " + specRoot);
        }

        final List<Path> files = discoverSpecFiles(specRoot);
        final List<ImportedScenario> imported = new ArrayList<>();
        final List<SkippedCase> skipped = new ArrayList<>();

        for (final Path file : files) {
            final String sourcePath = relativePath(specRoot, file);
            final Map<String, Object> spec;
            try {
                final Object parsed = parseFile(file);
                spec = asStringObjectMap(parsed, "spec file");
            } catch (final RuntimeException parseError) {
                skipped.add(new SkippedCase(
                        sourcePath + "::parse-error",
                        sourcePath,
                        SkipKind.INVALID,
                        parseError.getMessage()));
                continue;
            }

            final String defaultDatabase = readDefaultDatabase(spec);
            final String defaultCollection = readDefaultCollection(spec);
            final Object testsValue = spec.get("tests");
            if (!(testsValue instanceof List<?> testsRaw)) {
                skipped.add(new SkippedCase(
                        sourcePath + "::invalid-tests",
                        sourcePath,
                        SkipKind.INVALID,
                        "tests must be an array"));
                continue;
            }
            final List<Object> tests = List.copyOf(testsRaw);
            if (tests.isEmpty()) {
                skipped.add(new SkippedCase(
                        sourcePath + "::no-tests",
                        sourcePath,
                        SkipKind.INVALID,
                        "tests must not be empty"));
                continue;
            }

            for (int index = 0; index < tests.size(); index++) {
                final Object rawTest = tests.get(index);
                final Map<String, Object> testDefinition = asStringObjectMap(rawTest, "test definition");
                final String description = trimToEmpty(testDefinition.get("description"));
                final String caseId = buildCaseId(sourcePath, index + 1, description);

                final String explicitSkipReason = readSkipReason(testDefinition);
                if (explicitSkipReason != null) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.SKIPPED,
                            explicitSkipReason));
                    continue;
                }

                final String runOnSkipReason = runOnSkipReason(testDefinition);
                if (runOnSkipReason != null) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.SKIPPED,
                            runOnSkipReason));
                    continue;
                }

                final Object operationsValue = testDefinition.get("operations");
                if (!(operationsValue instanceof List<?> operationsRaw)) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.INVALID,
                            "operations must be an array"));
                    continue;
                }
                final List<Object> operations = List.copyOf(operationsRaw);
                if (operations.isEmpty()) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.INVALID,
                            "operations must not be empty"));
                    continue;
                }

                final List<ScenarioCommand> commands = new ArrayList<>(operations.size());
                String unsupportedReason = null;
                String invalidReason = null;
                for (final Object rawOperation : operations) {
                    final Map<String, Object> operation = asStringObjectMap(rawOperation, "operation");
                    try {
                        commands.add(convertOperation(operation, defaultDatabase, defaultCollection));
                    } catch (final UnsupportedOperationException unsupported) {
                        unsupportedReason = unsupported.getMessage();
                        break;
                    } catch (final IllegalArgumentException invalid) {
                        invalidReason = invalid.getMessage();
                        break;
                    }
                }

                if (unsupportedReason != null) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.UNSUPPORTED,
                            unsupportedReason));
                    continue;
                }
                if (invalidReason != null) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.INVALID,
                            invalidReason));
                    continue;
                }

                imported.add(new ImportedScenario(
                        caseId,
                        sourcePath,
                        new Scenario(caseId, description, List.copyOf(commands))));
            }
        }

        return new ImportResult(List.copyOf(imported), List.copyOf(skipped));
    }

    private List<Path> discoverSpecFiles(final Path specRoot) throws IOException {
        final List<Path> files = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(specRoot)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> isSupportedSpecFile(path.getFileName().toString()))
                    .forEach(files::add);
        }
        files.sort(Comparator.comparing(Path::toString));
        return List.copyOf(files);
    }

    private Object parseFile(final Path file) throws IOException {
        final String fileName = file.getFileName().toString().toLowerCase(Locale.ROOT);
        final String content = Files.readString(file, StandardCharsets.UTF_8);
        if (fileName.endsWith(EXT_JSON)) {
            return Document.parse(content);
        }
        if (fileName.endsWith(EXT_YAML) || fileName.endsWith(EXT_YML)) {
            final Object loaded = yaml.load(content);
            if (!(loaded instanceof Map<?, ?> mapValue)) {
                throw new IllegalArgumentException("yaml spec root must be a document: " + file);
            }
            return mapValue;
        }
        throw new IllegalArgumentException("unsupported file extension: " + file);
    }

    private static ScenarioCommand convertOperation(
            final Map<String, Object> operation,
            final String defaultDatabase,
            final String defaultCollection) {
        final String operationName = requireText(operation.get("name"), "operation.name");
        final String objectName = trimToEmpty(operation.get("object"));
        final Map<String, Object> arguments = asStringObjectMap(
                operation.getOrDefault("arguments", Map.of()),
                "operation.arguments");
        final String database = fallbackText(arguments.get("database"), defaultDatabase, "database");
        final String collection = fallbackText(arguments.get("collection"), defaultCollection, "collection");

        return switch (operationName) {
            case "insertOne" -> insertOne(arguments, database, collection);
            case "insertMany" -> insertMany(arguments, database, collection);
            case "find" -> find(arguments, database, collection);
            case "aggregate" -> aggregate(arguments, database, collection);
            case "updateOne" -> update(arguments, database, collection, false);
            case "updateMany" -> update(arguments, database, collection, true);
            case "deleteOne" -> delete(arguments, database, collection, 1);
            case "deleteMany" -> delete(arguments, database, collection, 0);
            case "createIndex" -> createIndex(arguments, database, collection);
            default -> throw new UnsupportedOperationException("unsupported UTF operation: " + operationName);
        };
    }

    private static ScenarioCommand insertOne(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        if (Boolean.FALSE.equals(arguments.get("ordered"))) {
            throw new UnsupportedOperationException("unsupported UTF insertOne option: ordered=false");
        }
        final Map<String, Object> document =
                asStringObjectMap(arguments.get("document"), "insertOne.arguments.document");
        if (containsUnsupportedKeyPath(document)) {
            throw new UnsupportedOperationException("unsupported UTF insertOne document keys: dot or dollar path");
        }
        final Map<String, Object> payload = commandEnvelope("insert", database, collection);
        payload.put("documents", List.of(deepCopyValue(document)));
        return new ScenarioCommand("insert", immutableMap(payload));
    }

    private static ScenarioCommand insertMany(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        if (Boolean.FALSE.equals(arguments.get("ordered"))) {
            throw new UnsupportedOperationException("unsupported UTF insertMany option: ordered=false");
        }
        final List<Object> documents = asList(arguments.get("documents"), "insertMany.arguments.documents");
        final List<Object> copied = new ArrayList<>(documents.size());
        for (final Object document : documents) {
            final Map<String, Object> mapped = asStringObjectMap(document, "insertMany document");
            if (containsUnsupportedKeyPath(mapped)) {
                throw new UnsupportedOperationException("unsupported UTF insertMany document keys: dot or dollar path");
            }
            copied.add(deepCopyValue(mapped));
        }
        final Map<String, Object> payload = commandEnvelope("insert", database, collection);
        payload.put("documents", List.copyOf(copied));
        return new ScenarioCommand("insert", immutableMap(payload));
    }

    private static ScenarioCommand find(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> payload = commandEnvelope("find", database, collection);
        payload.put("filter", deepCopyValue(arguments.getOrDefault("filter", Map.of())));
        copyIfPresent(arguments, payload, "projection");
        copyIfPresent(arguments, payload, "sort");
        copyIfPresent(arguments, payload, "limit");
        copyIfPresent(arguments, payload, "skip");
        copyIfPresent(arguments, payload, "batchSize");
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("find", immutableMap(payload));
    }

    private static ScenarioCommand aggregate(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final List<Object> pipeline = asList(arguments.getOrDefault("pipeline", List.of()), "aggregate.arguments.pipeline");
        final List<Object> copiedPipeline = new ArrayList<>(pipeline.size());
        for (final Object stage : pipeline) {
            copiedPipeline.add(deepCopyValue(asStringObjectMap(stage, "aggregate stage")));
        }

        final Map<String, Object> payload = commandEnvelope("aggregate", database, collection);
        payload.put("pipeline", List.copyOf(copiedPipeline));
        final Map<String, Object> cursor = new LinkedHashMap<>();
        if (arguments.containsKey("batchSize")) {
            cursor.put("batchSize", deepCopyValue(arguments.get("batchSize")));
        }
        payload.put("cursor", immutableMap(cursor));
        copyIfPresent(arguments, payload, "allowDiskUse");
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("aggregate", immutableMap(payload));
    }

    private static ScenarioCommand update(
            final Map<String, Object> arguments,
            final String database,
            final String collection,
            final boolean multi) {
        final Object rawUpdate = arguments.containsKey("update")
                ? arguments.get("update")
                : arguments.get("replacement");
        if (rawUpdate == null) {
            throw new IllegalArgumentException("update operation requires update/replacement argument");
        }
        if (rawUpdate instanceof List<?>) {
            throw new UnsupportedOperationException("unsupported UTF update pipeline");
        }
        if (arguments.containsKey("arrayFilters")) {
            throw new UnsupportedOperationException("unsupported UTF update option: arrayFilters");
        }
        if (multi && isReplacementDocument(rawUpdate)) {
            throw new UnsupportedOperationException("unsupported UTF replacement update with multi=true");
        }

        final Map<String, Object> updateEntry = new LinkedHashMap<>();
        updateEntry.put("q", deepCopyValue(arguments.getOrDefault("filter", Map.of())));
        updateEntry.put("u", deepCopyValue(rawUpdate));
        updateEntry.put("multi", multi);
        updateEntry.put("upsert", Boolean.TRUE.equals(arguments.get("upsert")));
        copyIfPresent(arguments, updateEntry, "arrayFilters");
        copyIfPresent(arguments, updateEntry, "collation");
        copyIfPresent(arguments, updateEntry, "hint");

        final Map<String, Object> payload = commandEnvelope("update", database, collection);
        payload.put("updates", List.of(immutableMap(updateEntry)));
        return new ScenarioCommand("update", immutableMap(payload));
    }

    private static boolean isReplacementDocument(final Object rawUpdate) {
        if (!(rawUpdate instanceof Map<?, ?> mapped) || mapped.isEmpty()) {
            return false;
        }
        for (final Object key : mapped.keySet()) {
            if (!(key instanceof String field) || !field.startsWith("$")) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsUnsupportedKeyPath(final Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                if (key.startsWith("$") || key.contains(".")) {
                    return true;
                }
                if (containsUnsupportedKeyPath(entry.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof List<?> listValue) {
            for (final Object item : listValue) {
                if (containsUnsupportedKeyPath(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static ScenarioCommand delete(
            final Map<String, Object> arguments,
            final String database,
            final String collection,
            final int limit) {
        final Map<String, Object> deleteEntry = new LinkedHashMap<>();
        deleteEntry.put("q", deepCopyValue(arguments.getOrDefault("filter", Map.of())));
        deleteEntry.put("limit", limit);
        copyIfPresent(arguments, deleteEntry, "collation");
        copyIfPresent(arguments, deleteEntry, "hint");

        final Map<String, Object> payload = commandEnvelope("delete", database, collection);
        payload.put("deletes", List.of(immutableMap(deleteEntry)));
        return new ScenarioCommand("delete", immutableMap(payload));
    }

    private static ScenarioCommand createIndex(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Object keyValue = arguments.containsKey("key") ? arguments.get("key") : arguments.get("keys");
        final Map<String, Object> key = asStringObjectMap(keyValue, "createIndex.arguments.key");
        final Map<String, Object> indexEntry = new LinkedHashMap<>();
        indexEntry.put("key", deepCopyValue(key));

        final String explicitName = trimToEmpty(arguments.get("name"));
        indexEntry.put("name", explicitName.isEmpty() ? defaultIndexName(key) : explicitName);
        copyIfPresent(arguments, indexEntry, "unique");
        copyIfPresent(arguments, indexEntry, "sparse");
        copyIfPresent(arguments, indexEntry, "partialFilterExpression");
        copyIfPresent(arguments, indexEntry, "collation");
        copyIfPresent(arguments, indexEntry, "expireAfterSeconds");

        final Map<String, Object> payload = commandEnvelope("createIndexes", database, collection);
        payload.put("indexes", List.of(immutableMap(indexEntry)));
        return new ScenarioCommand("createIndexes", immutableMap(payload));
    }

    private static String defaultIndexName(final Map<String, Object> key) {
        final List<String> parts = new ArrayList<>(key.size());
        for (final Map.Entry<String, Object> entry : key.entrySet()) {
            parts.add(entry.getKey() + "_" + String.valueOf(entry.getValue()));
        }
        return String.join("_", parts);
    }

    private static Map<String, Object> commandEnvelope(
            final String commandField,
            final String database,
            final String collection) {
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put(commandField, collection);
        payload.put("$db", database);
        return payload;
    }

    private static void copyIfPresent(
            final Map<String, Object> source,
            final Map<String, Object> target,
            final String key) {
        if (!source.containsKey(key)) {
            return;
        }
        target.put(key, deepCopyValue(source.get(key)));
    }

    private static String readDefaultDatabase(final Map<String, Object> spec) {
        final String database = firstNonBlank(
                trimToEmpty(spec.get("database_name")),
                trimToEmpty(spec.get("databaseName")),
                trimToEmpty(spec.get("database")));
        return database == null ? "test" : database;
    }

    private static String readDefaultCollection(final Map<String, Object> spec) {
        final String collection = firstNonBlank(
                trimToEmpty(spec.get("collection_name")),
                trimToEmpty(spec.get("collectionName")),
                trimToEmpty(spec.get("collection")));
        return collection == null ? "coll" : collection;
    }

    private static String readSkipReason(final Map<String, Object> testDefinition) {
        if (testDefinition.containsKey("skipReason")) {
            return requireText(testDefinition.get("skipReason"), "skipReason");
        }
        final Object skipValue = testDefinition.get("skip");
        if (skipValue instanceof Boolean skipBoolean && skipBoolean) {
            return "skip=true";
        }
        if (skipValue != null) {
            final String normalized = trimToEmpty(skipValue);
            if (!normalized.isEmpty()) {
                return "skip=" + normalized;
            }
        }
        return null;
    }

    private static String runOnSkipReason(final Map<String, Object> testDefinition) {
        if (!testDefinition.containsKey("runOnRequirements")) {
            return null;
        }
        final List<Object> requirements = asList(testDefinition.get("runOnRequirements"), "runOnRequirements");
        if (requirements.isEmpty()) {
            return null;
        }
        return "runOnRequirements not evaluated by importer";
    }

    private static String fallbackText(
            final Object value,
            final String fallback,
            final String fieldName) {
        final String normalized = trimToEmpty(value);
        if (!normalized.isEmpty()) {
            return normalized;
        }
        if (fallback != null && !fallback.isBlank()) {
            return fallback;
        }
        throw new IllegalArgumentException(fieldName + " must not be blank");
    }

    private static String relativePath(final Path root, final Path file) {
        return root.normalize().relativize(file.normalize()).toString().replace('\\', '/');
    }

    private static boolean isSupportedSpecFile(final String fileName) {
        final String normalized = fileName.toLowerCase(Locale.ROOT);
        return normalized.endsWith(EXT_JSON)
                || normalized.endsWith(EXT_YAML)
                || normalized.endsWith(EXT_YML);
    }

    private static String buildCaseId(
            final String relativePath,
            final int ordinal,
            final String description) {
        final String descriptionSlug = slug(description);
        return relativePath + "::" + ordinal + ":" + descriptionSlug;
    }

    private static String slug(final String rawText) {
        final String normalized = trimToEmpty(rawText).toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return "case";
        }
        final String slug = normalized.replaceAll("[^a-z0-9]+", "-").replaceAll("^-+|-+$", "");
        return slug.isEmpty() ? "case" : slug;
    }

    private static String firstNonBlank(final String... values) {
        for (final String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String requireText(final Object value, final String fieldName) {
        final String normalized = trimToEmpty(value);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String trimToEmpty(final Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value).trim();
    }

    private static List<Object> asList(final Object value, final String fieldName) {
        if (!(value instanceof List<?> rawList)) {
            throw new IllegalArgumentException(fieldName + " must be an array");
        }
        return List.copyOf(rawList);
    }

    private static Map<String, Object> asStringObjectMap(final Object value, final String fieldName) {
        if (!(value instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException(fieldName + " must be a document");
        }
        final Map<String, Object> normalized = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : rawMap.entrySet()) {
            final Object rawKey = entry.getKey();
            if (!(rawKey instanceof String key)) {
                throw new IllegalArgumentException(fieldName + " keys must be strings");
            }
            normalized.put(key, entry.getValue());
        }
        return Collections.unmodifiableMap(normalized);
    }

    private static Object deepCopyValue(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> mapValue) {
            final Map<String, Object> copied = new LinkedHashMap<>();
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copied.put(String.valueOf(entry.getKey()), deepCopyValue(entry.getValue()));
            }
            return immutableMap(copied);
        }
        if (value instanceof List<?> listValue) {
            final List<Object> copied = new ArrayList<>(listValue.size());
            for (final Object item : listValue) {
                copied.add(deepCopyValue(item));
            }
            return List.copyOf(copied);
        }
        return value;
    }

    private static Map<String, Object> immutableMap(final Map<String, Object> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    public enum SkipKind {
        SKIPPED,
        UNSUPPORTED,
        INVALID
    }

    public record ImportedScenario(String caseId, String sourcePath, Scenario scenario) {
        public ImportedScenario {
            Objects.requireNonNull(caseId, "caseId");
            Objects.requireNonNull(sourcePath, "sourcePath");
            Objects.requireNonNull(scenario, "scenario");
        }
    }

    public record SkippedCase(String caseId, String sourcePath, SkipKind kind, String reason) {
        public SkippedCase {
            Objects.requireNonNull(caseId, "caseId");
            Objects.requireNonNull(sourcePath, "sourcePath");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(reason, "reason");
        }
    }

    public record ImportResult(List<ImportedScenario> importedScenarios, List<SkippedCase> skippedCases) {
        public ImportResult {
            importedScenarios = List.copyOf(Objects.requireNonNull(importedScenarios, "importedScenarios"));
            skippedCases = List.copyOf(Objects.requireNonNull(skippedCases, "skippedCases"));
        }

        public int importedCount() {
            return importedScenarios.size();
        }

        public int skippedCount() {
            int count = 0;
            for (final SkippedCase skippedCase : skippedCases) {
                if (skippedCase.kind() == SkipKind.SKIPPED || skippedCase.kind() == SkipKind.INVALID) {
                    count++;
                }
            }
            return count;
        }

        public int unsupportedCount() {
            int count = 0;
            for (final SkippedCase skippedCase : skippedCases) {
                if (skippedCase.kind() == SkipKind.UNSUPPORTED) {
                    count++;
                }
            }
            return count;
        }
    }
}
