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
            final FileConversionContext baseContext = FileConversionContext.fromSpec(
                    defaultDatabase,
                    defaultCollection,
                    spec);
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
                final FileConversionContext context = baseContext.copy();
                String unsupportedReason = null;
                String invalidReason = null;
                for (final Object rawOperation : operations) {
                    final Map<String, Object> operation = asStringObjectMap(rawOperation, "operation");
                    try {
                        commands.addAll(context.convertOperation(operation));
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
                if (commands.isEmpty()) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.SKIPPED,
                            "no executable operations after setup/policy filtering"));
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

    private static ScenarioCommand convertCrudOperation(
            final String operationName,
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        return switch (operationName) {
            case "insertOne" -> insertOne(arguments, database, collection);
            case "insertMany" -> insertMany(arguments, database, collection);
            case "find" -> find(arguments, database, collection);
            case "aggregate" -> aggregate(arguments, database, collection);
            case "count" -> countDocuments(arguments, database, collection);
            case "countDocuments" -> countDocuments(arguments, database, collection);
            case "distinct" -> distinct(arguments, database, collection);
            case "updateOne" -> update(arguments, database, collection, false);
            case "updateMany" -> update(arguments, database, collection, true);
            case "replaceOne" -> replaceOne(arguments, database, collection);
            case "deleteOne" -> delete(arguments, database, collection, 1);
            case "deleteMany" -> delete(arguments, database, collection, 0);
            case "findOneAndDelete" -> findOneAndDelete(arguments, database, collection);
            case "findOneAndUpdate" -> findOneAndUpdate(arguments, database, collection);
            case "findOneAndReplace" -> findOneAndReplace(arguments, database, collection);
            case "bulkWrite" -> bulkWrite(arguments, database, collection);
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

    private static ScenarioCommand countDocuments(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> payload = commandEnvelope("countDocuments", database, collection);
        if (arguments.containsKey("filter")) {
            payload.put("filter", deepCopyValue(arguments.get("filter")));
        } else if (arguments.containsKey("query")) {
            payload.put("filter", deepCopyValue(arguments.get("query")));
        } else {
            payload.put("filter", Map.of());
        }
        copyIfPresent(arguments, payload, "skip");
        copyIfPresent(arguments, payload, "limit");
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("countDocuments", immutableMap(payload));
    }

    private static ScenarioCommand distinct(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> payload = commandEnvelope("distinct", database, collection);
        final String key = firstNonBlank(
                trimToEmpty(arguments.get("key")),
                trimToEmpty(arguments.get("fieldName")),
                trimToEmpty(arguments.get("field")));
        if (key == null) {
            throw new IllegalArgumentException("distinct operation requires key/fieldName argument");
        }
        payload.put("key", key);
        if (arguments.containsKey("filter")) {
            payload.put("query", deepCopyValue(arguments.get("filter")));
        } else if (arguments.containsKey("query")) {
            payload.put("query", deepCopyValue(arguments.get("query")));
        } else {
            payload.put("query", Map.of());
        }
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("distinct", immutableMap(payload));
    }

    private static ScenarioCommand aggregate(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final List<Object> pipeline = asList(arguments.getOrDefault("pipeline", List.of()), "aggregate.arguments.pipeline");
        final List<Object> copiedPipeline = new ArrayList<>(pipeline.size());
        for (final Object stage : pipeline) {
            final Map<String, Object> stageMap = asStringObjectMap(stage, "aggregate stage");
            if (containsUnsupportedAggregateStage(stageMap)) {
                throw new UnsupportedOperationException("unsupported UTF aggregate stage in pipeline");
            }
            copiedPipeline.add(deepCopyValue(stageMap));
        }
        if (arguments.containsKey("bypassDocumentValidation")) {
            throw new UnsupportedOperationException("unsupported UTF aggregate option: bypassDocumentValidation");
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

    private static boolean containsUnsupportedAggregateStage(final Map<String, Object> stage) {
        if (stage.containsKey("$out") || stage.containsKey("$merge") || stage.containsKey("$listLocalSessions")) {
            return true;
        }
        for (final Object value : stage.values()) {
            if (containsUnsupportedAggregateStageValue(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsUnsupportedAggregateStageValue(final Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                if ("$out".equals(key) || "$merge".equals(key) || "$listLocalSessions".equals(key)) {
                    return true;
                }
                if (containsUnsupportedAggregateStageValue(entry.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof List<?> listValue) {
            for (final Object item : listValue) {
                if (containsUnsupportedAggregateStageValue(item)) {
                    return true;
                }
            }
        }
        return false;
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

    private static ScenarioCommand replaceOne(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> replacement = asStringObjectMap(
                arguments.get("replacement"),
                "replaceOne.arguments.replacement");
        final Map<String, Object> payload = commandEnvelope("replaceOne", database, collection);
        payload.put("filter", deepCopyValue(arguments.getOrDefault("filter", Map.of())));
        payload.put("replacement", deepCopyValue(replacement));
        copyIfPresent(arguments, payload, "upsert");
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("replaceOne", immutableMap(payload));
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

    private static ScenarioCommand findOneAndUpdate(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Object updateValue = arguments.get("update");
        if (updateValue == null) {
            throw new IllegalArgumentException("findOneAndUpdate operation requires update argument");
        }
        if (updateValue instanceof List<?>) {
            throw new UnsupportedOperationException("unsupported UTF update pipeline");
        }
        if (arguments.containsKey("arrayFilters")) {
            throw new UnsupportedOperationException("unsupported UTF update option: arrayFilters");
        }

        final Map<String, Object> payload = commandEnvelope("findOneAndUpdate", database, collection);
        payload.put("filter", deepCopyValue(arguments.getOrDefault("filter", Map.of())));
        payload.put("update", deepCopyValue(updateValue));
        copyIfPresent(arguments, payload, "sort");
        copyIfPresent(arguments, payload, "projection");
        copyIfPresent(arguments, payload, "upsert");
        copyNormalizedReturnDocumentIfPresent(arguments, payload);
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("findOneAndUpdate", immutableMap(payload));
    }

    private static ScenarioCommand findOneAndDelete(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> payload = commandEnvelope("findAndModify", database, collection);
        payload.put("query", deepCopyValue(arguments.getOrDefault("filter", Map.of())));
        payload.put("remove", true);
        copyIfPresent(arguments, payload, "sort");
        if (arguments.containsKey("projection")) {
            payload.put("fields", deepCopyValue(arguments.get("projection")));
        }
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("findAndModify", immutableMap(payload));
    }

    private static ScenarioCommand findOneAndReplace(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> replacement = asStringObjectMap(
                arguments.get("replacement"),
                "findOneAndReplace.arguments.replacement");
        final Map<String, Object> payload = commandEnvelope("findOneAndReplace", database, collection);
        payload.put("filter", deepCopyValue(arguments.getOrDefault("filter", Map.of())));
        payload.put("replacement", deepCopyValue(replacement));
        copyIfPresent(arguments, payload, "sort");
        copyIfPresent(arguments, payload, "projection");
        copyIfPresent(arguments, payload, "upsert");
        copyNormalizedReturnDocumentIfPresent(arguments, payload);
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("findOneAndReplace", immutableMap(payload));
    }

    private static ScenarioCommand bulkWrite(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Object orderedValue = arguments.get("ordered");
        if (orderedValue != null && !(orderedValue instanceof Boolean)) {
            throw new IllegalArgumentException("bulkWrite.arguments.ordered must be a boolean");
        }
        if (Boolean.FALSE.equals(orderedValue)) {
            throw new UnsupportedOperationException("unsupported UTF bulkWrite option: ordered=false");
        }

        final List<Object> requests = asList(arguments.get("requests"), "bulkWrite.arguments.requests");
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("bulkWrite.arguments.requests must not be empty");
        }

        final List<Object> operations = new ArrayList<>(requests.size());
        for (final Object request : requests) {
            final Map<String, Object> requestDocument = asStringObjectMap(request, "bulkWrite request");
            if (requestDocument.size() != 1) {
                throw new IllegalArgumentException("bulkWrite request must contain exactly one operation");
            }

            final String operationName = requestDocument.keySet().iterator().next();
            final String normalizedOperationName = operationName.toLowerCase(Locale.ROOT);
            final Map<String, Object> operationArguments = asStringObjectMap(
                    requestDocument.get(operationName),
                    "bulkWrite request operation arguments");

            switch (normalizedOperationName) {
                case "insertone" -> {
                    final Map<String, Object> document = asStringObjectMap(
                            operationArguments.get("document"),
                            "bulkWrite.insertOne.document");
                    if (containsUnsupportedKeyPath(document)) {
                        throw new UnsupportedOperationException(
                                "unsupported UTF bulkWrite insertOne document keys: dot or dollar path");
                    }
                }
                case "updateone" -> validateBulkWriteUpdate(operationArguments, false);
                case "updatemany" -> validateBulkWriteUpdate(operationArguments, true);
                case "deleteone", "deletemany", "replaceone" -> {
                    // Accepted and forwarded as-is for command-layer validation/execution.
                }
                default -> throw new UnsupportedOperationException(
                        "unsupported UTF bulkWrite operation: " + operationName);
            }

            operations.add(deepCopyValue(requestDocument));
        }

        final Map<String, Object> payload = commandEnvelope("bulkWrite", database, collection);
        payload.put("ordered", true);
        payload.put("operations", List.copyOf(operations));
        return new ScenarioCommand("bulkWrite", immutableMap(payload));
    }

    private static void validateBulkWriteUpdate(final Map<String, Object> operationArguments, final boolean multi) {
        final Object updateValue = operationArguments.get("update");
        if (updateValue == null) {
            throw new IllegalArgumentException("bulkWrite update operation requires update argument");
        }
        if (updateValue instanceof List<?>) {
            throw new UnsupportedOperationException("unsupported UTF update pipeline");
        }
        if (operationArguments.containsKey("arrayFilters")) {
            throw new UnsupportedOperationException("unsupported UTF update option: arrayFilters");
        }
        if (multi && isReplacementDocument(updateValue)) {
            throw new UnsupportedOperationException("unsupported UTF replacement update with multi=true");
        }
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

    private static void copyNormalizedReturnDocumentIfPresent(
            final Map<String, Object> source,
            final Map<String, Object> target) {
        if (!source.containsKey("returnDocument")) {
            return;
        }
        final Object rawValue = source.get("returnDocument");
        if (rawValue instanceof String textValue) {
            target.put("returnDocument", textValue.toLowerCase(Locale.ROOT));
            return;
        }
        target.put("returnDocument", deepCopyValue(rawValue));
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

    private static final class FileConversionContext {
        private final String defaultDatabase;
        private final String defaultCollection;
        private final Map<String, String> databaseAliases;
        private final Map<String, CollectionTarget> collectionAliases;
        private final Map<String, SessionState> sessions;

        private FileConversionContext(
                final String defaultDatabase,
                final String defaultCollection,
                final Map<String, String> databaseAliases,
                final Map<String, CollectionTarget> collectionAliases,
                final Map<String, SessionState> sessions) {
            this.defaultDatabase = defaultDatabase;
            this.defaultCollection = defaultCollection;
            this.databaseAliases = databaseAliases;
            this.collectionAliases = collectionAliases;
            this.sessions = sessions;
        }

        private static FileConversionContext fromSpec(
                final String defaultDatabase,
                final String defaultCollection,
                final Map<String, Object> spec) {
            final FileConversionContext context = new FileConversionContext(
                    defaultDatabase,
                    defaultCollection,
                    new LinkedHashMap<>(),
                    new LinkedHashMap<>(),
                    new LinkedHashMap<>());
            final Object createEntities = spec.get("createEntities");
            if (createEntities != null) {
                context.applyCreateEntities(createEntities);
            }
            return context;
        }

        private FileConversionContext copy() {
            final Map<String, SessionState> copiedSessions = new LinkedHashMap<>(sessions.size());
            for (final Map.Entry<String, SessionState> entry : sessions.entrySet()) {
                copiedSessions.put(entry.getKey(), entry.getValue().copy());
            }
            return new FileConversionContext(
                    defaultDatabase,
                    defaultCollection,
                    new LinkedHashMap<>(databaseAliases),
                    new LinkedHashMap<>(collectionAliases),
                    copiedSessions);
        }

        private List<ScenarioCommand> convertOperation(final Map<String, Object> operation) {
            final String operationName = requireText(operation.get("name"), "operation.name");
            final String objectName = trimToEmpty(operation.get("object"));
            final Map<String, Object> arguments = asStringObjectMap(
                    operation.getOrDefault("arguments", Map.of()),
                    "operation.arguments");

            return switch (operationName) {
                case "createEntities" -> handleCreateEntities(arguments);
                case "startTransaction" -> {
                    startTransaction(objectName, arguments);
                    yield List.of();
                }
                case "commitTransaction" -> List.of(completeTransaction("commitTransaction", objectName, arguments));
                case "abortTransaction" -> List.of(completeTransaction("abortTransaction", objectName, arguments));
                case "failPoint" -> throw new UnsupportedOperationException("unsupported-by-policy UTF operation: failPoint");
                default -> {
                    final CollectionTarget target = resolveCollectionTarget(objectName, arguments);
                    final ScenarioCommand converted = convertCrudOperation(
                            operationName,
                            arguments,
                            target.database(),
                            target.collection());
                    yield List.of(applySessionEnvelope(converted, arguments));
                }
            };
        }

        private List<ScenarioCommand> handleCreateEntities(final Map<String, Object> arguments) {
            applyCreateEntities(arguments.get("entities"));
            return List.of();
        }

        private void applyCreateEntities(final Object entitiesValue) {
            final List<Object> entities = asList(entitiesValue, "createEntities.entities");
            for (final Object entitySpec : entities) {
                final Map<String, Object> wrapper = asStringObjectMap(entitySpec, "createEntities.entity");
                if (wrapper.size() != 1) {
                    throw new IllegalArgumentException("createEntities entity must contain exactly one type");
                }
                final Map.Entry<String, Object> entry = wrapper.entrySet().iterator().next();
                final String entityType = entry.getKey();
                final Map<String, Object> entity = asStringObjectMap(entry.getValue(), "createEntities." + entityType);
                switch (entityType) {
                    case "client" -> requireText(entity.get("id"), "createEntities.client.id");
                    case "database" -> registerDatabaseEntity(entity);
                    case "collection" -> registerCollectionEntity(entity);
                    case "session" -> registerSessionEntity(entity);
                    default -> throw new UnsupportedOperationException(
                            "unsupported UTF createEntities entity type: " + entityType);
                }
            }
        }

        private void registerDatabaseEntity(final Map<String, Object> entity) {
            final String id = requireText(entity.get("id"), "createEntities.database.id");
            final String databaseName = firstNonBlank(
                    trimToEmpty(entity.get("databaseName")),
                    trimToEmpty(entity.get("database_name")),
                    trimToEmpty(entity.get("database")));
            databaseAliases.put(id, fallbackText(databaseName, defaultDatabase, "createEntities.database.databaseName"));
        }

        private void registerCollectionEntity(final Map<String, Object> entity) {
            final String id = requireText(entity.get("id"), "createEntities.collection.id");
            final String collectionName = firstNonBlank(
                    trimToEmpty(entity.get("collectionName")),
                    trimToEmpty(entity.get("collection_name")),
                    trimToEmpty(entity.get("collection")));
            final String databaseRef = firstNonBlank(
                    trimToEmpty(entity.get("database")),
                    trimToEmpty(entity.get("databaseName")),
                    trimToEmpty(entity.get("database_name")));
            final String databaseName = resolveDatabaseName(databaseRef);
            collectionAliases.put(
                    id,
                    new CollectionTarget(
                            fallbackText(databaseName, defaultDatabase, "database"),
                            fallbackText(collectionName, defaultCollection, "collection")));
        }

        private void registerSessionEntity(final Map<String, Object> entity) {
            final String id = requireText(entity.get("id"), "createEntities.session.id");
            sessions.putIfAbsent(id, new SessionState());
        }

        private String resolveDatabaseName(final String rawDatabase) {
            if (rawDatabase == null || rawDatabase.isBlank()) {
                return defaultDatabase;
            }
            final String byAlias = databaseAliases.get(rawDatabase);
            if (byAlias != null && !byAlias.isBlank()) {
                return byAlias;
            }
            return rawDatabase;
        }

        private CollectionTarget resolveCollectionTarget(
                final String objectName,
                final Map<String, Object> arguments) {
            String database = trimToEmpty(arguments.get("database"));
            String collection = trimToEmpty(arguments.get("collection"));

            final CollectionTarget byAlias = objectName.isEmpty() ? null : collectionAliases.get(objectName);
            if (byAlias != null) {
                if (database.isEmpty()) {
                    database = byAlias.database();
                }
                if (collection.isEmpty()) {
                    collection = byAlias.collection();
                }
            }

            if (database.isEmpty() && !objectName.isEmpty() && databaseAliases.containsKey(objectName)) {
                database = databaseAliases.get(objectName);
            }
            if (collection.isEmpty() && !objectName.isEmpty() && !"testRunner".equals(objectName)) {
                collection = objectName;
            }

            return new CollectionTarget(
                    fallbackText(database, defaultDatabase, "database"),
                    fallbackText(collection, defaultCollection, "collection"));
        }

        private void startTransaction(final String objectName, final Map<String, Object> arguments) {
            final String sessionId = resolveSessionId("startTransaction", objectName, arguments);
            final SessionState state = sessions.computeIfAbsent(sessionId, key -> new SessionState());
            state.startTransaction(arguments.get("readConcern"));
        }

        private ScenarioCommand completeTransaction(
                final String commandName,
                final String objectName,
                final Map<String, Object> arguments) {
            final String sessionId = resolveSessionId(commandName, objectName, arguments);
            final SessionState state = sessions.computeIfAbsent(sessionId, key -> new SessionState());
            final long txnNumber = state.txnNumberForCompletion();

            final Map<String, Object> payload = new LinkedHashMap<>();
            payload.put(commandName, 1);
            payload.put("$db", "admin");
            payload.put("lsid", immutableMap(Map.of("id", sessionId)));
            payload.put("txnNumber", txnNumber);
            payload.put("autocommit", false);
            copyIfPresent(arguments, payload, "writeConcern");
            copyIfPresent(arguments, payload, "maxTimeMS");
            copyIfPresent(arguments, payload, "maxCommitTimeMS");

            state.finishTransaction();
            return new ScenarioCommand(commandName, immutableMap(payload));
        }

        private ScenarioCommand applySessionEnvelope(
                final ScenarioCommand command,
                final Map<String, Object> arguments) {
            final String sessionId = trimToEmpty(arguments.get("session"));
            if (sessionId.isEmpty()) {
                return command;
            }

            final SessionState state = sessions.computeIfAbsent(sessionId, key -> new SessionState());
            final Map<String, Object> payload = new LinkedHashMap<>(command.payload());
            payload.put("lsid", immutableMap(Map.of("id", sessionId)));

            if (state.activeTxnNumber() != null) {
                payload.put("txnNumber", state.activeTxnNumber());
                payload.put("autocommit", false);
                if (state.startPending()) {
                    payload.put("startTransaction", true);
                    if (state.pendingReadConcern() != null && !payload.containsKey("readConcern")) {
                        payload.put("readConcern", deepCopyValue(state.pendingReadConcern()));
                    }
                    state.markStartConsumed();
                }
            }
            return new ScenarioCommand(command.commandName(), immutableMap(payload));
        }

        private String resolveSessionId(
                final String operationName,
                final String objectName,
                final Map<String, Object> arguments) {
            final String sessionFromArgs = trimToEmpty(arguments.get("session"));
            if (!sessionFromArgs.isEmpty()) {
                return sessionFromArgs;
            }
            final String sessionFromObject = trimToEmpty(objectName);
            if (!sessionFromObject.isEmpty()) {
                return sessionFromObject;
            }
            throw new IllegalArgumentException(operationName + " requires a session identifier");
        }
    }

    private record CollectionTarget(String database, String collection) {}

    private static final class SessionState {
        private long nextTxnNumber;
        private Long activeTxnNumber;
        private Long lastTxnNumber;
        private boolean startPending;
        private Object pendingReadConcern;

        private SessionState() {
            this.nextTxnNumber = 1L;
        }

        private SessionState(final SessionState source) {
            this.nextTxnNumber = source.nextTxnNumber;
            this.activeTxnNumber = source.activeTxnNumber;
            this.lastTxnNumber = source.lastTxnNumber;
            this.startPending = source.startPending;
            this.pendingReadConcern = deepCopyValue(source.pendingReadConcern);
        }

        private SessionState copy() {
            return new SessionState(this);
        }

        private void startTransaction(final Object readConcern) {
            final long txnNumber = nextTxnNumber++;
            this.activeTxnNumber = txnNumber;
            this.lastTxnNumber = txnNumber;
            this.startPending = true;
            this.pendingReadConcern = deepCopyValue(readConcern);
        }

        private long txnNumberForCompletion() {
            if (activeTxnNumber != null) {
                return activeTxnNumber;
            }
            if (lastTxnNumber != null) {
                return lastTxnNumber;
            }
            final long fallback = Math.max(1L, nextTxnNumber - 1L);
            this.lastTxnNumber = fallback;
            return fallback;
        }

        private void finishTransaction() {
            this.activeTxnNumber = null;
            this.startPending = false;
            this.pendingReadConcern = null;
        }

        private Long activeTxnNumber() {
            return activeTxnNumber;
        }

        private boolean startPending() {
            return startPending;
        }

        private Object pendingReadConcern() {
            return pendingReadConcern;
        }

        private void markStartConsumed() {
            this.startPending = false;
            this.pendingReadConcern = null;
        }
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
