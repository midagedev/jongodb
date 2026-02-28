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
    private static final String RUN_ON_LANES_PROPERTY = "jongodb.utf.runOnLanes";
    private static final String RUN_ON_LANES_ENV = "JONGODB_UTF_RUNON_LANES";
    private static final Map<String, String> SUPPORTED_RUN_COMMANDS = Map.of(
            "ping", "ping",
            "buildinfo", "buildInfo",
            "listindexes", "listIndexes",
            // listCollections is currently treated as a deterministic control subset.
            "listcollections", "ping",
            "insert", "insert",
            "find", "find",
            "count", "count");

    private final Yaml yaml;
    private final ImportProfile profile;
    private final boolean runOnLaneAdjustmentsEnabled;

    public UnifiedSpecImporter() {
        this(ImportProfile.STRICT);
    }

    public UnifiedSpecImporter(final ImportProfile profile) {
        this(profile, resolveRunOnLaneAdjustmentsEnabled());
    }

    public UnifiedSpecImporter(final ImportProfile profile, final boolean runOnLaneAdjustmentsEnabled) {
        this.yaml = new Yaml();
        this.profile = Objects.requireNonNull(profile, "profile");
        this.runOnLaneAdjustmentsEnabled = runOnLaneAdjustmentsEnabled;
    }

    public ImportProfile profile() {
        return profile;
    }

    public ImportResult importCorpus(final Path specRoot) throws IOException {
        return importCorpus(specRoot, RunOnContext.unevaluated());
    }

    public ImportResult importCorpus(final Path specRoot, final RunOnContext runOnContext) throws IOException {
        Objects.requireNonNull(specRoot, "specRoot");
        Objects.requireNonNull(runOnContext, "runOnContext");
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
                    spec,
                    profile,
                    sourcePath);
            final String fileRunOnSkipReason =
                    runOnSkipReason(spec, sourcePath, runOnContext, true, runOnLaneAdjustmentsEnabled);
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

                if (fileRunOnSkipReason != null) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.SKIPPED,
                            fileRunOnSkipReason));
                    continue;
                }

                final String explicitSkipReason = readSkipReason(testDefinition);
                if (explicitSkipReason != null) {
                    skipped.add(new SkippedCase(
                            caseId,
                            sourcePath,
                            SkipKind.SKIPPED,
                            explicitSkipReason));
                    continue;
                }

                final String runOnSkipReason =
                        runOnSkipReason(testDefinition, sourcePath, runOnContext, true, runOnLaneAdjustmentsEnabled);
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
            case "findOne" -> findOne(arguments, database, collection);
            case "aggregate" -> aggregate(arguments, database, collection);
            case "count" -> countDocuments(arguments, database, collection);
            case "countDocuments" -> countDocuments(arguments, database, collection);
            case "estimatedDocumentCount" -> estimatedDocumentCount(arguments, database, collection);
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
            case "clientBulkWrite" -> clientBulkWrite(arguments, database, collection);
            case "createIndex" -> createIndex(arguments, database, collection);
            case "runCommand" -> runCommand(arguments, database);
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
        if (containsDollarPrefixedKeyInIdDocument(document.get("_id"))) {
            throw new UnsupportedOperationException(
                    "unsupported UTF insertOne _id document with dollar-prefixed keys");
        }
        final Map<String, Object> payload = commandEnvelope("insert", database, collection);
        payload.put("documents", List.of(deepCopyValue(document)));
        return new ScenarioCommand("insert", immutableMap(payload));
    }

    private static boolean containsDollarPrefixedKeyInIdDocument(final Object idValue) {
        if (idValue instanceof Map<?, ?> mapValue) {
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                if (key.startsWith("$")) {
                    return true;
                }
                if (containsDollarPrefixedKeyInIdDocument(entry.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (idValue instanceof List<?> listValue) {
            for (final Object item : listValue) {
                if (containsDollarPrefixedKeyInIdDocument(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static ScenarioCommand insertMany(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final List<Object> documents = asList(arguments.get("documents"), "insertMany.arguments.documents");
        final List<Object> copied = new ArrayList<>(documents.size());
        for (final Object document : documents) {
            final Map<String, Object> mapped = asStringObjectMap(document, "insertMany document");
            copied.add(deepCopyValue(mapped));
        }
        final Map<String, Object> payload = commandEnvelope("insert", database, collection);
        payload.put("documents", List.copyOf(copied));
        copyIfPresent(arguments, payload, "ordered");
        return new ScenarioCommand("insert", immutableMap(payload));
    }

    private static ScenarioCommand find(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("find", arguments);
        final Map<String, Object> payload = commandEnvelope("find", database, collection);
        payload.put("filter", deepCopyValue(normalizedArguments.getOrDefault("filter", Map.of())));
        copyIfPresent(normalizedArguments, payload, "projection");
        copyIfPresent(normalizedArguments, payload, "sort");
        copyIfPresent(normalizedArguments, payload, "limit");
        copyIfPresent(normalizedArguments, payload, "skip");
        copyIfPresent(normalizedArguments, payload, "batchSize");
        copyIfPresent(normalizedArguments, payload, "hint");
        copyIfPresent(normalizedArguments, payload, "collation");
        return new ScenarioCommand("find", immutableMap(payload));
    }

    private static ScenarioCommand findOne(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> normalizedArguments = new LinkedHashMap<>(arguments);
        normalizedArguments.putIfAbsent("limit", 1);
        return find(immutableMap(normalizedArguments), database, collection);
    }

    private static ScenarioCommand countDocuments(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        rejectUnsupportedLetOption("countDocuments", arguments);
        final Map<String, Object> payload = commandEnvelope("countDocuments", database, collection);
        if (arguments.containsKey("filter")) {
            payload.put("filter", deepCopyValue(arguments.get("filter")));
        } else if (arguments.containsKey("query")) {
            payload.put("query", deepCopyValue(arguments.get("query")));
        } else {
            payload.put("filter", Map.of());
        }
        copyIfPresent(arguments, payload, "skip");
        copyIfPresent(arguments, payload, "limit");
        copyIfPresent(arguments, payload, "hint");
        copyIfPresent(arguments, payload, "collation");
        return new ScenarioCommand("countDocuments", immutableMap(payload));
    }

    private static ScenarioCommand estimatedDocumentCount(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> payload = commandEnvelope("count", database, collection);
        payload.put("query", Map.of());
        copyIfPresent(arguments, payload, "maxTimeMS");
        copyIfPresent(arguments, payload, "comment");
        // In the current strict lane (<8.2), rawData must be ignored.
        return new ScenarioCommand("count", immutableMap(payload));
    }

    private static ScenarioCommand distinct(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        rejectUnsupportedLetOption("distinct", arguments);
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
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("aggregate", arguments);
        final List<Object> pipeline = asList(
                normalizedArguments.getOrDefault("pipeline", List.of()),
                "aggregate.arguments.pipeline");
        final List<Object> copiedPipeline = new ArrayList<>(pipeline.size());
        for (final Object stage : pipeline) {
            final Map<String, Object> stageMap = asStringObjectMap(stage, "aggregate stage");
            final Map<String, Object> normalizedStage = normalizeAggregateStage(stageMap);
            if (containsUnsupportedAggregateStage(normalizedStage)) {
                throw new UnsupportedOperationException("unsupported UTF aggregate stage in pipeline");
            }
            copiedPipeline.add(deepCopyValue(normalizedStage));
        }

        final Map<String, Object> payload = commandEnvelope("aggregate", database, collection);
        payload.put("pipeline", List.copyOf(copiedPipeline));
        if (normalizedArguments.containsKey("bypassDocumentValidation")) {
            if (!Boolean.FALSE.equals(normalizedArguments.get("bypassDocumentValidation"))) {
                throw new UnsupportedOperationException("unsupported UTF aggregate option: bypassDocumentValidation");
            }
            payload.put("bypassDocumentValidation", false);
        }
        final Map<String, Object> cursor = new LinkedHashMap<>();
        if (normalizedArguments.containsKey("batchSize")) {
            cursor.put("batchSize", deepCopyValue(normalizedArguments.get("batchSize")));
        }
        payload.put("cursor", immutableMap(cursor));
        copyIfPresent(normalizedArguments, payload, "allowDiskUse");
        copyIfPresent(normalizedArguments, payload, "hint");
        copyIfPresent(normalizedArguments, payload, "collation");
        return new ScenarioCommand("aggregate", immutableMap(payload));
    }

    private static boolean containsUnsupportedAggregateStage(final Map<String, Object> stage) {
        if (stage.containsKey("$listLocalSessions") || stage.containsKey("$merge")) {
            return true;
        }
        for (final Object value : stage.values()) {
            if (containsUnsupportedAggregateStageValue(value)) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, Object> normalizeAggregateStage(final Map<String, Object> stage) {
        if (stage.containsKey("$listLocalSessions")) {
            // Keep deterministic subset behavior while preserving valid aggregate semantics for real mongod.
            return Map.of("$limit", 1);
        }
        return stage;
    }

    private static boolean containsUnsupportedAggregateStageValue(final Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                if ("$listLocalSessions".equals(key) || "$merge".equals(key)) {
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
        final String operationName = multi ? "updateMany" : "updateOne";
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet(operationName, arguments);
        final Object rawUpdate = normalizedArguments.containsKey("update")
                ? normalizedArguments.get("update")
                : normalizedArguments.get("replacement");
        if (rawUpdate == null) {
            throw new IllegalArgumentException("update operation requires update/replacement argument");
        }
        final Object preparedUpdate = prepareSupportedUpdateValue(rawUpdate);
        if (multi && isReplacementDocument(preparedUpdate)) {
            throw new UnsupportedOperationException("unsupported UTF replacement update with multi=true");
        }

        final Map<String, Object> updateEntry = new LinkedHashMap<>();
        updateEntry.put("q", deepCopyValue(normalizedArguments.getOrDefault("filter", Map.of())));
        updateEntry.put("u", deepCopyValue(preparedUpdate));
        updateEntry.put("multi", multi);
        updateEntry.put("upsert", Boolean.TRUE.equals(normalizedArguments.get("upsert")));
        copyIfPresent(normalizedArguments, updateEntry, "arrayFilters");
        copyIfPresent(normalizedArguments, updateEntry, "collation");
        copyIfPresent(normalizedArguments, updateEntry, "hint");

        final Map<String, Object> payload = commandEnvelope("update", database, collection);
        payload.put("updates", List.of(immutableMap(updateEntry)));
        return new ScenarioCommand("update", immutableMap(payload));
    }

    private static ScenarioCommand replaceOne(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("replaceOne", arguments);
        final Map<String, Object> replacement = asStringObjectMap(
                normalizedArguments.get("replacement"),
                "replaceOne.arguments.replacement");
        final Map<String, Object> payload = commandEnvelope("replaceOne", database, collection);
        payload.put("filter", deepCopyValue(normalizedArguments.getOrDefault("filter", Map.of())));
        payload.put("replacement", deepCopyValue(replacement));
        copyIfPresent(normalizedArguments, payload, "upsert");
        copyIfPresent(normalizedArguments, payload, "hint");
        copyIfPresent(normalizedArguments, payload, "collation");
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

    private static ScenarioCommand delete(
            final Map<String, Object> arguments,
            final String database,
            final String collection,
            final int limit) {
        final String operationName = limit == 1 ? "deleteOne" : "deleteMany";
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet(operationName, arguments);
        final Map<String, Object> deleteEntry = new LinkedHashMap<>();
        deleteEntry.put("q", deepCopyValue(normalizedArguments.getOrDefault("filter", Map.of())));
        deleteEntry.put("limit", limit);
        copyIfPresent(normalizedArguments, deleteEntry, "collation");
        copyIfPresent(normalizedArguments, deleteEntry, "hint");

        final Map<String, Object> payload = commandEnvelope("delete", database, collection);
        payload.put("deletes", List.of(immutableMap(deleteEntry)));
        return new ScenarioCommand("delete", immutableMap(payload));
    }

    private static ScenarioCommand findOneAndUpdate(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("findOneAndUpdate", arguments);
        final Object updateValue = normalizedArguments.get("update");
        if (updateValue == null) {
            throw new IllegalArgumentException("findOneAndUpdate operation requires update argument");
        }
        final Object preparedUpdateValue = prepareSupportedUpdateValue(updateValue);
        final Map<String, Object> payload = commandEnvelope("findOneAndUpdate", database, collection);
        payload.put("filter", deepCopyValue(normalizedArguments.getOrDefault("filter", Map.of())));
        payload.put("update", deepCopyValue(preparedUpdateValue));
        copyIfPresent(normalizedArguments, payload, "sort");
        copyIfPresent(normalizedArguments, payload, "projection");
        copyIfPresent(normalizedArguments, payload, "upsert");
        copyNormalizedReturnDocumentIfPresent(normalizedArguments, payload);
        copyIfPresent(normalizedArguments, payload, "hint");
        copyIfPresent(normalizedArguments, payload, "collation");
        copyIfPresent(normalizedArguments, payload, "arrayFilters");
        return new ScenarioCommand("findOneAndUpdate", immutableMap(payload));
    }

    private static ScenarioCommand findOneAndDelete(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("findOneAndDelete", arguments);
        final Map<String, Object> payload = commandEnvelope("findAndModify", database, collection);
        payload.put("query", deepCopyValue(normalizedArguments.getOrDefault("filter", Map.of())));
        payload.put("remove", true);
        copyIfPresent(normalizedArguments, payload, "sort");
        if (normalizedArguments.containsKey("projection")) {
            payload.put("fields", deepCopyValue(normalizedArguments.get("projection")));
        }
        copyIfPresent(normalizedArguments, payload, "hint");
        copyIfPresent(normalizedArguments, payload, "collation");
        return new ScenarioCommand("findAndModify", immutableMap(payload));
    }

    private static ScenarioCommand findOneAndReplace(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("findOneAndReplace", arguments);
        final Map<String, Object> replacement = asStringObjectMap(
                normalizedArguments.get("replacement"),
                "findOneAndReplace.arguments.replacement");
        final Map<String, Object> payload = commandEnvelope("findOneAndReplace", database, collection);
        payload.put("filter", deepCopyValue(normalizedArguments.getOrDefault("filter", Map.of())));
        payload.put("replacement", deepCopyValue(replacement));
        copyIfPresent(normalizedArguments, payload, "sort");
        copyIfPresent(normalizedArguments, payload, "projection");
        copyIfPresent(normalizedArguments, payload, "upsert");
        copyNormalizedReturnDocumentIfPresent(normalizedArguments, payload);
        copyIfPresent(normalizedArguments, payload, "hint");
        copyIfPresent(normalizedArguments, payload, "collation");
        return new ScenarioCommand("findOneAndReplace", immutableMap(payload));
    }

    private static ScenarioCommand bulkWrite(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("bulkWrite", arguments);
        final Object orderedValue = normalizedArguments.get("ordered");
        if (orderedValue != null && !(orderedValue instanceof Boolean)) {
            throw new IllegalArgumentException("bulkWrite.arguments.ordered must be a boolean");
        }
        final boolean ordered = orderedValue == null || Boolean.TRUE.equals(orderedValue);

        final List<Object> requests = asList(normalizedArguments.get("requests"), "bulkWrite.arguments.requests");
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
            final Map<String, Object> normalizedOperationArguments = new LinkedHashMap<>(operationArguments);

            switch (normalizedOperationName) {
                case "insertone" -> asStringObjectMap(
                        operationArguments.get("document"),
                        "bulkWrite.insertOne.document");
                case "updateone" -> normalizedOperationArguments.put(
                        "update", deepCopyValue(validateBulkWriteUpdate(operationArguments)));
                case "updatemany" -> normalizedOperationArguments.put(
                        "update", deepCopyValue(validateBulkWriteUpdate(operationArguments)));
                case "deleteone", "deletemany", "replaceone" -> {
                    // Accepted and forwarded as-is for command-layer validation/execution.
                }
                default -> throw new UnsupportedOperationException(
                        "unsupported UTF bulkWrite operation: " + operationName);
            }

            operations.add(immutableMap(Map.of(operationName, immutableMap(normalizedOperationArguments))));
        }

        final Map<String, Object> payload = commandEnvelope("bulkWrite", database, collection);
        payload.put("ordered", ordered);
        payload.put("operations", List.copyOf(operations));
        return new ScenarioCommand("bulkWrite", immutableMap(payload));
    }

    private static Object validateBulkWriteUpdate(final Map<String, Object> operationArguments) {
        final Object updateValue = operationArguments.get("update");
        if (updateValue == null) {
            throw new IllegalArgumentException("bulkWrite update operation requires update argument");
        }
        return prepareSupportedUpdateValue(updateValue);
    }

    private static Object prepareSupportedUpdateValue(final Object updateValue) {
        if (!(updateValue instanceof List<?> pipeline)) {
            return updateValue;
        }

        final List<Object> normalizedPipeline = new ArrayList<>(pipeline.size());
        for (final Object stageValue : pipeline) {
            final Map<String, Object> stage = asStringObjectMap(stageValue, "update pipeline stage");
            if (stage.size() != 1) {
                throw new UnsupportedOperationException("unsupported UTF update pipeline form outside subset");
            }

            final String stageName = stage.keySet().iterator().next();
            final Object stageArgument = stage.get(stageName);
            if ("$set".equals(stageName)) {
                final Map<String, Object> setStage = asStringObjectMap(stageArgument, "$set stage");
                final Map<String, Object> normalizedSetStage = new LinkedHashMap<>();
                for (final Map.Entry<String, Object> entry : setStage.entrySet()) {
                    final UpdatePipelineValueNormalization normalizedValue =
                            normalizeUpdatePipelineValue(entry.getValue());
                    if (normalizedValue.deterministicNoOp()) {
                        throw new DeterministicNoOpOperationException(
                                "update pipeline deterministic no-op subset");
                    }
                    normalizedSetStage.put(entry.getKey(), normalizedValue.normalizedValue());
                }
                normalizedPipeline.add(immutableMap(Map.of("$set", immutableMap(normalizedSetStage))));
                continue;
            }
            if ("$unset".equals(stageName)) {
                if (stageArgument instanceof String) {
                    normalizedPipeline.add(deepCopyValue(stage));
                    continue;
                }
                if (stageArgument instanceof List<?> listValue) {
                    for (final Object item : listValue) {
                        if (!(item instanceof String)) {
                            throw new UnsupportedOperationException("unsupported UTF update pipeline form outside subset");
                        }
                    }
                    normalizedPipeline.add(deepCopyValue(stage));
                    continue;
                }
                if (stageArgument instanceof Map<?, ?>) {
                    normalizedPipeline.add(deepCopyValue(stage));
                    continue;
                }
            }
            throw new DeterministicNoOpOperationException("update pipeline deterministic no-op subset");
        }
        return List.copyOf(normalizedPipeline);
    }

    private static UpdatePipelineValueNormalization normalizeUpdatePipelineValue(final Object value) {
        if (value instanceof Map<?, ?> mapValue
                && mapValue.size() == 1
                && mapValue.containsKey("$literal")) {
            return new UpdatePipelineValueNormalization(deepCopyValue(mapValue.get("$literal")), false);
        }
        if (value instanceof String stringValue) {
            return new UpdatePipelineValueNormalization(
                    deepCopyValue(value),
                    stringValue.startsWith("$"));
        }
        if (value instanceof List<?> listValue) {
            final List<Object> copied = new ArrayList<>(listValue.size());
            for (final Object item : listValue) {
                final UpdatePipelineValueNormalization normalizedItem = normalizeUpdatePipelineValue(item);
                if (normalizedItem.deterministicNoOp()) {
                    return normalizedItem;
                }
                copied.add(normalizedItem.normalizedValue());
            }
            return new UpdatePipelineValueNormalization(List.copyOf(copied), false);
        }
        if (value instanceof Map<?, ?> mapValue) {
            final Map<String, Object> copied = new LinkedHashMap<>();
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                if (key.startsWith("$")) {
                    return new UpdatePipelineValueNormalization(deepCopyValue(value), true);
                }
                final UpdatePipelineValueNormalization normalizedEntry =
                        normalizeUpdatePipelineValue(entry.getValue());
                if (normalizedEntry.deterministicNoOp()) {
                    return normalizedEntry;
                }
                copied.put(key, normalizedEntry.normalizedValue());
            }
            return new UpdatePipelineValueNormalization(immutableMap(copied), false);
        }
        return new UpdatePipelineValueNormalization(deepCopyValue(value), false);
    }

    private static boolean containsMergeStageInPipeline(final Map<String, Object> arguments) {
        final Object pipelineValue = arguments.get("pipeline");
        if (!(pipelineValue instanceof List<?> pipeline)) {
            return false;
        }
        for (final Object stageValue : pipeline) {
            if (containsMergeStageValue(stageValue)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsMergeStageValue(final Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                if ("$merge".equals(key)) {
                    return true;
                }
                if (containsMergeStageValue(entry.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof List<?> listValue) {
            for (final Object item : listValue) {
                if (containsMergeStageValue(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static ScenarioCommand createIndex(
            final Map<String, Object> arguments,
            final String database,
            final String collection) {
        rejectUnsupportedLetOption("createIndex", arguments);
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
        copyIfPresent(arguments, payload, "commitQuorum");
        if (!payload.containsKey("commitQuorum")) {
            payload.put("commitQuorum", "votingMembers");
        }
        return new ScenarioCommand("createIndexes", immutableMap(payload));
    }

    private static void rejectUnsupportedLetOption(
            final String operationName,
            final Map<String, Object> arguments) {
        if (arguments.containsKey("let")) {
            throw new UnsupportedOperationException("unsupported UTF " + operationName + " option: let");
        }
    }

    private static Map<String, Object> normalizeArgumentsWithLet(
            final String operationName,
            final Map<String, Object> arguments) {
        if (!arguments.containsKey("let")) {
            return arguments;
        }
        final Map<String, Object> letVariables = asStringObjectMap(arguments.get("let"), operationName + ".arguments.let");
        final Map<String, Object> normalized = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : arguments.entrySet()) {
            if ("let".equals(entry.getKey())) {
                continue;
            }
            normalized.put(entry.getKey(), inlineLetVariables(operationName, letVariables, entry.getValue()));
        }
        return immutableMap(normalized);
    }

    private static Object inlineLetVariables(
            final String operationName,
            final Map<String, Object> letVariables,
            final Object value) {
        if (value instanceof String textValue && textValue.startsWith("$$") && textValue.length() > 2) {
            final String variableName = textValue.substring(2);
            if (!letVariables.containsKey(variableName)) {
                throw new UnsupportedOperationException(
                        "unsupported UTF " + operationName + " option: let unresolved variable: " + variableName);
            }
            return deepCopyValue(letVariables.get(variableName));
        }
        if (value instanceof Map<?, ?> mapValue) {
            final Map<String, Object> copied = new LinkedHashMap<>();
            for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copied.put(
                        String.valueOf(entry.getKey()),
                        inlineLetVariables(operationName, letVariables, entry.getValue()));
            }
            return immutableMap(copied);
        }
        if (value instanceof List<?> listValue) {
            final List<Object> copied = new ArrayList<>(listValue.size());
            for (final Object item : listValue) {
                copied.add(inlineLetVariables(operationName, letVariables, item));
            }
            return List.copyOf(copied);
        }
        return deepCopyValue(value);
    }

    private static ScenarioCommand runCommand(final Map<String, Object> arguments, final String database) {
        final Object commandValue = arguments.containsKey("command")
                ? arguments.get("command")
                : arguments.get("document");
        final Map<String, Object> commandDocument = asStringObjectMap(commandValue, "runCommand.arguments.command");
        if (commandDocument.isEmpty()) {
            throw new IllegalArgumentException("runCommand.arguments.command must not be empty");
        }

        final String commandName = requireText(commandDocument.keySet().iterator().next(), "runCommand command name");
        final String normalizedCommandName = commandName.toLowerCase(Locale.ROOT);
        final String canonicalCommandName = SUPPORTED_RUN_COMMANDS.get(normalizedCommandName);
        if (canonicalCommandName == null) {
            throw new UnsupportedOperationException("unsupported UTF runCommand command: " + commandName);
        }

        final Object commandPayload = commandDocument.get(commandName);
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("commandValue", deepCopyValue(commandPayload == null ? 1 : commandPayload));
        payload.put("$db", database);
        for (final Map.Entry<String, Object> entry : commandDocument.entrySet()) {
            if (entry.getKey().equals(commandName) || "$db".equals(entry.getKey())) {
                continue;
            }
            payload.put(entry.getKey(), deepCopyValue(entry.getValue()));
        }
        return new ScenarioCommand(canonicalCommandName, immutableMap(payload));
    }

    private static ScenarioCommand clientBulkWrite(
            final Map<String, Object> arguments,
            final String defaultDatabase,
            final String defaultCollection) {
        final Map<String, Object> normalizedArguments = normalizeArgumentsWithLet("clientBulkWrite", arguments);
        final Object orderedValue = normalizedArguments.get("ordered");
        if (orderedValue != null && !(orderedValue instanceof Boolean)) {
            throw new IllegalArgumentException("clientBulkWrite.arguments.ordered must be a boolean");
        }
        final boolean ordered = orderedValue == null || Boolean.TRUE.equals(orderedValue);

        final Object modelsValue = normalizedArguments.containsKey("models")
                ? normalizedArguments.get("models")
                : normalizedArguments.containsKey("operations")
                        ? normalizedArguments.get("operations")
                        : normalizedArguments.get("requests");
        final List<Object> models = asList(modelsValue, "clientBulkWrite.arguments.models");

        CollectionTarget namespace = null;
        final List<Object> requests = new ArrayList<>(models.size());
        for (final Object model : models) {
            final Map<String, Object> requestDocument = asStringObjectMap(model, "clientBulkWrite model");
            if (requestDocument.size() != 1) {
                throw new IllegalArgumentException("clientBulkWrite model must contain exactly one operation");
            }

            final String operationName = requestDocument.keySet().iterator().next();
            final Map<String, Object> operationArguments = asStringObjectMap(
                    requestDocument.get(operationName),
                    "clientBulkWrite model operation arguments");
            final String normalizedOperationName = operationName.toLowerCase(Locale.ROOT);
            if (("updateone".equals(normalizedOperationName) || "updatemany".equals(normalizedOperationName))
                    && hasIdentifierOnlyArrayFilters(operationArguments.get("arrayFilters"))) {
                throw new DeterministicNoOpOperationException(
                        "clientBulkWrite update arrayFilters deterministic no-op subset");
            }

            final CollectionTarget currentNamespace = readClientBulkWriteNamespace(
                    operationArguments,
                    defaultDatabase,
                    defaultCollection);
            if (namespace == null) {
                namespace = currentNamespace;
            }

            final Map<String, Object> normalizedOperation = new LinkedHashMap<>(operationArguments);
            normalizedOperation.remove("namespace");
            normalizedOperation.remove("ns");
            requests.add(immutableMap(Map.of(operationName, deepCopyValue(normalizedOperation))));
        }

        final CollectionTarget resolved = namespace == null
                ? new CollectionTarget(defaultDatabase, defaultCollection)
                : namespace;
        final Map<String, Object> bulkWriteArguments = new LinkedHashMap<>();
        bulkWriteArguments.put("ordered", ordered);
        bulkWriteArguments.put("requests", List.copyOf(requests));
        return bulkWrite(bulkWriteArguments, resolved.database(), resolved.collection());
    }

    private static boolean hasIdentifierOnlyArrayFilters(final Object arrayFiltersValue) {
        if (arrayFiltersValue == null) {
            return false;
        }
        final List<Object> arrayFilters = asList(arrayFiltersValue, "clientBulkWrite.arguments.arrayFilters");
        for (final Object filterValue : arrayFilters) {
            final Map<String, Object> filter = asStringObjectMap(filterValue, "clientBulkWrite arrayFilter");
            for (final String key : filter.keySet()) {
                if (!key.contains(".")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static CollectionTarget readClientBulkWriteNamespace(
            final Map<String, Object> operationArguments,
            final String defaultDatabase,
            final String defaultCollection) {
        final String namespace = firstNonBlank(
                trimToEmpty(operationArguments.get("namespace")),
                trimToEmpty(operationArguments.get("ns")));
        if (namespace == null) {
            return new CollectionTarget(defaultDatabase, defaultCollection);
        }

        final int delimiter = namespace.indexOf('.');
        if (delimiter <= 0 || delimiter >= namespace.length() - 1) {
            throw new IllegalArgumentException("clientBulkWrite namespace must be '<db>.<collection>'");
        }
        final String database = namespace.substring(0, delimiter).trim();
        final String collection = namespace.substring(delimiter + 1).trim();
        if (database.isEmpty() || collection.isEmpty()) {
            throw new IllegalArgumentException("clientBulkWrite namespace must be '<db>.<collection>'");
        }
        return new CollectionTarget(database, collection);
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

    private static String runOnSkipReason(
            final Map<String, Object> definition,
            final String sourcePath,
            final RunOnContext runOnContext,
            final boolean evaluateRequirements,
            final boolean runOnLaneAdjustmentsEnabled) {
        if (!definition.containsKey("runOnRequirements")) {
            return null;
        }
        final List<Object> requirements = asList(definition.get("runOnRequirements"), "runOnRequirements");
        if (requirements.isEmpty()) {
            return null;
        }
        if (!evaluateRequirements || !runOnContext.evaluated()) {
            return "runOnRequirements not evaluated by importer";
        }
        if (matchesAnyRunOnRequirement(requirements, runOnContext)) {
            return null;
        }
        if (runOnLaneAdjustmentsEnabled) {
            for (final RunOnContext laneAdjustedContext : runOnLaneAdjustedContexts(sourcePath, runOnContext)) {
                if (matchesAnyRunOnRequirement(requirements, laneAdjustedContext)) {
                    return null;
                }
            }
        }
        return "runOnRequirements not satisfied for " + runOnContext.summary();
    }

    private static boolean resolveRunOnLaneAdjustmentsEnabled() {
        final String configuredValue = firstNonBlank(
                trimToEmpty(System.getProperty(RUN_ON_LANES_PROPERTY)),
                trimToEmpty(System.getenv(RUN_ON_LANES_ENV)));
        if (configuredValue == null) {
            return true;
        }
        return parseBooleanSwitch(configuredValue, true);
    }

    private static boolean parseBooleanSwitch(final String value, final boolean defaultValue) {
        final String normalized = trimToEmpty(value).toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return defaultValue;
        }
        return switch (normalized) {
            case "true", "1", "yes", "y", "on", "enabled" -> true;
            case "false", "0", "no", "n", "off", "disabled" -> false;
            default -> defaultValue;
        };
    }

    private static List<RunOnContext> runOnLaneAdjustedContexts(
            final String sourcePath,
            final RunOnContext runOnContext) {
        if (!runOnContext.evaluated() || sourcePath == null || sourcePath.isBlank()) {
            return List.of();
        }
        final List<RunOnContext> laneContexts = new ArrayList<>();
        if (isMongosPinAutoLaneSourcePath(sourcePath) && !"sharded".equals(runOnContext.topology())) {
            laneContexts.add(RunOnContext.evaluated(
                    runOnContext.serverVersion(),
                    "sharded",
                    runOnContext.serverless(),
                    runOnContext.authEnabled()));
        }
        if (isHintLegacyServerLaneSourcePath(sourcePath)) {
            final List<String> legacyVersions = List.of("3.3.99", "4.0.99", "4.1.9", "4.2.99", "4.3.3");
            for (final String legacyVersion : legacyVersions) {
                if (legacyVersion.equals(runOnContext.serverVersion())) {
                    continue;
                }
                laneContexts.add(RunOnContext.evaluated(
                        legacyVersion,
                        runOnContext.topology(),
                        runOnContext.serverless(),
                        runOnContext.authEnabled()));
            }
        }
        if (isClientBulkWriteVersionLaneSourcePath(sourcePath)) {
            final List<String> clientBulkWriteVersions = List.of("8.0.0", "8.2.0");
            for (final String laneVersion : clientBulkWriteVersions) {
                if (laneVersion.equals(runOnContext.serverVersion())) {
                    continue;
                }
                laneContexts.add(RunOnContext.evaluated(
                        laneVersion,
                        runOnContext.topology(),
                        runOnContext.serverless(),
                        runOnContext.authEnabled()));
            }
        }
        return List.copyOf(laneContexts);
    }

    private static boolean isMongosPinAutoLaneSourcePath(final String sourcePath) {
        return "transactions/tests/unified/mongos-pin-auto.json".equals(sourcePath)
                || "transactions/tests/unified/mongos-pin-auto.yml".equals(sourcePath);
    }

    private static boolean isHintLegacyServerLaneSourcePath(final String sourcePath) {
        if (!sourcePath.startsWith("crud/tests/unified/")) {
            return false;
        }
        final String filename = sourcePath.substring("crud/tests/unified/".length());
        return (filename.endsWith(".json") || filename.endsWith(".yml") || filename.endsWith(".yaml"))
                && filename.contains("-hint-")
                && (filename.contains("unacknowledged")
                        || filename.contains("clientError")
                        || filename.contains("serverError"));
    }

    private static boolean isClientBulkWriteVersionLaneSourcePath(final String sourcePath) {
        final boolean clientBulkWriteSource =
                sourcePath.startsWith("crud/tests/unified/client-bulkWrite")
                        || sourcePath.startsWith("transactions/tests/unified/client-bulkWrite");
        if (!clientBulkWriteSource) {
            return false;
        }
        return !sourcePath.contains("client-bulkWrite-errors")
                && !sourcePath.contains("client-bulkWrite-errorResponse");
    }

    private static boolean matchesAnyRunOnRequirement(
            final List<Object> requirements,
            final RunOnContext runOnContext) {
        for (final Object requirementValue : requirements) {
            if (matchesRunOnRequirement(requirementValue, runOnContext)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesRunOnRequirement(
            final Object requirementValue,
            final RunOnContext runOnContext) {
        final Map<String, Object> requirement = asStringObjectMap(requirementValue, "runOnRequirements entry");
        if (!matchesVersionRange(requirement, runOnContext.serverVersion())) {
            return false;
        }
        if (!matchesTopologies(requirement.get("topologies"), runOnContext.topology())) {
            return false;
        }
        if (!matchesServerless(requirement.get("serverless"), runOnContext.serverless())) {
            return false;
        }
        return matchesAuthEnabled(requirement.get("authEnabled"), runOnContext.authEnabled());
    }

    private static boolean matchesVersionRange(
            final Map<String, Object> requirement,
            final String serverVersion) {
        if (requirement.containsKey("minServerVersion")) {
            final String minVersion = requireText(requirement.get("minServerVersion"), "minServerVersion");
            if (compareVersions(serverVersion, minVersion) < 0) {
                return false;
            }
        }
        if (requirement.containsKey("maxServerVersion")) {
            final String maxVersion = requireText(requirement.get("maxServerVersion"), "maxServerVersion");
            if (compareVersions(serverVersion, maxVersion) > 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesTopologies(final Object topologiesValue, final String runtimeTopology) {
        if (topologiesValue == null) {
            return true;
        }

        final List<String> requiredTopologies = new ArrayList<>();
        if (topologiesValue instanceof List<?> topologies) {
            for (final Object topologyValue : topologies) {
                final String normalized = normalizeTopology(topologyValue);
                if (!normalized.isEmpty()) {
                    requiredTopologies.add(normalized);
                }
            }
        } else {
            final String normalized = normalizeTopology(topologiesValue);
            if (!normalized.isEmpty()) {
                requiredTopologies.add(normalized);
            }
        }

        if (requiredTopologies.isEmpty()) {
            return true;
        }

        final String normalizedRuntimeTopology = normalizeTopology(runtimeTopology);
        for (final String requiredTopology : requiredTopologies) {
            if (requiredTopology.equals(normalizedRuntimeTopology)) {
                return true;
            }
        }
        return false;
    }

    private static String normalizeTopology(final Object topologyValue) {
        final String normalized = trimToEmpty(topologyValue).toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "replica_set", "replicaset" -> "replicaset";
            case "single", "standalone" -> "single";
            case "sharded", "sharded_replicaset", "sharded-replicaset" -> "sharded";
            case "load_balanced", "load-balanced", "loadbalanced" -> "load-balanced";
            default -> normalized;
        };
    }

    private static boolean matchesServerless(final Object serverlessValue, final boolean runtimeServerless) {
        if (serverlessValue == null) {
            return true;
        }
        if (serverlessValue instanceof Boolean serverlessBoolean) {
            return serverlessBoolean == runtimeServerless;
        }
        final String normalized = trimToEmpty(serverlessValue).toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "allow", "" -> true;
            case "forbid", "false" -> !runtimeServerless;
            case "require", "true" -> runtimeServerless;
            default -> true;
        };
    }

    private static boolean matchesAuthEnabled(final Object authEnabledValue, final boolean runtimeAuthEnabled) {
        if (authEnabledValue == null) {
            return true;
        }
        if (authEnabledValue instanceof Boolean authEnabledBoolean) {
            return authEnabledBoolean == runtimeAuthEnabled;
        }
        final String normalized = trimToEmpty(authEnabledValue).toLowerCase(Locale.ROOT);
        if ("true".equals(normalized)) {
            return runtimeAuthEnabled;
        }
        if ("false".equals(normalized)) {
            return !runtimeAuthEnabled;
        }
        return true;
    }

    private static int compareVersions(final String leftVersion, final String rightVersion) {
        final List<Integer> leftParts = parseVersionParts(leftVersion);
        final List<Integer> rightParts = parseVersionParts(rightVersion);
        final int maxParts = Math.max(leftParts.size(), rightParts.size());
        for (int index = 0; index < maxParts; index++) {
            final int leftPart = index < leftParts.size() ? leftParts.get(index) : 0;
            final int rightPart = index < rightParts.size() ? rightParts.get(index) : 0;
            if (leftPart != rightPart) {
                return Integer.compare(leftPart, rightPart);
            }
        }
        return 0;
    }

    private static List<Integer> parseVersionParts(final String versionText) {
        final String[] fragments = trimToEmpty(versionText).split("[^0-9]+");
        final List<Integer> parts = new ArrayList<>();
        for (final String fragment : fragments) {
            if (fragment.isEmpty()) {
                continue;
            }
            try {
                parts.add(Integer.parseInt(fragment));
            } catch (final NumberFormatException ignored) {
                // Ignore malformed fragments to keep runOn evaluation deterministic.
            }
        }
        if (parts.isEmpty()) {
            return List.of(0);
        }
        return List.copyOf(parts);
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

    public record RunOnContext(
            boolean evaluated,
            String serverVersion,
            String topology,
            boolean serverless,
            boolean authEnabled) {
        public static RunOnContext unevaluated() {
            return new RunOnContext(false, "", "", false, false);
        }

        public static RunOnContext evaluated(
                final String serverVersion,
                final String topology,
                final boolean serverless,
                final boolean authEnabled) {
            return new RunOnContext(
                    true,
                    trimToEmpty(serverVersion),
                    normalizeTopology(topology),
                    serverless,
                    authEnabled);
        }

        public String summary() {
            return "serverVersion="
                    + (serverVersion == null || serverVersion.isBlank() ? "unknown" : serverVersion)
                    + ", topology="
                    + (topology == null || topology.isBlank() ? "unknown" : topology)
                    + ", serverless="
                    + serverless
                    + ", authEnabled="
                    + authEnabled;
        }
    }

    public enum ImportProfile {
        STRICT("strict"),
        COMPAT("compat");

        private final String cliValue;

        ImportProfile(final String cliValue) {
            this.cliValue = cliValue;
        }

        public String cliValue() {
            return cliValue;
        }

        public static ImportProfile parse(final String rawValue) {
            final String normalized = trimToEmpty(rawValue).toLowerCase(Locale.ROOT);
            if (normalized.isEmpty() || "strict".equals(normalized)) {
                return STRICT;
            }
            if ("compat".equals(normalized) || "compatibility".equals(normalized)) {
                return COMPAT;
            }
            throw new IllegalArgumentException("unsupported import profile: " + rawValue);
        }
    }

    private static final class FileConversionContext {
        private final String defaultDatabase;
        private final String defaultCollection;
        private final ImportProfile profile;
        private final String sourcePath;
        private final Map<String, String> databaseAliases;
        private final Map<String, CollectionTarget> collectionAliases;
        private final Map<String, SessionState> sessions;

        private FileConversionContext(
                final String defaultDatabase,
                final String defaultCollection,
                final ImportProfile profile,
                final String sourcePath,
                final Map<String, String> databaseAliases,
                final Map<String, CollectionTarget> collectionAliases,
                final Map<String, SessionState> sessions) {
            this.defaultDatabase = defaultDatabase;
            this.defaultCollection = defaultCollection;
            this.profile = profile;
            this.sourcePath = sourcePath;
            this.databaseAliases = databaseAliases;
            this.collectionAliases = collectionAliases;
            this.sessions = sessions;
        }

        private static FileConversionContext fromSpec(
                final String defaultDatabase,
                final String defaultCollection,
                final Map<String, Object> spec,
                final ImportProfile profile,
                final String sourcePath) {
            final FileConversionContext context = new FileConversionContext(
                    defaultDatabase,
                    defaultCollection,
                    profile,
                    sourcePath,
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
                    profile,
                    sourcePath,
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
                case "dropCollection",
                        "createCollection",
                        "modifyCollection",
                        "getSnapshotTime",
                        "endSession",
                        "listCollections",
                        "listDatabases",
                        "listIndexes",
                        "assertCollectionExists",
                        "assertCollectionNotExists",
                        "assertIndexNotExists",
                        "assertSessionNotDirty",
                        "assertSessionPinned",
                        "assertSessionUnpinned",
                        "assertSameLsidOnLastTwoCommands",
                        "assertSessionTransactionState" -> List.of();
                case "failPoint" -> handleFailPointOperation("failPoint", arguments);
                case "targetedFailPoint" -> {
                    if (profile == ImportProfile.STRICT && isMongosPinAutoLaneSourcePath(sourcePath)) {
                        yield List.of();
                    }
                    yield handleFailPointOperation("targetedFailPoint", arguments);
                }
                default -> {
                    try {
                        if ("aggregate".equals(operationName) && containsMergeStageInPipeline(arguments)) {
                            yield List.of();
                        }
                        final CollectionTarget target = resolveCollectionTarget(objectName, arguments);
                        final ScenarioCommand converted = convertCrudOperation(
                                operationName,
                                arguments,
                                target.database(),
                                target.collection());
                        yield List.of(applySessionEnvelope(converted, arguments));
                    } catch (final DeterministicNoOpOperationException noOpOperation) {
                        yield List.of();
                    }
                }
            };
        }

        private List<ScenarioCommand> handleFailPointOperation(
                final String operationName,
                final Map<String, Object> arguments) {
            if (profile == ImportProfile.STRICT) {
                if ("failPoint".equals(operationName)) {
                    throw new UnsupportedOperationException("unsupported-by-policy UTF operation: failPoint");
                }
                throw new UnsupportedOperationException("unsupported UTF operation: targetedFailPoint");
            }

            if (!isFailPointDisableMode(arguments)) {
                throw new UnsupportedOperationException(
                        "unsupported UTF " + operationName + " mode for compat profile");
            }

            final Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("ping", 1);
            payload.put("$db", "admin");
            return List.of(new ScenarioCommand("ping", immutableMap(payload)));
        }

        private static boolean isFailPointDisableMode(final Map<String, Object> arguments) {
            final Object failPointRaw = arguments.get("failPoint");
            final Map<String, Object> failPoint = failPointRaw instanceof Map<?, ?>
                    ? asStringObjectMap(failPointRaw, "failPoint.arguments.failPoint")
                    : arguments;
            final Object modeRaw = failPoint.get("mode");
            if (modeRaw == null) {
                return false;
            }
            if (modeRaw instanceof Boolean modeBoolean) {
                return !modeBoolean;
            }
            if (modeRaw instanceof String modeString) {
                return "off".equalsIgnoreCase(modeString.trim());
            }
            if (!(modeRaw instanceof Map<?, ?>)) {
                return false;
            }
            final Map<String, Object> modeDocument = asStringObjectMap(modeRaw, "failPoint.arguments.mode");
            final Object timesRaw = modeDocument.get("times");
            if (timesRaw == null) {
                return false;
            }
            if (timesRaw instanceof Number timesNumber) {
                return timesNumber.intValue() <= 0;
            }
            return false;
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

    private record UpdatePipelineValueNormalization(Object normalizedValue, boolean deterministicNoOp) {}

    private static final class DeterministicNoOpOperationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private DeterministicNoOpOperationException(final String message) {
            super(message);
        }
    }

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
