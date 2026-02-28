package org.jongodb.testkit.springsuite;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jongodb.testkit.Scenario;
import org.jongodb.testkit.ScenarioCommand;
import org.jongodb.testkit.ScenarioOutcome;
import org.jongodb.testkit.WireCommandIngressBackend;

/**
 * Runs representative Spring Data Mongo scenarios against jongodb endpoint abstraction.
 *
 * <p>The first major slice models Spring profile/version matrix dimensions in config and executes
 * one deterministic runtime implementation ({@link WireCommandIngressBackend}) for all targets.
 */
public final class SpringCompatibilityMatrixRunner {
    static final Path DEFAULT_OUTPUT_DIR = Path.of("build/reports/spring-matrix");
    private static final String DEFAULT_ENDPOINT_ID = "jongodb-wire-command-ingress";
    private static final String MATRIX_JSON = "spring-compatibility-matrix.json";
    private static final String MATRIX_MARKDOWN = "spring-compatibility-matrix.md";
    private static final int DEFAULT_FAILURE_SAMPLE_LIMIT = 5;
    private static final int FAILURE_MESSAGE_LIMIT = 180;

    private final Clock clock;
    private final EndpointFactory endpointFactory;
    private final List<SpringProfileTarget> targetCatalog;
    private final List<SpringScenario> scenarioCatalog;

    public SpringCompatibilityMatrixRunner() {
        this(
            Clock.systemUTC(),
            target -> new WireIngressEndpoint("spring-suite-" + target.id()),
            defaultTargets(),
            defaultScenarios()
        );
    }

    SpringCompatibilityMatrixRunner(
        Clock clock,
        EndpointFactory endpointFactory,
        List<SpringProfileTarget> targetCatalog,
        List<SpringScenario> scenarioCatalog
    ) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.endpointFactory = Objects.requireNonNull(endpointFactory, "endpointFactory");
        this.targetCatalog = copyTargets(targetCatalog);
        this.scenarioCatalog = copyScenarios(scenarioCatalog);
    }

    public static void main(String[] args) throws Exception {
        if (containsHelpFlag(args)) {
            printUsage();
            return;
        }

        final EvidenceConfig config;
        try {
            config = EvidenceConfig.fromArgs(args);
        } catch (IllegalArgumentException exception) {
            System.err.println("Invalid argument: " + exception.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        SpringCompatibilityMatrixRunner runner = new SpringCompatibilityMatrixRunner();
        MatrixReport report = runner.runAndWrite(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());

        System.out.println("Spring Data Mongo compatibility matrix generated.");
        System.out.println("- generatedAt: " + report.generatedAt());
        System.out.println("- endpoint: " + report.endpointId());
        System.out.println("- targets: " + report.targets().size());
        System.out.println("- scenarios: " + report.scenarios().size());
        System.out.println("- totalCells: " + report.totalCells());
        System.out.println("- pass: " + report.passCount());
        System.out.println("- fail: " + report.failCount());
        System.out.println("- passRate: " + formatPercent(report.passRate()));
        System.out.println("- matrixJson: " + paths.matrixJson());
        System.out.println("- matrixMarkdown: " + paths.matrixMarkdown());

        if (config.failOnFailures() && report.failCount() > 0) {
            System.err.println("Spring compatibility matrix contains failures.");
            System.exit(2);
        }
    }

    public MatrixReport runAndWrite(EvidenceConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        MatrixReport report = run(config);
        ArtifactPaths paths = artifactPaths(config.outputDir());
        Files.createDirectories(config.outputDir());
        Files.writeString(paths.matrixJson(), renderJson(report), StandardCharsets.UTF_8);
        Files.writeString(paths.matrixMarkdown(), renderMarkdown(report), StandardCharsets.UTF_8);
        return report;
    }

    public MatrixReport run(EvidenceConfig config) {
        Objects.requireNonNull(config, "config");
        List<SpringProfileTarget> targets = resolveTargets(config.targetIds());

        List<MatrixCellResult> results = new ArrayList<>(targets.size() * scenarioCatalog.size());
        String endpointId = null;
        for (SpringProfileTarget target : targets) {
            JongodbEndpoint endpoint = Objects.requireNonNull(
                endpointFactory.create(target),
                "endpointFactory result"
            );
            String currentEndpointId = requireText(endpoint.id(), "endpoint.id");
            if (endpointId == null) {
                endpointId = currentEndpointId;
            } else if (!endpointId.equals(currentEndpointId)) {
                endpointId = "mixed-endpoints";
            }

            for (SpringScenario scenario : scenarioCatalog) {
                long startedAtNanos = System.nanoTime();
                MatrixCellStatus status = MatrixCellStatus.PASS;
                String errorMessage = null;

                try {
                    ScenarioOutcome outcome = endpoint.execute(scenario.scenario());
                    if (outcome == null) {
                        status = MatrixCellStatus.FAIL;
                        errorMessage = "endpoint returned null outcome";
                    } else if (!outcome.success()) {
                        status = MatrixCellStatus.FAIL;
                        errorMessage = minimizeFailure(outcome.errorMessage().orElse("unknown failure"));
                    }
                } catch (RuntimeException exception) {
                    status = MatrixCellStatus.FAIL;
                    errorMessage = minimizeFailure(
                        exception.getClass().getSimpleName() + ": " + exception.getMessage()
                    );
                }

                long durationMillis = Math.max(0L, (System.nanoTime() - startedAtNanos) / 1_000_000L);
                results.add(
                    new MatrixCellResult(
                        target.id(),
                        scenario.id(),
                        scenario.surface(),
                        status,
                        errorMessage,
                        durationMillis,
                        scenario.scenario().commands().size()
                    )
                );
            }
        }

        return new MatrixReport(
            Instant.now(clock),
            endpointId == null ? DEFAULT_ENDPOINT_ID : endpointId,
            targets,
            scenarioCatalog,
            results
        );
    }

    public static ArtifactPaths artifactPaths(Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir");
        Path normalized = outputDir.normalize();
        return new ArtifactPaths(
            normalized.resolve(MATRIX_JSON),
            normalized.resolve(MATRIX_MARKDOWN)
        );
    }

    String renderMarkdown(MatrixReport report) {
        Objects.requireNonNull(report, "report");
        StringBuilder sb = new StringBuilder();
        sb.append("# Spring Data Mongo Compatibility Matrix\n\n");
        sb.append("- generatedAt: ").append(report.generatedAt()).append('\n');
        sb.append("- endpoint: ").append(report.endpointId()).append('\n');
        sb.append("- targets: ").append(report.targets().size()).append('\n');
        sb.append("- scenarios: ").append(report.scenarios().size()).append('\n');
        sb.append("- totalCells: ").append(report.totalCells()).append('\n');
        sb.append("- pass: ").append(report.passCount()).append('\n');
        sb.append("- fail: ").append(report.failCount()).append('\n');
        sb.append("- passRate: ").append(formatPercent(report.passRate())).append("\n\n");

        sb.append("## Targets\n");
        for (SpringProfileTarget target : report.targets()) {
            sb.append("- ")
                .append(target.id())
                .append(": profile=")
                .append(target.profile())
                .append(", springBoot=")
                .append(target.springBootLine())
                .append(", springData=")
                .append(target.springDataLine())
                .append(", java=")
                .append(target.javaLine())
                .append('\n');
        }
        sb.append('\n');

        Map<String, MatrixCellResult> lookup = buildLookup(report.results());
        sb.append("## Matrix\n");
        sb.append("| scenarioId | surface |");
        for (SpringProfileTarget target : report.targets()) {
            sb.append(' ').append(target.id()).append(" |");
        }
        sb.append('\n');
        sb.append("| --- | --- |");
        for (int i = 0; i < report.targets().size(); i++) {
            sb.append(" --- |");
        }
        sb.append('\n');

        for (SpringScenario scenario : report.scenarios()) {
            sb.append("| ")
                .append(scenario.id())
                .append(" | ")
                .append(scenario.surface().label())
                .append(" |");
            for (SpringProfileTarget target : report.targets()) {
                MatrixCellResult cell = lookup.get(resultKey(target.id(), scenario.id()));
                sb.append(' ').append(cell.status()).append(" |");
            }
            sb.append('\n');
        }
        sb.append('\n');

        sb.append("## Complex Query Matrix\n");
        List<SpringScenario> complexScenarios = report.complexScenarios();
        if (complexScenarios.isEmpty()) {
            sb.append("- none\n\n");
        } else {
            sb.append("- scenarioCount: ").append(complexScenarios.size()).append('\n');
            sb.append("- totalCells: ").append(report.complexTotalCells()).append('\n');
            sb.append("- pass: ").append(report.complexPassCount()).append('\n');
            sb.append("- fail: ").append(report.complexFailCount()).append('\n');
            sb.append("- passRate: ").append(formatPercent(report.complexPassRate())).append("\n\n");

            sb.append("| scenarioId | certPatternId |");
            for (SpringProfileTarget target : report.targets()) {
                sb.append(' ').append(target.id()).append(" |");
            }
            sb.append('\n');
            sb.append("| --- | --- |");
            for (int i = 0; i < report.targets().size(); i++) {
                sb.append(" --- |");
            }
            sb.append('\n');

            for (SpringScenario scenario : complexScenarios) {
                sb.append("| ")
                    .append(scenario.id())
                    .append(" | ")
                    .append(scenario.certificationPatternId() == null ? "-" : scenario.certificationPatternId())
                    .append(" |");
                for (SpringProfileTarget target : report.targets()) {
                    MatrixCellResult cell = lookup.get(resultKey(target.id(), scenario.id()));
                    sb.append(' ').append(cell.status()).append(" |");
                }
                sb.append('\n');
            }
            sb.append('\n');
        }

        sb.append("## Certification Pattern Mapping\n");
        List<SpringScenario> mappedScenarios = scenariosWithCertificationPattern(report.scenarios());
        if (mappedScenarios.isEmpty()) {
            sb.append("- none\n\n");
        } else {
            sb.append("- mappedScenarioCount: ").append(mappedScenarios.size()).append('\n');
            sb.append("| scenarioId | certPatternId | surface | complexQuery |");
            sb.append('\n');
            sb.append("| --- | --- | --- | --- |");
            sb.append('\n');
            for (SpringScenario scenario : mappedScenarios) {
                sb.append("| ")
                    .append(scenario.id())
                    .append(" | ")
                    .append(scenario.certificationPatternId())
                    .append(" | ")
                    .append(scenario.surface().label())
                    .append(" | ")
                    .append(scenario.isComplexQueryScenario() ? "YES" : "NO")
                    .append(" |");
                sb.append('\n');
            }
            sb.append('\n');
        }

        sb.append("## Target Summary\n");
        for (TargetSummary summary : report.targetSummaries()) {
            sb.append("- ")
                .append(summary.targetId())
                .append(": pass=")
                .append(summary.passCount())
                .append(", fail=")
                .append(summary.failCount())
                .append(", passRate=")
                .append(formatPercent(summary.passRate()))
                .append('\n');
        }
        sb.append('\n');

        sb.append("## Surface Summary\n");
        for (SurfaceSummary summary : report.surfaceSummaries()) {
            sb.append("- ")
                .append(summary.surface().label())
                .append(": pass=")
                .append(summary.passCount())
                .append(", fail=")
                .append(summary.failCount())
                .append(", passRate=")
                .append(formatPercent(summary.passRate()))
                .append('\n');
        }
        sb.append('\n');

        sb.append("## Failure Minimization\n");
        List<FailureSample> failures = report.failureMinimization(DEFAULT_FAILURE_SAMPLE_LIMIT);
        if (failures.isEmpty()) {
            sb.append("- none\n");
        } else {
            for (FailureSample failure : failures) {
                sb.append("- ")
                    .append(failure.targetId())
                    .append(" / ")
                    .append(failure.scenarioId())
                    .append(" (")
                    .append(failure.surface().label())
                    .append("): ")
                    .append(failure.errorMessage())
                    .append('\n');
            }
        }
        return sb.toString();
    }

    String renderJson(MatrixReport report) {
        Objects.requireNonNull(report, "report");
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedAt", report.generatedAt().toString());
        root.put("endpointId", report.endpointId());

        Map<String, Object> dimensions = new LinkedHashMap<>();
        dimensions.put("targetCount", report.targets().size());
        dimensions.put("scenarioCount", report.scenarios().size());
        dimensions.put("totalCells", report.totalCells());
        root.put("matrixDimensions", dimensions);

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("pass", report.passCount());
        summary.put("fail", report.failCount());
        summary.put("passRate", report.passRate());
        root.put("summary", summary);

        List<Map<String, Object>> targets = new ArrayList<>();
        for (SpringProfileTarget target : report.targets()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("targetId", target.id());
            item.put("profile", target.profile());
            item.put("springBootLine", target.springBootLine());
            item.put("springDataLine", target.springDataLine());
            item.put("javaLine", target.javaLine());
            targets.add(item);
        }
        root.put("targets", targets);

        List<Map<String, Object>> scenarios = new ArrayList<>();
        for (SpringScenario scenario : report.scenarios()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("scenarioId", scenario.id());
            item.put("surface", scenario.surface().name());
            item.put("description", scenario.description());
            item.put("certificationPatternId", scenario.certificationPatternId());
            item.put("complexQueryScenario", scenario.isComplexQueryScenario());
            item.put("commandCount", scenario.scenario().commands().size());
            scenarios.add(item);
        }
        root.put("scenarios", scenarios);

        Map<String, Object> complexQuerySummary = new LinkedHashMap<>();
        complexQuerySummary.put("scenarioCount", report.complexScenarios().size());
        complexQuerySummary.put("totalCells", report.complexTotalCells());
        complexQuerySummary.put("pass", report.complexPassCount());
        complexQuerySummary.put("fail", report.complexFailCount());
        complexQuerySummary.put("passRate", report.complexPassRate());
        root.put("complexQuerySummary", complexQuerySummary);

        List<Map<String, Object>> complexQueryScenarios = new ArrayList<>();
        for (SpringScenario scenario : report.complexScenarios()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("scenarioId", scenario.id());
            item.put("surface", scenario.surface().name());
            item.put("description", scenario.description());
            item.put("certificationPatternId", scenario.certificationPatternId());
            item.put("commandCount", scenario.scenario().commands().size());
            complexQueryScenarios.add(item);
        }
        root.put("complexQueryScenarios", complexQueryScenarios);

        List<Map<String, Object>> certificationPatternMapping = new ArrayList<>();
        for (SpringScenario scenario : scenariosWithCertificationPattern(report.scenarios())) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("scenarioId", scenario.id());
            item.put("certificationPatternId", scenario.certificationPatternId());
            item.put("surface", scenario.surface().name());
            item.put("complexQueryScenario", scenario.isComplexQueryScenario());
            certificationPatternMapping.add(item);
        }
        root.put("certificationPatternMapping", certificationPatternMapping);

        List<Map<String, Object>> results = new ArrayList<>();
        for (MatrixCellResult result : report.results()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("targetId", result.targetId());
            item.put("scenarioId", result.scenarioId());
            item.put("surface", result.surface().name());
            item.put("status", result.status().name());
            item.put("errorMessage", result.errorMessage());
            item.put("durationMillis", result.durationMillis());
            item.put("commandCount", result.commandCount());
            results.add(item);
        }
        root.put("results", results);

        List<Map<String, Object>> complexQueryResults = new ArrayList<>();
        for (MatrixCellResult result : report.complexResults()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("targetId", result.targetId());
            item.put("scenarioId", result.scenarioId());
            item.put("surface", result.surface().name());
            item.put("status", result.status().name());
            item.put("errorMessage", result.errorMessage());
            item.put("durationMillis", result.durationMillis());
            item.put("commandCount", result.commandCount());
            complexQueryResults.add(item);
        }
        root.put("complexQueryResults", complexQueryResults);

        List<Map<String, Object>> targetSummary = new ArrayList<>();
        for (TargetSummary item : report.targetSummaries()) {
            Map<String, Object> encoded = new LinkedHashMap<>();
            encoded.put("targetId", item.targetId());
            encoded.put("pass", item.passCount());
            encoded.put("fail", item.failCount());
            encoded.put("passRate", item.passRate());
            targetSummary.add(encoded);
        }
        root.put("targetSummary", targetSummary);

        List<Map<String, Object>> surfaceSummary = new ArrayList<>();
        for (SurfaceSummary item : report.surfaceSummaries()) {
            Map<String, Object> encoded = new LinkedHashMap<>();
            encoded.put("surface", item.surface().name());
            encoded.put("pass", item.passCount());
            encoded.put("fail", item.failCount());
            encoded.put("passRate", item.passRate());
            surfaceSummary.add(encoded);
        }
        root.put("surfaceSummary", surfaceSummary);

        List<Map<String, Object>> failureMinimization = new ArrayList<>();
        for (FailureSample item : report.failureMinimization(DEFAULT_FAILURE_SAMPLE_LIMIT)) {
            Map<String, Object> encoded = new LinkedHashMap<>();
            encoded.put("targetId", item.targetId());
            encoded.put("scenarioId", item.scenarioId());
            encoded.put("surface", item.surface().name());
            encoded.put("errorMessage", item.errorMessage());
            failureMinimization.add(encoded);
        }
        root.put("failureMinimization", failureMinimization);

        return JsonEncoder.encode(root);
    }

    static List<SpringProfileTarget> defaultTargets() {
        return List.of(
            new SpringProfileTarget("petclinic-boot-2.7-data-3.4", "petclinic27", "2.7.x", "3.4.x", "17"),
            new SpringProfileTarget("petclinic-boot-3.2-data-4.2", "petclinic32", "3.2.x", "4.2.x", "17"),
            new SpringProfileTarget("commerce-boot-3.2-data-4.2", "commerce32", "3.2.x", "4.2.x", "17"),
            new SpringProfileTarget("commerce-boot-3.3-data-4.3", "commerce33", "3.3.x", "4.3.x", "21"),
            new SpringProfileTarget("analytics-boot-3.4-data-4.4", "analytics34", "3.4.x", "4.4.x", "21")
        );
    }

    static List<SpringScenario> defaultScenarios() {
        return List.of(
            new SpringScenario(
                "spring.mongo-template.basic-crud",
                SpringSurface.MONGO_TEMPLATE,
                "MongoTemplate-style insert/update/find path",
                scenario(
                    "spring.mongo-template.basic-crud",
                    "insert + update + find",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_template_users",
                            "documents",
                            List.of(
                                payload("_id", 1, "status", "new", "name", "alpha"),
                                payload("_id", 2, "status", "new", "name", "beta")
                            )
                        )
                    ),
                    command(
                        "update",
                        payload(
                            "collection",
                            "spring_template_users",
                            "updates",
                            List.of(
                                payload(
                                    "q",
                                    payload("status", "new"),
                                    "u",
                                    payload("$set", payload("status", "active")),
                                    "multi",
                                    true
                                )
                            )
                        )
                    ),
                    command("find", payload("collection", "spring_template_users", "filter", payload("status", "active")))
                )
            ),
            new SpringScenario(
                "spring.repository.derived-query",
                SpringSurface.REPOSITORY,
                "Repository-style saveAll + findByStatus",
                scenario(
                    "spring.repository.derived-query",
                    "insert + query by status",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_repository_orders",
                            "documents",
                            List.of(
                                payload("_id", 1, "status", "OPEN", "region", "APAC"),
                                payload("_id", 2, "status", "CLOSED", "region", "EMEA"),
                                payload("_id", 3, "status", "OPEN", "region", "EMEA")
                            )
                        )
                    ),
                    command("find", payload("collection", "spring_repository_orders", "filter", payload("status", "OPEN")))
                )
            ),
            new SpringScenario(
                "spring.transaction-template.commit",
                SpringSurface.TRANSACTION_TEMPLATE,
                "TransactionTemplate-style start/commit transactional path",
                scenario(
                    "spring.transaction-template.commit",
                    "start transaction + commit",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_txn_orders",
                            "documents",
                            List.of(payload("_id", 1, "state", "pending")),
                            "lsid",
                            payload("id", "spring-session-tx-1"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false,
                            "startTransaction",
                            true
                        )
                    ),
                    command(
                        "update",
                        payload(
                            "collection",
                            "spring_txn_orders",
                            "updates",
                            List.of(
                                payload(
                                    "q",
                                    payload("_id", 1),
                                    "u",
                                    payload("$set", payload("state", "committed"))
                                )
                            ),
                            "lsid",
                            payload("id", "spring-session-tx-1"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false
                        )
                    ),
                    command(
                        "commitTransaction",
                        payload(
                            "lsid",
                            payload("id", "spring-session-tx-1"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false
                        )
                    ),
                    command("find", payload("collection", "spring_txn_orders", "filter", payload("_id", 1)))
                )
            ),
            new SpringScenario(
                "spring.transaction-template.multi-template.same-manager",
                SpringSurface.TRANSACTION_TEMPLATE,
                "Single transaction writes across two template-backed namespaces",
                scenario(
                    "spring.transaction-template.multi-template.same-manager",
                    "start transaction + write namespace A/B + commit",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_txn_users",
                            "documents",
                            List.of(payload("_id", 11, "name", "txn-user")),
                            "lsid",
                            payload("id", "spring-session-tx-2"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false,
                            "startTransaction",
                            true
                        )
                    ),
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_txn_tenant_mappings",
                            "documents",
                            List.of(payload("_id", 12, "tenantId", "tenant-a")),
                            "lsid",
                            payload("id", "spring-session-tx-2"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false
                        )
                    ),
                    command(
                        "commitTransaction",
                        payload(
                            "lsid",
                            payload("id", "spring-session-tx-2"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false
                        )
                    ),
                    command("find", payload("collection", "spring_txn_users", "filter", payload("_id", 11))),
                    command("find", payload("collection", "spring_txn_tenant_mappings", "filter", payload("_id", 12)))
                )
            ),
            new SpringScenario(
                "spring.transaction-template.out-of-scope.namespace-interleaving",
                SpringSurface.TRANSACTION_TEMPLATE,
                "Transactional namespace commit preserves non-transactional writes in other namespace",
                scenario(
                    "spring.transaction-template.out-of-scope.namespace-interleaving",
                    "start transaction + out-of-scope write + commit",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_txn_scope_users",
                            "documents",
                            List.of(payload("_id", 21, "state", "txn")),
                            "lsid",
                            payload("id", "spring-session-tx-3"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false,
                            "startTransaction",
                            true
                        )
                    ),
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_txn_scope_mappings",
                            "documents",
                            List.of(payload("_id", 22, "state", "outside"))
                        )
                    ),
                    command(
                        "commitTransaction",
                        payload(
                            "lsid",
                            payload("id", "spring-session-tx-3"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false
                        )
                    ),
                    command("find", payload("collection", "spring_txn_scope_users", "filter", payload("_id", 21))),
                    command("find", payload("collection", "spring_txn_scope_mappings", "filter", payload("_id", 22)))
                )
            ),
            new SpringScenario(
                "spring.transaction-template.out-of-scope.same-namespace-interleaving",
                SpringSurface.TRANSACTION_TEMPLATE,
                "Transactional commit preserves non-transactional writes in same namespace when ids differ",
                scenario(
                    "spring.transaction-template.out-of-scope.same-namespace-interleaving",
                    "start transaction + out-of-scope write same namespace + commit",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_txn_scope_same_namespace",
                            "documents",
                            List.of(payload("_id", 31, "state", "txn")),
                            "lsid",
                            payload("id", "spring-session-tx-4"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false,
                            "startTransaction",
                            true
                        )
                    ),
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_txn_scope_same_namespace",
                            "documents",
                            List.of(payload("_id", 32, "state", "outside"))
                        )
                    ),
                    command(
                        "commitTransaction",
                        payload(
                            "lsid",
                            payload("id", "spring-session-tx-4"),
                            "txnNumber",
                            1,
                            "autocommit",
                            false
                        )
                    ),
                    command("find", payload("collection", "spring_txn_scope_same_namespace", "filter", payload()))
                )
            ),
            new SpringScenario(
                "spring.repository.aggregation-lookup",
                SpringSurface.REPOSITORY,
                "Repository aggregation with $lookup join",
                "cq.lookup.local-foreign-unwind",
                scenario(
                    "spring.repository.aggregation-lookup",
                    "insert + lookup + aggregate",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_lookup_users",
                            "documents",
                            List.of(
                                payload("_id", 1, "name", "kim"),
                                payload("_id", 2, "name", "park")
                            )
                        )
                    ),
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_lookup_orders",
                            "documents",
                            List.of(
                                payload("_id", 11, "userId", 1, "total", 120),
                                payload("_id", 12, "userId", 2, "total", 90)
                            )
                        )
                    ),
                    command(
                        "aggregate",
                        payload(
                            "collection",
                            "spring_lookup_orders",
                            "pipeline",
                            List.of(
                                payload(
                                    "$lookup",
                                    payload(
                                        "from",
                                        "spring_lookup_users",
                                        "localField",
                                        "userId",
                                        "foreignField",
                                        "_id",
                                        "as",
                                        "user"
                                    )
                                ),
                                payload("$match", payload("user.name", "kim"))
                            ),
                            "cursor",
                            payload()
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.complex.query.nested-criteria",
                SpringSurface.REPOSITORY,
                "Nested criteria query aligned with certification pack",
                "cq.nested.logic.and-or-dotted",
                scenario(
                    "spring.complex.query.nested-criteria",
                    "insert + nested and/or criteria query",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_nested_users",
                            "documents",
                            List.of(
                                payload(
                                    "_id",
                                    1,
                                    "profile",
                                    payload("city", "Seoul", "tier", "gold"),
                                    "active",
                                    true,
                                    "tags",
                                    List.of("core", "java")
                                ),
                                payload(
                                    "_id",
                                    2,
                                    "profile",
                                    payload("city", "Busan", "tier", "silver"),
                                    "active",
                                    true,
                                    "tags",
                                    List.of("ops")
                                ),
                                payload(
                                    "_id",
                                    3,
                                    "profile",
                                    payload("city", "Seoul", "tier", "bronze"),
                                    "active",
                                    false,
                                    "tags",
                                    List.of("ops")
                                )
                            )
                        )
                    ),
                    command(
                        "find",
                        payload(
                            "collection",
                            "spring_complex_nested_users",
                            "filter",
                            payload(
                                "$and",
                                List.of(
                                    payload("profile.city", "Seoul"),
                                    payload(
                                        "$or",
                                        List.of(
                                            payload("active", true),
                                            payload("tags", "ops")
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.complex.aggregation.projection-sort",
                SpringSurface.MONGO_TEMPLATE,
                "Projection and sort aggregation composition aligned with certification pack",
                "cq.aggregate.sortbycount-after-project",
                scenario(
                    "spring.complex.aggregation.projection-sort",
                    "insert + aggregate project/sort",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_projection_sort",
                            "documents",
                            List.of(
                                payload("_id", 1, "name", "kim", "profile", payload("city", "Seoul")),
                                payload("_id", 2, "name", "park", "profile", payload("city", "Busan")),
                                payload("_id", 3, "name", "lee", "profile", payload("city", "Seoul"))
                            )
                        )
                    ),
                    command(
                        "aggregate",
                        payload(
                            "collection",
                            "spring_complex_projection_sort",
                            "pipeline",
                            List.of(
                                payload("$project", payload("_id", 1, "name", 1, "city", "$profile.city")),
                                payload("$sort", payload("city", 1, "name", 1))
                            ),
                            "cursor",
                            payload()
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.complex.aggregation.lookup-unwind-group",
                SpringSurface.REPOSITORY,
                "Lookup + unwind + group composition aligned with certification pack",
                "cq.lookup.local-foreign-unwind",
                scenario(
                    "spring.complex.aggregation.lookup-unwind-group",
                    "insert + lookup + unwind + group",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_lookup_orders",
                            "documents",
                            List.of(
                                payload("_id", 1, "customerId", 11, "amount", 120),
                                payload("_id", 2, "customerId", 11, "amount", 30),
                                payload("_id", 3, "customerId", 12, "amount", 80)
                            )
                        )
                    ),
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_lookup_customers",
                            "documents",
                            List.of(
                                payload("_id", 11, "customerId", 11, "tier", "gold"),
                                payload("_id", 12, "customerId", 12, "tier", "silver")
                            )
                        )
                    ),
                    command(
                        "aggregate",
                        payload(
                            "collection",
                            "spring_complex_lookup_orders",
                            "pipeline",
                            List.of(
                                payload(
                                    "$lookup",
                                    payload(
                                        "from",
                                        "spring_complex_lookup_customers",
                                        "localField",
                                        "customerId",
                                        "foreignField",
                                        "customerId",
                                        "as",
                                        "customer"
                                    )
                                ),
                                payload("$unwind", "$customer"),
                                payload("$match", payload("customer.tier", "gold")),
                                payload(
                                    "$group",
                                    payload("_id", "$customer.tier", "totalAmount", payload("$sum", "$amount"))
                                ),
                                payload("$sort", payload("_id", 1))
                            ),
                            "cursor",
                            payload()
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.complex.aggregation.lookup-pipeline-let",
                SpringSurface.REPOSITORY,
                "Lookup pipeline + let composition aligned with certification pack",
                "cq.lookup.pipeline-let-match",
                scenario(
                    "spring.complex.aggregation.lookup-pipeline-let",
                    "insert + lookup pipeline with let",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_lookup_pipeline_orders",
                            "documents",
                            List.of(
                                payload("_id", 1, "orderId", "O-1"),
                                payload("_id", 2, "orderId", "O-2")
                            )
                        )
                    ),
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_lookup_pipeline_items",
                            "documents",
                            List.of(
                                payload("_id", 11, "orderId", "O-1", "sku", "A", "qty", 3),
                                payload("_id", 12, "orderId", "O-1", "sku", "B", "qty", 1),
                                payload("_id", 13, "orderId", "O-2", "sku", "A", "qty", 1)
                            )
                        )
                    ),
                    command(
                        "aggregate",
                        payload(
                            "collection",
                            "spring_complex_lookup_pipeline_orders",
                            "pipeline",
                            List.of(
                                payload(
                                    "$lookup",
                                    payload(
                                        "from",
                                        "spring_complex_lookup_pipeline_items",
                                        "let",
                                        payload("orderIdVar", "$orderId"),
                                        "pipeline",
                                        List.of(
                                            payload(
                                                "$match",
                                                payload(
                                                    "$expr",
                                                    payload("$eq", List.of("$orderId", "$$orderIdVar"))
                                                )
                                            ),
                                            payload("$match", payload("qty", payload("$gte", 2)))
                                        ),
                                        "as",
                                        "items"
                                    )
                                ),
                                payload("$match", payload("items", payload("$elemMatch", payload("sku", "A"))))
                            ),
                            "cursor",
                            payload()
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.complex.query.expr-array-index-comparison",
                SpringSurface.REPOSITORY,
                "Expr array-index traversal compatibility aligned with certification pack",
                "cq.expr.array-index-comparison",
                scenario(
                    "spring.complex.query.expr-array-index-comparison",
                    "insert + expr array-index compatibility checks",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_expr_array",
                            "documents",
                            List.of(
                                payload("_id", 1, "metrics", List.of(10, 20), "series", List.of(payload("value", 5))),
                                payload("_id", 2, "metrics", List.of(9), "series", List.of(payload("value", 5)))
                            )
                        )
                    ),
                    command(
                        "find",
                        payload(
                            "collection",
                            "spring_complex_expr_array",
                            "filter",
                            payload(
                                "$expr",
                                payload("$eq", List.of("$metrics.0", 10))
                            )
                        )
                    ),
                    command(
                        "find",
                        payload(
                            "collection",
                            "spring_complex_expr_array",
                            "filter",
                            payload(
                                "$expr",
                                payload("$eq", List.of("$metrics", List.of(10, 20)))
                            )
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.complex.query.array-index-comparison",
                SpringSurface.MONGO_TEMPLATE,
                "Array index comparison compatibility aligned with certification pack",
                "cq.path.array-index-comparison",
                scenario(
                    "spring.complex.query.array-index-comparison",
                    "insert + array-index comparison query",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_array_index_compare",
                            "documents",
                            List.of(
                                payload("_id", 1, "metrics", List.of(11, 2)),
                                payload("_id", 2, "metrics", List.of(9, 8)),
                                payload("_id", 3, "metrics", List.of(12, 1))
                            )
                        )
                    ),
                    command(
                        "find",
                        payload(
                            "collection",
                            "spring_complex_array_index_compare",
                            "filter",
                            payload("metrics.0", payload("$gt", 10))
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.complex.query.deep-array-document",
                SpringSurface.REPOSITORY,
                "Deep array document path compatibility aligned with certification pack",
                "cq.path.deep-array-document",
                scenario(
                    "spring.complex.query.deep-array-document",
                    "insert + deep array document query",
                    command(
                        "insert",
                        payload(
                            "collection",
                            "spring_complex_deep_array_docs",
                            "documents",
                            List.of(
                                payload(
                                    "_id",
                                    1,
                                    "series",
                                    List.of(
                                        payload("meta", payload("code", "A"), "value", 11),
                                        payload("meta", payload("code", "B"), "value", 3)
                                    )
                                ),
                                payload(
                                    "_id",
                                    2,
                                    "series",
                                    List.of(
                                        payload("meta", payload("code", "B"), "value", 7)
                                    )
                                )
                            )
                        )
                    ),
                    command(
                        "find",
                        payload(
                            "collection",
                            "spring_complex_deep_array_docs",
                            "filter",
                            payload("series.0.meta.code", "A")
                        )
                    )
                )
            ),
            new SpringScenario(
                "spring.mongo-template.index-ttl-partial",
                SpringSurface.MONGO_TEMPLATE,
                "MongoTemplate index lifecycle with ttl/partial/collation metadata",
                scenario(
                    "spring.mongo-template.index-ttl-partial",
                    "create index + list indexes",
                    command(
                        "createIndexes",
                        payload(
                            "collection",
                            "spring_template_indexes",
                            "indexes",
                            List.of(
                                payload(
                                    "name",
                                    "email_1",
                                    "key",
                                    payload("email", 1),
                                    "sparse",
                                    true,
                                    "partialFilterExpression",
                                    payload("email", payload("$exists", true)),
                                    "collation",
                                    payload("locale", "en", "strength", 2),
                                    "expireAfterSeconds",
                                    3600
                                )
                            )
                        )
                    ),
                    command("listIndexes", payload("collection", "spring_template_indexes"))
                )
            )
        );
    }

    private List<SpringProfileTarget> resolveTargets(List<String> requestedTargetIds) {
        if (requestedTargetIds.isEmpty()) {
            return targetCatalog;
        }

        Map<String, SpringProfileTarget> byId = new LinkedHashMap<>();
        for (SpringProfileTarget target : targetCatalog) {
            byId.put(target.id(), target);
        }

        List<SpringProfileTarget> selected = new ArrayList<>(requestedTargetIds.size());
        for (String targetId : requestedTargetIds) {
            SpringProfileTarget target = byId.get(targetId);
            if (target == null) {
                throw new IllegalArgumentException("unknown target id: " + targetId);
            }
            selected.add(target);
        }
        if (selected.size() < 2) {
            throw new IllegalArgumentException("matrix requires at least two targets");
        }
        return List.copyOf(selected);
    }

    private static boolean containsHelpFlag(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static void printUsage() {
        System.out.println("Usage: SpringCompatibilityMatrixRunner [options]");
        System.out.println("  --output-dir=<path>       Output directory for JSON/MD artifacts");
        System.out.println("  --targets=<id,id>         Target ids from built-in matrix catalog (at least 2)");
        System.out.println("  --fail-on-failures        Exit non-zero if any matrix cell fails (default)");
        System.out.println("  --no-fail-on-failures     Always exit zero");
        System.out.println("  --help                    Show this help message");
    }

    private static String minimizeFailure(String message) {
        String normalized = normalizeText(message);
        if (normalized == null) {
            return "unknown failure";
        }
        String singleLine = normalized.replace('\r', ' ').replace('\n', ' ').replaceAll("\\s+", " ").trim();
        if (singleLine.length() <= FAILURE_MESSAGE_LIMIT) {
            return singleLine;
        }
        return singleLine.substring(0, FAILURE_MESSAGE_LIMIT - 3) + "...";
    }

    private static Map<String, MatrixCellResult> buildLookup(List<MatrixCellResult> results) {
        Map<String, MatrixCellResult> lookup = new LinkedHashMap<>();
        for (MatrixCellResult result : results) {
            lookup.put(resultKey(result.targetId(), result.scenarioId()), result);
        }
        return lookup;
    }

    private static List<SpringScenario> scenariosWithCertificationPattern(List<SpringScenario> scenarios) {
        List<SpringScenario> mapped = new ArrayList<>();
        for (SpringScenario scenario : scenarios) {
            if (scenario.certificationPatternId() != null) {
                mapped.add(scenario);
            }
        }
        return List.copyOf(mapped);
    }

    private static String resultKey(String targetId, String scenarioId) {
        return targetId + "::" + scenarioId;
    }

    private static String formatPercent(double ratio) {
        return String.format(Locale.ROOT, "%.2f%%", ratio * 100.0d);
    }

    private static String requireText(String value, String fieldName) {
        String normalized = normalizeText(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String normalizeText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static double requireFinite(double value, String fieldName) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(fieldName + " must be finite");
        }
        return value;
    }

    private static List<SpringProfileTarget> copyTargets(List<SpringProfileTarget> source) {
        Objects.requireNonNull(source, "targetCatalog");
        if (source.size() < 2) {
            throw new IllegalArgumentException("targetCatalog must include at least two targets");
        }
        List<SpringProfileTarget> copy = new ArrayList<>(source.size());
        Set<String> seenIds = new LinkedHashSet<>();
        for (SpringProfileTarget target : source) {
            SpringProfileTarget nonNullTarget = Objects.requireNonNull(target, "target");
            if (!seenIds.add(nonNullTarget.id())) {
                throw new IllegalArgumentException("duplicate target id: " + nonNullTarget.id());
            }
            copy.add(nonNullTarget);
        }
        return List.copyOf(copy);
    }

    private static List<SpringScenario> copyScenarios(List<SpringScenario> source) {
        Objects.requireNonNull(source, "scenarioCatalog");
        if (source.isEmpty()) {
            throw new IllegalArgumentException("scenarioCatalog must not be empty");
        }
        List<SpringScenario> copy = new ArrayList<>(source.size());
        Set<String> seenIds = new LinkedHashSet<>();
        for (SpringScenario scenario : source) {
            SpringScenario nonNullScenario = Objects.requireNonNull(scenario, "scenario");
            if (!seenIds.add(nonNullScenario.id())) {
                throw new IllegalArgumentException("duplicate scenario id: " + nonNullScenario.id());
            }
            copy.add(nonNullScenario);
        }
        return List.copyOf(copy);
    }

    private static Scenario scenario(String id, String description, ScenarioCommand... commands) {
        return new Scenario(id, description, List.of(commands));
    }

    private static ScenarioCommand command(String commandName, Map<String, Object> payload) {
        return new ScenarioCommand(commandName, payload);
    }

    private static Map<String, Object> payload(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues length must be even");
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            Object key = keyValues[i];
            if (!(key instanceof String keyString)) {
                throw new IllegalArgumentException("keyValues keys must be strings");
            }
            values.put(keyString, keyValues[i + 1]);
        }
        return Collections.unmodifiableMap(values);
    }

    @FunctionalInterface
    public interface EndpointFactory {
        JongodbEndpoint create(SpringProfileTarget target);
    }

    public interface JongodbEndpoint {
        String id();

        ScenarioOutcome execute(Scenario scenario);
    }

    private static final class WireIngressEndpoint implements JongodbEndpoint {
        private final WireCommandIngressBackend backend;

        private WireIngressEndpoint(String backendName) {
            this.backend = new WireCommandIngressBackend(requireText(backendName, "backendName"));
        }

        @Override
        public String id() {
            return DEFAULT_ENDPOINT_ID;
        }

        @Override
        public ScenarioOutcome execute(Scenario scenario) {
            return backend.execute(scenario);
        }
    }

    public static final class ArtifactPaths {
        private final Path matrixJson;
        private final Path matrixMarkdown;

        ArtifactPaths(Path matrixJson, Path matrixMarkdown) {
            this.matrixJson = Objects.requireNonNull(matrixJson, "matrixJson");
            this.matrixMarkdown = Objects.requireNonNull(matrixMarkdown, "matrixMarkdown");
        }

        public Path matrixJson() {
            return matrixJson;
        }

        public Path matrixMarkdown() {
            return matrixMarkdown;
        }
    }

    public static final class EvidenceConfig {
        private final Path outputDir;
        private final List<String> targetIds;
        private final boolean failOnFailures;

        public EvidenceConfig(Path outputDir, List<String> targetIds, boolean failOnFailures) {
            this.outputDir = Objects.requireNonNull(outputDir, "outputDir").normalize();
            this.targetIds = copyTargetIds(targetIds);
            if (!this.targetIds.isEmpty() && this.targetIds.size() < 2) {
                throw new IllegalArgumentException("targetIds must include at least two ids when set");
            }
            this.failOnFailures = failOnFailures;
        }

        public static EvidenceConfig fromArgs(String[] args) {
            Path outputDir = DEFAULT_OUTPUT_DIR;
            List<String> targetIds = List.of();
            boolean failOnFailures = true;

            for (String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--output-dir=")) {
                    outputDir = Path.of(readValue(arg, "--output-dir="));
                    continue;
                }
                if (arg.startsWith("--targets=")) {
                    targetIds = parseTargetIds(readValue(arg, "--targets="));
                    continue;
                }
                if ("--fail-on-failures".equals(arg)) {
                    failOnFailures = true;
                    continue;
                }
                if ("--no-fail-on-failures".equals(arg)) {
                    failOnFailures = false;
                    continue;
                }
                throw new IllegalArgumentException("unknown option: " + arg);
            }

            return new EvidenceConfig(outputDir, targetIds, failOnFailures);
        }

        public Path outputDir() {
            return outputDir;
        }

        public List<String> targetIds() {
            return targetIds;
        }

        public boolean failOnFailures() {
            return failOnFailures;
        }

        private static List<String> parseTargetIds(String raw) {
            String normalized = requireText(raw, "targets");
            LinkedHashSet<String> unique = new LinkedHashSet<>();
            for (String token : normalized.split(",")) {
                String id = normalizeText(token);
                if (id != null) {
                    unique.add(id);
                }
            }
            if (unique.isEmpty()) {
                throw new IllegalArgumentException("targets must not be empty");
            }
            if (unique.size() < 2) {
                throw new IllegalArgumentException("targets must include at least two ids");
            }
            return List.copyOf(unique);
        }

        private static String readValue(String arg, String prefix) {
            String value = arg.substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(prefix + " requires a value");
            }
            return value;
        }

        private static List<String> copyTargetIds(List<String> source) {
            Objects.requireNonNull(source, "targetIds");
            LinkedHashSet<String> unique = new LinkedHashSet<>();
            for (String value : source) {
                unique.add(requireText(value, "targetId"));
            }
            return List.copyOf(unique);
        }
    }

    public enum SpringSurface {
        MONGO_TEMPLATE("MongoTemplate"),
        REPOSITORY("Repository"),
        TRANSACTION_TEMPLATE("TransactionTemplate");

        private final String label;

        SpringSurface(String label) {
            this.label = requireText(label, "label");
        }

        public String label() {
            return label;
        }
    }

    public enum MatrixCellStatus {
        PASS,
        FAIL
    }

    public static final class SpringProfileTarget {
        private final String id;
        private final String profile;
        private final String springBootLine;
        private final String springDataLine;
        private final String javaLine;

        public SpringProfileTarget(
            String id,
            String profile,
            String springBootLine,
            String springDataLine,
            String javaLine
        ) {
            this.id = requireText(id, "id");
            this.profile = requireText(profile, "profile");
            this.springBootLine = requireText(springBootLine, "springBootLine");
            this.springDataLine = requireText(springDataLine, "springDataLine");
            this.javaLine = requireText(javaLine, "javaLine");
        }

        public String id() {
            return id;
        }

        public String profile() {
            return profile;
        }

        public String springBootLine() {
            return springBootLine;
        }

        public String springDataLine() {
            return springDataLine;
        }

        public String javaLine() {
            return javaLine;
        }
    }

    public static final class SpringScenario {
        private final String id;
        private final SpringSurface surface;
        private final String description;
        private final String certificationPatternId;
        private final Scenario scenario;

        public SpringScenario(String id, SpringSurface surface, String description, Scenario scenario) {
            this(id, surface, description, null, scenario);
        }

        public SpringScenario(
            String id,
            SpringSurface surface,
            String description,
            String certificationPatternId,
            Scenario scenario
        ) {
            this.id = requireText(id, "id");
            this.surface = Objects.requireNonNull(surface, "surface");
            this.description = requireText(description, "description");
            this.certificationPatternId = normalizeText(certificationPatternId);
            this.scenario = Objects.requireNonNull(scenario, "scenario");
        }

        public String id() {
            return id;
        }

        public SpringSurface surface() {
            return surface;
        }

        public String description() {
            return description;
        }

        public String certificationPatternId() {
            return certificationPatternId;
        }

        public boolean isComplexQueryScenario() {
            return certificationPatternId != null || id.startsWith("spring.complex.");
        }

        public Scenario scenario() {
            return scenario;
        }
    }

    public static final class MatrixCellResult {
        private final String targetId;
        private final String scenarioId;
        private final SpringSurface surface;
        private final MatrixCellStatus status;
        private final String errorMessage;
        private final long durationMillis;
        private final int commandCount;

        public MatrixCellResult(
            String targetId,
            String scenarioId,
            SpringSurface surface,
            MatrixCellStatus status,
            String errorMessage,
            long durationMillis,
            int commandCount
        ) {
            this.targetId = requireText(targetId, "targetId");
            this.scenarioId = requireText(scenarioId, "scenarioId");
            this.surface = Objects.requireNonNull(surface, "surface");
            this.status = Objects.requireNonNull(status, "status");
            this.errorMessage = normalizeText(errorMessage);
            if (durationMillis < 0L) {
                throw new IllegalArgumentException("durationMillis must be >= 0");
            }
            if (commandCount <= 0) {
                throw new IllegalArgumentException("commandCount must be > 0");
            }
            this.durationMillis = durationMillis;
            this.commandCount = commandCount;
        }

        public String targetId() {
            return targetId;
        }

        public String scenarioId() {
            return scenarioId;
        }

        public SpringSurface surface() {
            return surface;
        }

        public MatrixCellStatus status() {
            return status;
        }

        public boolean passed() {
            return status == MatrixCellStatus.PASS;
        }

        public String errorMessage() {
            return errorMessage;
        }

        public long durationMillis() {
            return durationMillis;
        }

        public int commandCount() {
            return commandCount;
        }
    }

    public static final class TargetSummary {
        private final String targetId;
        private final int passCount;
        private final int failCount;

        TargetSummary(String targetId, int passCount, int failCount) {
            this.targetId = requireText(targetId, "targetId");
            if (passCount < 0 || failCount < 0) {
                throw new IllegalArgumentException("pass/fail counts must be >= 0");
            }
            this.passCount = passCount;
            this.failCount = failCount;
        }

        public String targetId() {
            return targetId;
        }

        public int passCount() {
            return passCount;
        }

        public int failCount() {
            return failCount;
        }

        public int totalCells() {
            return passCount + failCount;
        }

        public double passRate() {
            return ratio(passCount, totalCells());
        }
    }

    public static final class SurfaceSummary {
        private final SpringSurface surface;
        private final int passCount;
        private final int failCount;

        SurfaceSummary(SpringSurface surface, int passCount, int failCount) {
            this.surface = Objects.requireNonNull(surface, "surface");
            if (passCount < 0 || failCount < 0) {
                throw new IllegalArgumentException("pass/fail counts must be >= 0");
            }
            this.passCount = passCount;
            this.failCount = failCount;
        }

        public SpringSurface surface() {
            return surface;
        }

        public int passCount() {
            return passCount;
        }

        public int failCount() {
            return failCount;
        }

        public int totalCells() {
            return passCount + failCount;
        }

        public double passRate() {
            return ratio(passCount, totalCells());
        }
    }

    public static final class FailureSample {
        private final String targetId;
        private final String scenarioId;
        private final SpringSurface surface;
        private final String errorMessage;

        FailureSample(String targetId, String scenarioId, SpringSurface surface, String errorMessage) {
            this.targetId = requireText(targetId, "targetId");
            this.scenarioId = requireText(scenarioId, "scenarioId");
            this.surface = Objects.requireNonNull(surface, "surface");
            this.errorMessage = requireText(errorMessage, "errorMessage");
        }

        public String targetId() {
            return targetId;
        }

        public String scenarioId() {
            return scenarioId;
        }

        public SpringSurface surface() {
            return surface;
        }

        public String errorMessage() {
            return errorMessage;
        }
    }

    public static final class MatrixReport {
        private final Instant generatedAt;
        private final String endpointId;
        private final List<SpringProfileTarget> targets;
        private final List<SpringScenario> scenarios;
        private final List<MatrixCellResult> results;
        private final List<SpringScenario> complexScenarios;
        private final List<MatrixCellResult> complexResults;

        MatrixReport(
            Instant generatedAt,
            String endpointId,
            List<SpringProfileTarget> targets,
            List<SpringScenario> scenarios,
            List<MatrixCellResult> results
        ) {
            this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt");
            this.endpointId = requireText(endpointId, "endpointId");
            this.targets = copyTargets(targets);
            this.scenarios = copyScenarios(scenarios);
            this.results = copyResults(this.targets, this.scenarios, results);
            this.complexScenarios = computeComplexScenarios(this.scenarios);
            this.complexResults = computeComplexResults(this.complexScenarios, this.results);
        }

        public Instant generatedAt() {
            return generatedAt;
        }

        public String endpointId() {
            return endpointId;
        }

        public List<SpringProfileTarget> targets() {
            return targets;
        }

        public List<SpringScenario> scenarios() {
            return scenarios;
        }

        public List<MatrixCellResult> results() {
            return results;
        }

        public int totalCells() {
            return results.size();
        }

        public int passCount() {
            int count = 0;
            for (MatrixCellResult result : results) {
                if (result.passed()) {
                    count++;
                }
            }
            return count;
        }

        public int failCount() {
            return totalCells() - passCount();
        }

        public double passRate() {
            return ratio(passCount(), totalCells());
        }

        public List<SpringScenario> complexScenarios() {
            return complexScenarios;
        }

        public List<MatrixCellResult> complexResults() {
            return complexResults;
        }

        public int complexTotalCells() {
            return complexResults().size();
        }

        public int complexPassCount() {
            int pass = 0;
            for (MatrixCellResult result : complexResults()) {
                if (result.passed()) {
                    pass++;
                }
            }
            return pass;
        }

        public int complexFailCount() {
            return complexTotalCells() - complexPassCount();
        }

        public double complexPassRate() {
            return ratio(complexPassCount(), complexTotalCells());
        }

        private static List<SpringScenario> computeComplexScenarios(List<SpringScenario> scenarios) {
            List<SpringScenario> complex = new ArrayList<>();
            for (SpringScenario scenario : scenarios) {
                if (scenario.isComplexQueryScenario()) {
                    complex.add(scenario);
                }
            }
            return List.copyOf(complex);
        }

        private static List<MatrixCellResult> computeComplexResults(
            List<SpringScenario> complexScenarios,
            List<MatrixCellResult> results
        ) {
            Set<String> complexIds = new LinkedHashSet<>();
            for (SpringScenario scenario : complexScenarios) {
                complexIds.add(scenario.id());
            }
            if (complexIds.isEmpty()) {
                return List.of();
            }
            List<MatrixCellResult> complex = new ArrayList<>();
            for (MatrixCellResult result : results) {
                if (complexIds.contains(result.scenarioId())) {
                    complex.add(result);
                }
            }
            return List.copyOf(complex);
        }

        public List<TargetSummary> targetSummaries() {
            List<TargetSummary> summaries = new ArrayList<>(targets.size());
            for (SpringProfileTarget target : targets) {
                int pass = 0;
                int fail = 0;
                for (MatrixCellResult result : results) {
                    if (!target.id().equals(result.targetId())) {
                        continue;
                    }
                    if (result.passed()) {
                        pass++;
                    } else {
                        fail++;
                    }
                }
                summaries.add(new TargetSummary(target.id(), pass, fail));
            }
            return List.copyOf(summaries);
        }

        public List<SurfaceSummary> surfaceSummaries() {
            List<SurfaceSummary> summaries = new ArrayList<>();
            for (SpringSurface surface : SpringSurface.values()) {
                int pass = 0;
                int fail = 0;
                for (MatrixCellResult result : results) {
                    if (result.surface() != surface) {
                        continue;
                    }
                    if (result.passed()) {
                        pass++;
                    } else {
                        fail++;
                    }
                }
                if (pass + fail > 0) {
                    summaries.add(new SurfaceSummary(surface, pass, fail));
                }
            }
            return List.copyOf(summaries);
        }

        public List<FailureSample> failureMinimization(int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be > 0");
            }
            List<FailureSample> failures = new ArrayList<>();
            for (MatrixCellResult result : results) {
                if (result.passed()) {
                    continue;
                }
                failures.add(
                    new FailureSample(
                        result.targetId(),
                        result.scenarioId(),
                        result.surface(),
                        requireText(minimizeFailure(result.errorMessage()), "errorMessage")
                    )
                );
                if (failures.size() == limit) {
                    break;
                }
            }
            return List.copyOf(failures);
        }

        private static List<MatrixCellResult> copyResults(
            List<SpringProfileTarget> targets,
            List<SpringScenario> scenarios,
            List<MatrixCellResult> source
        ) {
            Objects.requireNonNull(source, "results");
            if (source.isEmpty()) {
                throw new IllegalArgumentException("results must not be empty");
            }
            Map<String, MatrixCellResult> byKey = new LinkedHashMap<>();
            List<MatrixCellResult> copy = new ArrayList<>(source.size());
            for (MatrixCellResult result : source) {
                MatrixCellResult nonNullResult = Objects.requireNonNull(result, "result");
                String key = resultKey(nonNullResult.targetId(), nonNullResult.scenarioId());
                if (byKey.put(key, nonNullResult) != null) {
                    throw new IllegalArgumentException("duplicate matrix result key: " + key);
                }
                copy.add(nonNullResult);
            }

            int expectedCells = targets.size() * scenarios.size();
            if (copy.size() != expectedCells) {
                throw new IllegalArgumentException(
                    "results size mismatch: expected " + expectedCells + " but got " + copy.size()
                );
            }

            for (SpringProfileTarget target : targets) {
                for (SpringScenario scenario : scenarios) {
                    String key = resultKey(target.id(), scenario.id());
                    if (!byKey.containsKey(key)) {
                        throw new IllegalArgumentException("missing matrix result key: " + key);
                    }
                }
            }

            return List.copyOf(copy);
        }
    }

    private static double ratio(int numerator, int denominator) {
        if (denominator <= 0) {
            return 0.0d;
        }
        return requireFinite((double) numerator / (double) denominator, "ratio");
    }

    private static final class JsonEncoder {
        private JsonEncoder() {
        }

        static String encode(Object value) {
            StringBuilder sb = new StringBuilder();
            appendValue(sb, value);
            return sb.toString();
        }

        @SuppressWarnings("unchecked")
        private static void appendValue(StringBuilder sb, Object value) {
            if (value == null) {
                sb.append("null");
                return;
            }
            if (value instanceof String s) {
                appendString(sb, s);
                return;
            }
            if (value instanceof Number || value instanceof Boolean) {
                sb.append(value);
                return;
            }
            if (value instanceof Map<?, ?> map) {
                appendObject(sb, (Map<Object, Object>) map);
                return;
            }
            if (value instanceof Collection<?> collection) {
                appendArray(sb, collection);
                return;
            }
            appendString(sb, String.valueOf(value));
        }

        private static void appendObject(StringBuilder sb, Map<Object, Object> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendString(sb, String.valueOf(entry.getKey()));
                sb.append(':');
                appendValue(sb, entry.getValue());
            }
            sb.append('}');
        }

        private static void appendArray(StringBuilder sb, Collection<?> values) {
            sb.append('[');
            boolean first = true;
            for (Object value : values) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendValue(sb, value);
            }
            sb.append(']');
        }

        private static void appendString(StringBuilder sb, String value) {
            sb.append('"');
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '"' -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\b' -> sb.append("\\b");
                    case '\f' -> sb.append("\\f");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default -> {
                        if (c <= 0x1F) {
                            sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                        } else {
                            sb.append(c);
                        }
                    }
                }
            }
            sb.append('"');
        }
    }
}
