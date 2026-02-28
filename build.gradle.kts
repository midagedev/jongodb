import org.gradle.api.publish.maven.MavenPublication

plugins {
    `java-library`
    id("maven-publish")
    id("org.jreleaser") version "1.22.0"
}

group = providers.gradleProperty("publishGroup").orElse("io.github.midagedev").get()
version = providers.gradleProperty("publishVersion").orElse("0.1.3-SNAPSHOT").get()

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

sourceSets {
    named("main") {
        java.srcDir("testkit/spring-suite/src/main/java")
    }
    named("test") {
        java.srcDir("testkit/spring-suite/src/test/java")
    }
}

dependencies {
    api("org.mongodb:bson:4.11.2")
    implementation("org.mongodb:mongodb-driver-sync:4.11.2")
    implementation("org.yaml:snakeyaml:2.2")
    compileOnly("org.springframework:spring-context:6.1.17")
    compileOnly("org.springframework:spring-test:6.1.17")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.springframework:spring-context:6.1.17")
    testImplementation("org.springframework:spring-test:6.1.17")
}

jreleaser {
    configFile.set(layout.projectDirectory.file("jreleaser.yml"))
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = providers.gradleProperty("publishArtifactId").orElse("jongodb").get()

            pom {
                name.set(
                    providers.gradleProperty("pomName")
                        .orElse("jongodb: in-memory MongoDB-compatible test backend")
                )
                description.set(
                    providers.gradleProperty("pomDescription")
                        .orElse(
                            "In-memory MongoDB-compatible backend for Spring Boot and Java integration tests. " +
                                    "Runs in-process without Docker/Testcontainers and targets deterministic test environments."
                        )
                )
                url.set(providers.gradleProperty("pomUrl").orElse("https://github.com/midagedev/jongodb"))
                inceptionYear.set(providers.gradleProperty("pomInceptionYear").orElse("2026"))

                organization {
                    name.set(providers.gradleProperty("pomOrganizationName").orElse("midagedev"))
                    url.set(providers.gradleProperty("pomOrganizationUrl").orElse("https://github.com/midagedev"))
                }

                licenses {
                    license {
                        name.set(
                            providers.gradleProperty("pomLicenseName")
                                .orElse("The Apache License, Version 2.0")
                        )
                        url.set(
                            providers.gradleProperty("pomLicenseUrl")
                                .orElse("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        )
                        distribution.set("repo")
                    }
                }

                developers {
                    developer {
                        id.set(providers.gradleProperty("pomDeveloperId").orElse("hckim"))
                        name.set(providers.gradleProperty("pomDeveloperName").orElse("hckim"))
                        url.set(providers.gradleProperty("pomDeveloperUrl").orElse("https://github.com/midagedev"))
                    }
                }

                issueManagement {
                    system.set(providers.gradleProperty("pomIssueManagementSystem").orElse("GitHub Issues"))
                    url.set(
                        providers.gradleProperty("pomIssueManagementUrl")
                            .orElse("https://github.com/midagedev/jongodb/issues")
                    )
                }

                ciManagement {
                    system.set(providers.gradleProperty("pomCiManagementSystem").orElse("GitHub Actions"))
                    url.set(
                        providers.gradleProperty("pomCiManagementUrl")
                            .orElse("https://github.com/midagedev/jongodb/actions")
                    )
                }

                scm {
                    connection.set(
                        providers.gradleProperty("pomScmConnection")
                            .orElse("scm:git:https://github.com/midagedev/jongodb.git")
                    )
                    developerConnection.set(
                        providers.gradleProperty("pomScmDeveloperConnection")
                            .orElse("scm:git:ssh://git@github.com/midagedev/jongodb.git")
                    )
                    url.set(providers.gradleProperty("pomScmUrl").orElse("https://github.com/midagedev/jongodb"))
                    tag.set(providers.gradleProperty("pomScmTag").orElse("HEAD"))
                }
            }
        }
    }

    repositories {
        maven {
            name = "staging"
            url = layout.buildDirectory.dir("staging-deploy").get().asFile.toURI()
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.named("jreleaserDeploy") {
    dependsOn("publishAllPublicationsToStagingRepository")
}

tasks.register("centralRelease") {
    group = "publishing"
    description = "Stages Maven artifacts and deploys to Maven Central via JReleaser."
    dependsOn("jreleaserDeploy")
}

tasks.register<JavaExec>("m3GateEvidence") {
    group = "verification"
    description = "Runs M3 release-readiness gate automation and writes JSON/MD artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.M3GateAutomation")

    val outputDir = (findProperty("m3OutputDir") as String?) ?: "build/reports/m3-gate"
    val flakeRuns = (findProperty("m3FlakeRuns") as String?) ?: "30"
    val reproSamples = (findProperty("m3ReproSamples") as String?) ?: "21"
    val failOnGate = (findProperty("m3FailOnGate") as String?)?.toBoolean() ?: true

    args(
        "--output-dir=$outputDir",
        "--flake-runs=$flakeRuns",
        "--repro-samples=$reproSamples",
        if (failOnGate) "--fail-on-gate" else "--no-fail-on-gate"
    )
}

tasks.register<JavaExec>("r1PerformanceStabilityGateEvidence") {
    group = "verification"
    description = "Runs R1 performance/stability gates and writes JSON/MD artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.R1PerformanceStabilityGateAutomation")

    val outputDir = (findProperty("r1OutputDir") as String?) ?: "build/reports/r1-gates"
    val flakeRuns = (findProperty("r1FlakeRuns") as String?) ?: "20"
    val coldStartSamples = (findProperty("r1ColdStartSamples") as String?) ?: "21"
    val resetSamples = (findProperty("r1ResetSamples") as String?) ?: "21"
    val warmupOps = (findProperty("r1WarmupOps") as String?) ?: "100"
    val measuredOps = (findProperty("r1MeasuredOps") as String?) ?: "500"
    val failOnGate = (findProperty("r1FailOnGate") as String?)?.toBoolean() ?: true

    args(
        "--output-dir=$outputDir",
        "--flake-runs=$flakeRuns",
        "--cold-start-samples=$coldStartSamples",
        "--reset-samples=$resetSamples",
        "--warmup-ops=$warmupOps",
        "--measured-ops=$measuredOps",
        if (failOnGate) "--fail-on-gate" else "--no-fail-on-gate"
    )
}

tasks.register<JavaExec>("realMongodDifferentialBaseline") {
    group = "verification"
    description = "Runs differential corpus against wire backend vs real mongod and writes JSON/MD artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.RealMongodCorpusRunner")

    val outputDir = (findProperty("realMongodOutputDir") as String?) ?: "build/reports/real-mongod-baseline"
    val seed = (findProperty("realMongodSeed") as String?) ?: "wire-vs-real-mongod-baseline-v1"
    val scenarioCount = (findProperty("realMongodScenarioCount") as String?) ?: "2000"
    val topRegressions = (findProperty("realMongodTopRegressions") as String?) ?: "10"
    val mongoUri = (findProperty("realMongodUri") as String?) ?: (System.getenv("JONGODB_REAL_MONGOD_URI") ?: "")

    args(
        "--output-dir=$outputDir",
        "--seed=$seed",
        "--scenario-count=$scenarioCount",
        "--top-regressions=$topRegressions"
    )
    if (mongoUri.isNotBlank()) {
        args("--mongo-uri=$mongoUri")
    }
}

tasks.register<JavaExec>("springCompatibilityMatrixEvidence") {
    group = "verification"
    description = "Runs Spring Data Mongo compatibility matrix against jongodb endpoint and writes JSON/MD artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.springsuite.SpringCompatibilityMatrixRunner")

    val outputDir = (findProperty("springMatrixOutputDir") as String?) ?: "build/reports/spring-matrix"
    val targets = (findProperty("springMatrixTargets") as String?) ?: ""
    val failOnFailures = (findProperty("springMatrixFailOnFailures") as String?)?.toBoolean() ?: true

    args("--output-dir=$outputDir")
    if (targets.isNotBlank()) {
        args("--targets=$targets")
    }
    args(if (failOnFailures) "--fail-on-failures" else "--no-fail-on-failures")
}

tasks.register<JavaExec>("utfCorpusEvidence") {
    group = "verification"
    description = "Runs Unified Test Format corpus through differential harness and writes JSON/MD artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.UnifiedSpecCorpusRunner")

    val specRoot = (findProperty("utfSpecRoot") as String?) ?: "testkit/specs/unified"
    val outputDir = (findProperty("utfOutputDir") as String?) ?: "build/reports/unified-spec"
    val seed = (findProperty("utfSeed") as String?) ?: "utf-corpus-v1"
    val replayLimit = (findProperty("utfReplayLimit") as String?) ?: "20"
    val mongoUri = (findProperty("utfMongoUri") as String?) ?: (System.getenv("JONGODB_REAL_MONGOD_URI") ?: "")

    args(
        "--spec-root=$specRoot",
        "--output-dir=$outputDir",
        "--seed=$seed",
        "--replay-limit=$replayLimit"
    )
    if (mongoUri.isNotBlank()) {
        args("--mongo-uri=$mongoUri")
    }
}

tasks.register<JavaExec>("fixtureManifestPlan") {
    group = "verification"
    description = "Validates fixture manifest and renders deterministic profile extraction plan."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.FixtureManifestTool")

    val manifestPath = (findProperty("fixtureManifestPath") as String?)?.trim().orEmpty()
    val profile = (findProperty("fixtureProfile") as String?)?.trim().orEmpty().ifBlank { "dev" }
    val renderJson = (findProperty("fixturePlanJson") as String?)?.toBoolean() ?: false

    doFirst {
        if (manifestPath.isBlank()) {
            throw GradleException("fixtureManifestPath property is required")
        }
    }

    args("--manifest=$manifestPath", "--profile=$profile")
    if (renderJson) {
        args("--json")
    }
}

tasks.register<JavaExec>("inProcessTemplatePocEvidence") {
    group = "verification"
    description = "Runs in-process vs TCP template PoC benchmark and trace validation, then writes JSON/MD artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.InProcessTemplatePocRunner")

    val outputDir = (findProperty("inProcessPocOutputDir") as String?) ?: "build/reports/in-process-template-poc"
    val seed = (findProperty("inProcessPocSeed") as String?) ?: "in-process-template-poc-v1"
    val coldStartSamples = (findProperty("inProcessPocColdStartSamples") as String?) ?: "7"
    val warmupOps = (findProperty("inProcessPocWarmupOps") as String?) ?: "100"
    val measuredOps = (findProperty("inProcessPocMeasuredOps") as String?) ?: "500"
    val p95Threshold = (findProperty("inProcessPocP95Threshold") as String?) ?: "0.10"
    val throughputThreshold = (findProperty("inProcessPocThroughputThreshold") as String?) ?: "0.10"
    val failOnNoGo = (findProperty("inProcessPocFailOnNoGo") as String?)?.toBoolean() ?: false

    args(
        "--output-dir=$outputDir",
        "--seed=$seed",
        "--cold-start-samples=$coldStartSamples",
        "--warmup-ops=$warmupOps",
        "--measured-ops=$measuredOps",
        "--p95-improvement-threshold=$p95Threshold",
        "--throughput-improvement-threshold=$throughputThreshold"
    )
    if (failOnNoGo) {
        args("--fail-on-no-go")
    }
}

tasks.register<JavaExec>("fixtureExtract") {
    group = "verification"
    description = "Extracts fixture documents from MongoDB using manifest profile with resume/report guardrails."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.FixtureExtractionTool")

    val manifestPath = (findProperty("fixtureManifestPath") as String?)?.trim().orEmpty()
    val profile = (findProperty("fixtureProfile") as String?)?.trim().orEmpty().ifBlank { "dev" }
    val outputDir = (findProperty("fixtureOutputDir") as String?)?.trim().orEmpty()
    val mongoUri = (findProperty("fixtureMongoUri") as String?)?.trim().orEmpty()
    val allowUriAlias = (findProperty("fixtureAllowUriAlias") as String?)?.trim().orEmpty()
    val readPreference = (findProperty("fixtureReadPreference") as String?)?.trim().orEmpty().ifBlank { "secondaryPreferred" }
    val timeoutMs = (findProperty("fixtureTimeoutMs") as String?)?.trim().orEmpty().ifBlank { "30000" }
    val batchSize = (findProperty("fixtureBatchSize") as String?)?.trim().orEmpty().ifBlank { "500" }
    val maxDocs = (findProperty("fixtureMaxDocs") as String?)?.trim().orEmpty()
    val rateLimitMs = (findProperty("fixtureRateLimitMs") as String?)?.trim().orEmpty().ifBlank { "0" }
    val resume = (findProperty("fixtureResume") as String?)?.toBoolean() ?: false
    val readonlyCheck = (findProperty("fixtureReadonlyCheck") as String?)?.trim().orEmpty().ifBlank { "best-effort" }

    doFirst {
        if (manifestPath.isBlank()) {
            throw GradleException("fixtureManifestPath property is required")
        }
        if (outputDir.isBlank()) {
            throw GradleException("fixtureOutputDir property is required")
        }
    }

    args(
        "--manifest=$manifestPath",
        "--profile=$profile",
        "--output-dir=$outputDir",
        "--read-preference=$readPreference",
        "--timeout-ms=$timeoutMs",
        "--batch-size=$batchSize",
        "--rate-limit-ms=$rateLimitMs",
        "--readonly-check=$readonlyCheck"
    )
    if (mongoUri.isNotBlank()) {
        args("--mongo-uri=$mongoUri")
    }
    if (allowUriAlias.isNotBlank()) {
        args("--allow-uri-alias=$allowUriAlias")
    }
    if (maxDocs.isNotBlank()) {
        args("--max-docs=$maxDocs")
    }
    if (resume) {
        args("--resume")
    }
}

tasks.register<JavaExec>("fixtureSanitize") {
    group = "verification"
    description = "Runs deterministic fixture sanitization + pii lint + normalization pipeline."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.FixtureSanitizationTool")

    val inputDir = (findProperty("fixtureSanitizeInputDir") as String?)?.trim().orEmpty()
    val outputDir = (findProperty("fixtureSanitizeOutputDir") as String?)?.trim().orEmpty()
    val policyFile = (findProperty("fixtureSanitizePolicyFile") as String?)?.trim().orEmpty()
    val seed = (findProperty("fixtureSanitizeSeed") as String?)?.trim().orEmpty().ifBlank { "fixture-sanitize-v1" }
    val failOnPii = (findProperty("fixtureSanitizeFailOnPii") as String?)?.toBoolean() ?: true

    doFirst {
        if (inputDir.isBlank()) {
            throw GradleException("fixtureSanitizeInputDir property is required")
        }
        if (outputDir.isBlank()) {
            throw GradleException("fixtureSanitizeOutputDir property is required")
        }
        if (policyFile.isBlank()) {
            throw GradleException("fixtureSanitizePolicyFile property is required")
        }
    }

    args(
        "--input-dir=$inputDir",
        "--output-dir=$outputDir",
        "--policy-file=$policyFile",
        "--seed=$seed"
    )
    args(if (failOnPii) "--fail-on-pii" else "--no-fail-on-pii")
}

tasks.register<JavaExec>("fixtureArtifactPack") {
    group = "verification"
    description = "Packs fixture ndjson into portable + fast dual artifacts with checksum manifest."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.FixtureArtifactTool")

    val inputDir = (findProperty("fixtureArtifactInputDir") as String?)?.trim().orEmpty()
    val outputDir = (findProperty("fixtureArtifactOutputDir") as String?)?.trim().orEmpty()
    val engineVersion = (findProperty("fixtureArtifactEngineVersion") as String?)?.trim().orEmpty()

    doFirst {
        if (inputDir.isBlank()) {
            throw GradleException("fixtureArtifactInputDir property is required")
        }
        if (outputDir.isBlank()) {
            throw GradleException("fixtureArtifactOutputDir property is required")
        }
    }

    args(
        "--input-dir=$inputDir",
        "--output-dir=$outputDir"
    )
    if (engineVersion.isNotBlank()) {
        args("--engine-version=$engineVersion")
    }
}

tasks.register<JavaExec>("fixtureRefresh") {
    group = "verification"
    description = "Runs full/incremental fixture refresh and writes diff report + refreshed ndjson output."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.FixtureRefreshTool")

    val baselineDir = (findProperty("fixtureRefreshBaselineDir") as String?)?.trim().orEmpty()
    val candidateDir = (findProperty("fixtureRefreshCandidateDir") as String?)?.trim().orEmpty()
    val outputDir = (findProperty("fixtureRefreshOutputDir") as String?)?.trim().orEmpty()
    val mode = (findProperty("fixtureRefreshMode") as String?)?.trim().orEmpty().ifBlank { "full" }
    val requireApproval = (findProperty("fixtureRefreshRequireApproval") as String?)?.toBoolean() ?: false
    val approved = (findProperty("fixtureRefreshApproved") as String?)?.toBoolean() ?: false
    val warnThreshold = (findProperty("fixtureRefreshWarnThreshold") as String?)?.trim().orEmpty().ifBlank { "0.15" }
    val failThreshold = (findProperty("fixtureRefreshFailThreshold") as String?)?.trim().orEmpty().ifBlank { "0.30" }
    val failOnThreshold = (findProperty("fixtureRefreshFailOnThreshold") as String?)?.toBoolean() ?: false

    doFirst {
        if (baselineDir.isBlank()) {
            throw GradleException("fixtureRefreshBaselineDir property is required")
        }
        if (candidateDir.isBlank()) {
            throw GradleException("fixtureRefreshCandidateDir property is required")
        }
        if (outputDir.isBlank()) {
            throw GradleException("fixtureRefreshOutputDir property is required")
        }
    }

    args(
        "--baseline-dir=$baselineDir",
        "--candidate-dir=$candidateDir",
        "--output-dir=$outputDir",
        "--mode=$mode",
        "--warn-threshold=$warnThreshold",
        "--fail-threshold=$failThreshold"
    )
    if (requireApproval) {
        args("--require-approval")
    }
    if (approved) {
        args("--approved")
    }
    args(if (failOnThreshold) "--fail-on-threshold" else "--no-fail-on-threshold")
}

tasks.register<JavaExec>("fixtureRestore") {
    group = "verification"
    description = "Restores fixture ndjson into target MongoDB using replace/merge mode with diagnostics report."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.FixtureRestoreTool")

    val inputDir = (findProperty("fixtureRestoreInputDir") as String?)?.trim().orEmpty()
    val mongoUri = (findProperty("fixtureRestoreMongoUri") as String?)?.trim().orEmpty()
    val mode = (findProperty("fixtureRestoreMode") as String?)?.trim().orEmpty().ifBlank { "replace" }
    val database = (findProperty("fixtureRestoreDatabase") as String?)?.trim().orEmpty()
    val namespace = (findProperty("fixtureRestoreNamespace") as String?)?.trim().orEmpty()
    val reportDir = (findProperty("fixtureRestoreReportDir") as String?)?.trim().orEmpty()
    val regenerateFastCache = (findProperty("fixtureRestoreRegenerateFastCache") as String?)?.toBoolean() ?: true

    doFirst {
        if (inputDir.isBlank()) {
            throw GradleException("fixtureRestoreInputDir property is required")
        }
        if (mongoUri.isBlank()) {
            throw GradleException("fixtureRestoreMongoUri property is required")
        }
    }

    args(
        "--input-dir=$inputDir",
        "--mongo-uri=$mongoUri",
        "--mode=$mode"
    )
    if (database.isNotBlank()) {
        args("--database=$database")
    }
    if (namespace.isNotBlank()) {
        args("--namespace=$namespace")
    }
    if (reportDir.isNotBlank()) {
        args("--report-dir=$reportDir")
    }
    args(if (regenerateFastCache) "--regenerate-fast-cache" else "--no-regenerate-fast-cache")
}

tasks.register<JavaExec>("complexQueryCertificationEvidence") {
    group = "verification"
    description = "Runs canonical complex-query certification pack and enforces gate policy."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.ComplexQueryCertificationRunner")

    val outputDir = (findProperty("complexQueryOutputDir") as String?)
        ?: "build/reports/complex-query-certification"
    val seed = (findProperty("complexQuerySeed") as String?) ?: "complex-query-cert-v1"
    val patternLimit = (findProperty("complexQueryPatternLimit") as String?)?.trim()
    val mongoUri = (findProperty("complexQueryMongoUri") as String?)
        ?: (System.getenv("JONGODB_REAL_MONGOD_URI") ?: "")
    val failOnGate = (findProperty("complexQueryFailOnGate") as String?)?.toBoolean() ?: true

    args(
        "--output-dir=$outputDir",
        "--seed=$seed",
        if (failOnGate) "--fail-on-gate" else "--no-fail-on-gate"
    )
    if (!patternLimit.isNullOrBlank()) {
        args("--pattern-limit=$patternLimit")
    }
    if (mongoUri.isNotBlank()) {
        args("--mongo-uri=$mongoUri")
    }
}

tasks.register<JavaExec>("replayFailureBundle") {
    group = "verification"
    description = "Replays one deterministic failure bundle by failure-id."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.DeterministicReplayBundleRunner")

    val bundleDir = (findProperty("replayBundleDir") as String?)
        ?: "build/reports/unified-spec/failure-replay-bundles"
    val failureId = (findProperty("replayFailureId") as String?) ?: ""

    args("--bundle-dir=$bundleDir")
    if (failureId.isNotBlank()) {
        args("--failure-id=$failureId")
    }
}

tasks.register<JavaExec>("finalReadinessEvidence") {
    group = "verification"
    description = "Aggregates R1 release-readiness evidence into a unified JSON/MD report."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.FinalReleaseReadinessAggregator")

    val outputDir = (findProperty("finalReadinessOutputDir") as String?) ?: "build/reports/release-readiness"
    val m3OutputDir = (findProperty("finalReadinessM3OutputDir") as String?) ?: "build/reports/m3-gate"
    val realOutputDir = (findProperty("finalReadinessRealMongodOutputDir") as String?) ?: "build/reports/real-mongod-baseline"
    val r1OutputDir = (findProperty("finalReadinessR1OutputDir") as String?) ?: "build/reports/r1-gates"
    val springMatrixJson = (findProperty("finalReadinessSpringMatrixJson") as String?)
        ?: "build/reports/spring-matrix/spring-compatibility-matrix.json"
    val realMongodUri = (findProperty("finalReadinessRealMongodUri") as String?)
        ?: (System.getenv("JONGODB_REAL_MONGOD_URI") ?: "")
    val generateMissingEvidence = (findProperty("finalReadinessGenerateMissingEvidence") as String?)?.toBoolean() ?: false
    val failOnGate = (findProperty("finalReadinessFailOnGate") as String?)?.toBoolean() ?: true

    args(
        "--output-dir=$outputDir",
        "--m3-output-dir=$m3OutputDir",
        "--real-mongod-output-dir=$realOutputDir",
        "--r1-output-dir=$r1OutputDir",
        "--spring-matrix-json=$springMatrixJson"
    )
    if (realMongodUri.isNotBlank()) {
        args("--real-mongod-uri=$realMongodUri")
    }
    args(if (generateMissingEvidence) "--generate-missing-evidence" else "--no-generate-missing-evidence")
    args(if (failOnGate) "--fail-on-gate" else "--no-fail-on-gate")
}

tasks.register<JavaExec>("r2CompatibilityEvidence") {
    group = "verification"
    description = "Generates R2 compatibility scorecard and support manifest artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.R2CompatibilityScorecard")

    val outputDir = (findProperty("r2CompatibilityOutputDir") as String?) ?: "build/reports/r2-compatibility"
    val utfReport = (findProperty("r2CompatibilityUtfReport") as String?) ?: "build/reports/unified-spec/utf-differential-report.json"
    val springMatrixJson = (findProperty("r2CompatibilitySpringMatrixJson") as String?)
        ?: "build/reports/spring-matrix/spring-compatibility-matrix.json"
    val failOnGate = (findProperty("r2CompatibilityFailOnGate") as String?)?.toBoolean() ?: true

    args(
        "--output-dir=$outputDir",
        "--utf-report=$utfReport",
        "--spring-matrix-json=$springMatrixJson",
        if (failOnGate) "--fail-on-gate" else "--no-fail-on-gate"
    )
}

tasks.register("printLauncherClasspath") {
    group = "help"
    description = "Prints the runtime classpath required for TcpMongoServerLauncher."
    dependsOn("classes")
    doLast {
        println(sourceSets["main"].runtimeClasspath.asPath)
    }
}

tasks.register<JavaExec>("r2CanaryCertificationEvidence") {
    group = "verification"
    description = "Generates R2 canary certification artifacts from project canary results."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.R2CanaryCertification")

    val inputJson = (findProperty("r2CanaryInputJson") as String?) ?: "build/reports/spring-canary/projects.json"
    val outputDir = (findProperty("r2CanaryOutputDir") as String?) ?: "build/reports/r2-canary"
    val failOnGate = (findProperty("r2CanaryFailOnGate") as String?)?.toBoolean() ?: true

    args(
        "--input-json=$inputJson",
        "--output-dir=$outputDir",
        if (failOnGate) "--fail-on-gate" else "--no-fail-on-gate"
    )
}

tasks.register<JavaExec>("r3CanaryCertificationEvidence") {
    group = "verification"
    description = "Generates R3 external canary certification artifacts from canary result JSON."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.R2CanaryCertification")

    val inputJson = (findProperty("r3CanaryInputJson") as String?)
        ?: "build/reports/spring-canary/r3-projects.json"
    val outputDir = (findProperty("r3CanaryOutputDir") as String?) ?: "build/reports/r3-canary"
    val failOnGate = (findProperty("r3CanaryFailOnGate") as String?)?.toBoolean() ?: true

    args(
        "--input-json=$inputJson",
        "--output-dir=$outputDir",
        if (failOnGate) "--fail-on-gate" else "--no-fail-on-gate"
    )
}

tasks.register<JavaExec>("r3FailureLedger") {
    group = "verification"
    description = "Generates deterministic R3 failure-ledger artifacts from official suite runs."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.R3FailureLedgerRunner")

    val specRepoRoot = (findProperty("r3SpecRepoRoot") as String?)
        ?: "third_party/mongodb-specs/.checkout/specifications"
    val outputDir = (findProperty("r3FailureLedgerOutputDir") as String?)
        ?: "build/reports/r3-failure-ledger"
    val seed = (findProperty("r3FailureLedgerSeed") as String?) ?: "r3-failure-ledger-v1"
    val replayLimit = (findProperty("r3FailureLedgerReplayLimit") as String?) ?: "20"
    val mongoUri = (findProperty("r3FailureLedgerMongoUri") as String?)
        ?: (System.getenv("JONGODB_REAL_MONGOD_URI") ?: "")
    val failOnFailures = (findProperty("r3FailureLedgerFailOnFailures") as String?)?.toBoolean() ?: false

    args(
        "--spec-repo-root=$specRepoRoot",
        "--output-dir=$outputDir",
        "--seed=$seed",
        "--replay-limit=$replayLimit",
        if (failOnFailures) "--fail-on-failures" else "--no-fail-on-failures"
    )
    if (mongoUri.isNotBlank()) {
        args("--mongo-uri=$mongoUri")
    }
}
