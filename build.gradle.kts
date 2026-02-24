import org.gradle.api.publish.maven.MavenPublication

plugins {
    `java-library`
    id("maven-publish")
    id("org.jreleaser") version "1.22.0"
}

group = providers.gradleProperty("publishGroup").orElse("io.github.midagedev").get()
version = providers.gradleProperty("publishVersion").orElse("0.1.0-SNAPSHOT").get()

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

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
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
                name.set(providers.gradleProperty("pomName").orElse("jongodb"))
                description.set(
                    providers.gradleProperty("pomDescription")
                        .orElse("In-memory MongoDB-compatible command engine for integration testing.")
                )
                url.set(providers.gradleProperty("pomUrl").orElse("https://github.com/midagedev/jongodb"))

                licenses {
                    license {
                        name.set(providers.gradleProperty("pomLicenseName").orElse("Apache-2.0"))
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
                    }
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
