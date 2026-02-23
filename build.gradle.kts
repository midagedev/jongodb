plugins {
    java
}

group = "org.jongodb"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

dependencies {
    implementation("org.mongodb:bson:4.11.2")
    implementation("org.mongodb:mongodb-driver-sync:4.11.2")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
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

tasks.register<JavaExec>("realMongodDifferentialBaseline") {
    group = "verification"
    description = "Runs differential corpus against wire backend vs real mongod and writes JSON/MD artifacts."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.jongodb.testkit.RealMongodCorpusRunner")

    val outputDir = (findProperty("realMongodOutputDir") as String?) ?: "build/reports/real-mongod-baseline"
    val seed = (findProperty("realMongodSeed") as String?) ?: "wire-vs-real-mongod-baseline-v1"
    val topRegressions = (findProperty("realMongodTopRegressions") as String?) ?: "10"
    val mongoUri = (findProperty("realMongodUri") as String?) ?: (System.getenv("JONGODB_REAL_MONGOD_URI") ?: "")

    args(
        "--output-dir=$outputDir",
        "--seed=$seed",
        "--top-regressions=$topRegressions"
    )
    if (mongoUri.isNotBlank()) {
        args("--mongo-uri=$mongoUri")
    }
}
