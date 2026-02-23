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
