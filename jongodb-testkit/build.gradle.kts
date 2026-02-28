plugins {
    `java-library`
}

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

sourceSets {
    named("main") {
        java.setSrcDirs(listOf("../src/main/java"))
        java.include("org/jongodb/testkit/**")
    }
    named("test") {
        java.setSrcDirs(listOf("../src/test/java"))
        java.include("org/jongodb/testkit/**")
    }
}

dependencies {
    implementation(project(":"))
    implementation("org.mongodb:mongodb-driver-sync:4.11.2")
    implementation("org.yaml:snakeyaml:2.2")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    filter {
        isFailOnNoMatchingTests = false
    }
    workingDir = rootProject.projectDir
}
