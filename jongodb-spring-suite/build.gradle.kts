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
        java.setSrcDirs(listOf("../testkit/spring-suite/src/main/java"))
    }
    named("test") {
        java.setSrcDirs(listOf("../testkit/spring-suite/src/test/java"))
    }
}

dependencies {
    implementation(project(":jongodb-testkit"))

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
