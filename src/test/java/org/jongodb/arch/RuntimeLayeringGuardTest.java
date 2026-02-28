package org.jongodb.arch;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = "org.jongodb", importOptions = ImportOption.DoNotIncludeTests.class)
class RuntimeLayeringGuardTest {
    @ArchTest
    static final ArchRule runtime_does_not_depend_on_testkit = noClasses()
            .that()
            .resideInAnyPackage(
                    "org.jongodb.command..",
                    "org.jongodb.engine..",
                    "org.jongodb.obs..",
                    "org.jongodb.server..",
                    "org.jongodb.spring..",
                    "org.jongodb.txn..",
                    "org.jongodb.wire..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("org.jongodb.testkit..", "org.jongodb.testkit.springsuite..");
}
