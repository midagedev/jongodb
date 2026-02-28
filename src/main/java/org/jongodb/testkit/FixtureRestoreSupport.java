package org.jongodb.testkit;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Convenience hook for test frameworks to reset/restore fixtures before each test.
 */
public final class FixtureRestoreSupport {
    private FixtureRestoreSupport() {}

    public static void beforeEachReplace(final String mongoUri, final Path fixtureDir) {
        beforeEach(mongoUri, fixtureDir, FixtureRestoreTool.RestoreMode.REPLACE);
    }

    public static void beforeEach(
            final String mongoUri,
            final Path fixtureDir,
            final FixtureRestoreTool.RestoreMode mode) {
        Objects.requireNonNull(mongoUri, "mongoUri");
        Objects.requireNonNull(fixtureDir, "fixtureDir");
        Objects.requireNonNull(mode, "mode");
        FixtureRestoreTool.resetAndRestore(mongoUri, fixtureDir, mode);
    }
}
