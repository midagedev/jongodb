package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WireCommandIngressStartupTest {
    @Test
    void inMemoryBootstrapIsUnderFiveHundredMilliseconds() {
        final long startedAt = System.nanoTime();
        final WireCommandIngress ingress = WireCommandIngress.inMemory();
        final long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

        assertTrue(elapsedMillis <= 500, "startup took " + elapsedMillis + "ms");
        // Keep a reference used, avoiding accidental elimination in future refactors.
        assertTrue(ingress != null);
    }
}

