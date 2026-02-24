package org.jongodb.command;

import java.util.Locale;

public enum TopologyProfile {
    STANDALONE("standalone"),
    SINGLE_NODE_REPLICA_SET("singleNodeReplicaSet");

    private final String cliValue;

    TopologyProfile(final String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public boolean replicaSetSemanticsEnabled() {
        return this == SINGLE_NODE_REPLICA_SET;
    }

    public static TopologyProfile parse(final String rawValue) {
        final String normalized = rawValue == null ? "" : rawValue.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty() || "standalone".equals(normalized)) {
            return STANDALONE;
        }
        if ("singlenodereplicaset".equals(normalized)
                || "single_node_replicaset".equals(normalized)
                || "replicaset".equals(normalized)) {
            return SINGLE_NODE_REPLICA_SET;
        }
        throw new IllegalArgumentException("unsupported topology profile: " + rawValue);
    }
}
