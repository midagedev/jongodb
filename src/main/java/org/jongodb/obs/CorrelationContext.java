package org.jongodb.obs;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Correlation metadata emitted with every structured log event.
 */
public final class CorrelationContext {
    private final String requestId;
    private final String commandName;
    private final String sessionId;
    private final Long txnNumber;

    private CorrelationContext(Builder builder) {
        this.requestId = requireText(builder.requestId, "requestId");
        this.commandName = requireText(builder.commandName, "commandName");
        this.sessionId = normalize(builder.sessionId);
        this.txnNumber = builder.txnNumber;
    }

    public static CorrelationContext of(String requestId, String commandName) {
        return builder(requestId, commandName).build();
    }

    public static Builder builder(String requestId, String commandName) {
        return new Builder(requestId, commandName);
    }

    public String requestId() {
        return requestId;
    }

    public String commandName() {
        return commandName;
    }

    public Optional<String> sessionId() {
        return Optional.ofNullable(sessionId);
    }

    public Optional<Long> txnNumber() {
        return Optional.ofNullable(txnNumber);
    }

    public Map<String, Object> asFields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("requestId", requestId);
        fields.put("commandName", commandName);
        if (sessionId != null) {
            fields.put("sessionId", sessionId);
        }
        if (txnNumber != null) {
            fields.put("txnNumber", txnNumber);
        }
        return fields;
    }

    private static String requireText(String value, String fieldName) {
        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public static final class Builder {
        private final String requestId;
        private final String commandName;
        private String sessionId;
        private Long txnNumber;

        private Builder(String requestId, String commandName) {
            this.requestId = Objects.requireNonNull(requestId, "requestId");
            this.commandName = Objects.requireNonNull(commandName, "commandName");
        }

        public Builder sessionId(String sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder txnNumber(Long txnNumber) {
            this.txnNumber = txnNumber;
            return this;
        }

        public CorrelationContext build() {
            return new CorrelationContext(this);
        }
    }
}
