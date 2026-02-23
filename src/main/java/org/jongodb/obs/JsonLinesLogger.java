package org.jongodb.obs;

import java.util.Collections;
import java.util.Map;

/**
 * Minimal structured logger that writes one JSON object per line.
 */
public interface JsonLinesLogger extends AutoCloseable {
    void log(String level, String message, CorrelationContext correlationContext, Map<String, ?> fields);

    default void log(String level, String message, CorrelationContext correlationContext) {
        log(level, message, correlationContext, Collections.emptyMap());
    }

    default void info(String message, CorrelationContext correlationContext, Map<String, ?> fields) {
        log("INFO", message, correlationContext, fields);
    }

    default void info(String message, CorrelationContext correlationContext) {
        info(message, correlationContext, Collections.emptyMap());
    }

    default void error(String message, CorrelationContext correlationContext, Map<String, ?> fields) {
        log("ERROR", message, correlationContext, fields);
    }

    default void error(String message, CorrelationContext correlationContext) {
        error(message, correlationContext, Collections.emptyMap());
    }

    @Override
    void close();
}
