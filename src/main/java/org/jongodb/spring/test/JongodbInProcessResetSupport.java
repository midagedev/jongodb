package org.jongodb.spring.test;

import java.util.Objects;
import org.springframework.context.ApplicationContext;

/**
 * Reset helper for Spring contexts using {@link JongodbInProcessInitializer}.
 */
public final class JongodbInProcessResetSupport {
    private JongodbInProcessResetSupport() {}

    public static void reset(final ApplicationContext context) {
        Objects.requireNonNull(context, "context");
        final InProcessIntegrationTemplate template =
                context.getBean(JongodbInProcessInitializer.TEMPLATE_BEAN_NAME, InProcessIntegrationTemplate.class);
        template.reset();
    }
}
