package org.jongodb.spring.test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jongodb.testkit.InProcessIntegrationTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.env.MapPropertySource;

/**
 * Spring initializer that registers {@link InProcessIntegrationTemplate} bean for command-level integration tests.
 */
public final class JongodbInProcessInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    public static final String TEMPLATE_BEAN_NAME = "jongodbInProcessTemplate";
    public static final String TRACE_ARTIFACTS_ENABLED_PROPERTY = "jongodb.inprocess.traceArtifacts.enabled";
    public static final String TRACE_ARTIFACTS_DIR_PROPERTY = "jongodb.inprocess.traceArtifacts.dir";
    public static final String DEFAULT_TRACE_ARTIFACTS_DIR = "build/reports/in-process-template/traces";

    @Override
    public void initialize(final ConfigurableApplicationContext context) {
        Objects.requireNonNull(context, "context");
        final InProcessIntegrationTemplate template = new InProcessIntegrationTemplate();
        configureTraceArtifacts(context, template);

        final Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("jongodb.inprocess.enabled", "true");
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("jongodbInProcessTemplate", properties));
        context.getBeanFactory().registerSingleton(TEMPLATE_BEAN_NAME, template);
        context.addApplicationListener(event -> {
            if (event instanceof ContextClosedEvent) {
                template.disableFailureTraceArtifacts();
                template.reset();
            }
        });
    }

    private static void configureTraceArtifacts(
            final ConfigurableApplicationContext context,
            final InProcessIntegrationTemplate template) {
        final boolean enabled = Boolean.parseBoolean(
                context.getEnvironment().getProperty(TRACE_ARTIFACTS_ENABLED_PROPERTY, "false"));
        if (!enabled) {
            return;
        }
        final String path = context.getEnvironment().getProperty(TRACE_ARTIFACTS_DIR_PROPERTY, DEFAULT_TRACE_ARTIFACTS_DIR);
        template.enableFailureTraceArtifacts(Path.of(path));
    }
}
