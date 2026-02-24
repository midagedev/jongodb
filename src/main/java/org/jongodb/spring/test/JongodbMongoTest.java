package org.jongodb.spring.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.core.annotation.AliasFor;
import org.springframework.test.context.ContextConfiguration;

/**
 * Single-hook Spring test integration for jongodb.
 *
 * <p>Use with {@code @SpringBootTest} (or other Spring TestContext entry points) to bootstrap an
 * in-memory MongoDB endpoint without custom initializer boilerplate.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@ContextConfiguration(initializers = JongodbMongoInitializer.class)
public @interface JongodbMongoTest {
    @AliasFor(annotation = ContextConfiguration.class, attribute = "classes")
    Class<?>[] classes() default {};
}
