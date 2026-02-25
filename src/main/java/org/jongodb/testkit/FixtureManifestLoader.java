package org.jongodb.testkit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads fixture manifest files (JSON or YAML) and validates schema constraints.
 */
public final class FixtureManifestLoader {
    private FixtureManifestLoader() {}

    public static FixtureManifest load(final Path manifestPath) throws IOException {
        Objects.requireNonNull(manifestPath, "manifestPath");
        final Path normalized = manifestPath.toAbsolutePath().normalize();
        if (!Files.exists(normalized)) {
            throw new IllegalArgumentException("manifest path does not exist: " + normalized);
        }
        if (!Files.isRegularFile(normalized)) {
            throw new IllegalArgumentException("manifest path must be a file: " + normalized);
        }

        final String content = Files.readString(normalized, StandardCharsets.UTF_8);
        final FixtureManifest manifest = parse(content, normalized.getFileName().toString());
        FixtureManifestValidator.validateOrThrow(manifest);
        return manifest;
    }

    static FixtureManifest parse(final String content, final String sourceName) {
        Objects.requireNonNull(content, "content");
        final String normalizedName = Objects.requireNonNull(sourceName, "sourceName")
                .trim()
                .toLowerCase(Locale.ROOT);
        if (normalizedName.endsWith(".yaml") || normalizedName.endsWith(".yml")) {
            return parseYaml(content);
        }
        return FixtureManifest.fromJson(content);
    }

    private static FixtureManifest parseYaml(final String content) {
        final Object root = new Yaml().load(content);
        if (root == null) {
            throw new IllegalArgumentException("manifest is empty");
        }
        if (!(root instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException("manifest root must be an object");
        }
        final Map<String, Object> normalized = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : rawMap.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return FixtureManifest.fromMap(normalized);
    }
}
