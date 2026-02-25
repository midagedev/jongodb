package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Static validator for {@link FixtureManifest}.
 */
public final class FixtureManifestValidator {
    private static final Pattern URI_ALIAS_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    private static final Pattern FIELD_PATH_PATTERN = Pattern.compile("^[a-zA-Z0-9_.$]+$");

    private FixtureManifestValidator() {}

    public static void validateOrThrow(final FixtureManifest manifest) {
        final List<String> errors = validate(manifest);
        if (!errors.isEmpty()) {
            throw new FixtureManifestValidationException(errors);
        }
    }

    public static List<String> validate(final FixtureManifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        final List<String> errors = new ArrayList<>();

        if (!FixtureManifest.SCHEMA_VERSION.equals(manifest.schemaVersion())) {
            errors.add("schemaVersion must be '" + FixtureManifest.SCHEMA_VERSION
                    + "' (actual: " + manifest.schemaVersion() + ")");
        }
        validateSource(manifest.source(), errors);
        validateProfiles(manifest, errors);
        return List.copyOf(errors);
    }

    private static void validateSource(final FixtureManifest.Source source, final List<String> errors) {
        if (source.uriAlias().length() < 2) {
            errors.add("source.uriAlias must be at least 2 characters");
        }
        if (!URI_ALIAS_PATTERN.matcher(source.uriAlias()).matches()) {
            errors.add("source.uriAlias may contain only letters, numbers, dot, underscore, and hyphen");
        }
    }

    private static void validateProfiles(final FixtureManifest manifest, final List<String> errors) {
        final EnumSet<FixtureManifest.ScenarioProfile> expected = EnumSet.allOf(FixtureManifest.ScenarioProfile.class);
        final Set<FixtureManifest.ScenarioProfile> provided = manifest.profiles().keySet();
        for (final FixtureManifest.ScenarioProfile profile : expected) {
            if (!provided.contains(profile)) {
                errors.add("profiles." + profile.value() + " is required");
            }
        }

        for (final FixtureManifest.ScenarioProfile profile : provided) {
            final FixtureManifest.ProfileConfig config = manifest.profiles().get(profile);
            final String path = "profiles." + profile.value();
            if (config == null) {
                errors.add(path + " must not be null");
                continue;
            }
            validateProfile(path, config, errors);
        }
    }

    private static void validateProfile(
            final String path,
            final FixtureManifest.ProfileConfig profileConfig,
            final List<String> errors) {
        if (profileConfig.collections().isEmpty()) {
            errors.add(path + ".collections must not be empty");
        }
        validateFieldRules(path + ".fieldRules", profileConfig.fieldRules(), errors);

        final Set<String> seenCollections = new HashSet<>();
        for (int i = 0; i < profileConfig.collections().size(); i++) {
            final FixtureManifest.CollectionRule collection = profileConfig.collections().get(i);
            final String collectionPath = path + ".collections[" + i + "]";
            final String key = collection.key().toLowerCase(Locale.ROOT);
            if (!seenCollections.add(key)) {
                errors.add(collectionPath + " duplicates collection '" + collection.key() + "'");
            }
            validateCollection(collectionPath, profileConfig.refreshMode(), collection, errors);
        }
    }

    private static void validateCollection(
            final String path,
            final FixtureManifest.RefreshMode refreshMode,
            final FixtureManifest.CollectionRule collection,
            final List<String> errors) {
        if (collection.limit() != null && collection.limit() <= 0) {
            errors.add(path + ".limit must be > 0 when provided");
        }
        if (collection.limit() != null && collection.sort().isEmpty()) {
            errors.add(path + ".sort is required when limit is set (deterministic top-N extraction)");
        }
        if (collection.sample() != null) {
            if (collection.sample().size() <= 0) {
                errors.add(path + ".sample.size must be > 0");
            }
            if (collection.sample().seed().isBlank()) {
                errors.add(path + ".sample.seed must not be blank");
            }
            if (collection.sort().isEmpty()) {
                errors.add(path + ".sort is required when sample is set (deterministic sample ordering)");
            }
        }
        if (refreshMode == FixtureManifest.RefreshMode.INCREMENTAL && collection.sort().isEmpty()) {
            errors.add(path + ".sort is required for incremental refresh mode");
        }
        validateSort(path + ".sort", collection.sort(), errors);
        validateFieldRules(path + ".fieldRules", collection.fieldRules(), errors);
    }

    private static void validateSort(
            final String path,
            final java.util.Map<String, Object> sort,
            final List<String> errors) {
        for (final java.util.Map.Entry<String, Object> entry : sort.entrySet()) {
            final String fieldPath = entry.getKey();
            if (!isValidFieldPath(fieldPath)) {
                errors.add(path + "." + fieldPath + " is not a valid field path");
            }
            final Object value = entry.getValue();
            if (!(value instanceof Number number)) {
                errors.add(path + "." + fieldPath + " sort direction must be numeric 1 or -1");
                continue;
            }
            final int direction = number.intValue();
            if (direction != 1 && direction != -1) {
                errors.add(path + "." + fieldPath + " sort direction must be 1 or -1");
            }
        }
    }

    private static void validateFieldRules(
            final String path,
            final FixtureManifest.FieldRules rules,
            final List<String> errors) {
        final Set<String> include = new HashSet<>();
        for (final String field : rules.include()) {
            if (!isValidFieldPath(field)) {
                errors.add(path + ".include contains invalid field path '" + field + "'");
            }
            include.add(field);
        }

        final Set<String> overlap = new HashSet<>();
        for (final String field : rules.exclude()) {
            if (!isValidFieldPath(field)) {
                errors.add(path + ".exclude contains invalid field path '" + field + "'");
            }
            if (include.contains(field)) {
                overlap.add(field);
            }
        }
        if (!overlap.isEmpty()) {
            errors.add(path + " include/exclude overlap is not allowed: " + overlap);
        }
    }

    private static boolean isValidFieldPath(final String fieldPath) {
        if (fieldPath == null || fieldPath.isBlank()) {
            return false;
        }
        if (!FIELD_PATH_PATTERN.matcher(fieldPath).matches()) {
            return false;
        }
        return !fieldPath.startsWith(".")
                && !fieldPath.endsWith(".")
                && !fieldPath.contains("..");
    }
}
