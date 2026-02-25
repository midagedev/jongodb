package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import org.bson.Document;

/**
 * Declarative fixture extraction manifest model.
 */
public record FixtureManifest(
        String schemaVersion,
        Source source,
        Map<ScenarioProfile, ProfileConfig> profiles) {
    public static final String SCHEMA_VERSION = "fixture-manifest.v1";

    public FixtureManifest {
        schemaVersion = requireText(schemaVersion, "schemaVersion");
        source = Objects.requireNonNull(source, "source");
        profiles = unmodifiableMapCopy(Objects.requireNonNull(profiles, "profiles"));
    }

    public static FixtureManifest fromJson(final String json) {
        final Document document = Document.parse(Objects.requireNonNull(json, "json"));
        return fromMap(asStringMap(document, "manifest"));
    }

    public static FixtureManifest fromMap(final Map<String, Object> root) {
        Objects.requireNonNull(root, "root");
        final String schemaVersion = requireText((String) root.get("schemaVersion"), "schemaVersion");
        final Source source = Source.fromMap(asStringMap(root.get("source"), "source"), "source");

        final Map<String, Object> profileRoot = asStringMap(root.get("profiles"), "profiles");
        final Map<ScenarioProfile, ProfileConfig> profiles = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : profileRoot.entrySet()) {
            final ScenarioProfile scenarioProfile = ScenarioProfile.fromText(entry.getKey());
            final String path = "profiles." + scenarioProfile.value();
            final ProfileConfig profileConfig = ProfileConfig.fromMap(
                    scenarioProfile,
                    asStringMap(entry.getValue(), path),
                    path);
            profiles.put(scenarioProfile, profileConfig);
        }

        return new FixtureManifest(schemaVersion, source, profiles);
    }

    public String toJson() {
        return DiffSummaryGenerator.JsonEncoder.encode(toMap());
    }

    public ProfileConfig profile(final ScenarioProfile profile) {
        final ScenarioProfile target = Objects.requireNonNull(profile, "profile");
        final ProfileConfig config = profiles.get(target);
        if (config == null) {
            throw new IllegalArgumentException("profile '" + target.value() + "' is not defined in manifest");
        }
        return config;
    }

    Map<String, Object> toMap() {
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("schemaVersion", schemaVersion);
        root.put("source", source.toMap());

        final Map<String, Object> profileRoot = new LinkedHashMap<>();
        for (final ScenarioProfile profile : ScenarioProfile.values()) {
            final ProfileConfig config = profiles.get(profile);
            if (config != null) {
                profileRoot.put(profile.value(), config.toMap());
            }
        }
        root.put("profiles", profileRoot);
        return root;
    }

    public enum ScenarioProfile {
        DEV("dev"),
        SMOKE("smoke"),
        FULL("full");

        private final String value;

        ScenarioProfile(final String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        static ScenarioProfile fromText(final String rawValue) {
            final String value = requireText(rawValue, "profile").toLowerCase(Locale.ROOT);
            for (final ScenarioProfile profile : values()) {
                if (profile.value.equals(value)) {
                    return profile;
                }
            }
            throw new IllegalArgumentException("unsupported profile: " + rawValue + " (expected: dev|smoke|full)");
        }
    }

    public enum RefreshMode {
        FULL("full"),
        INCREMENTAL("incremental");

        private final String value;

        RefreshMode(final String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        static RefreshMode fromText(final String rawValue, final String fieldName) {
            final String value = requireText(rawValue, fieldName).toLowerCase(Locale.ROOT);
            for (final RefreshMode mode : values()) {
                if (mode.value.equals(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException(
                    fieldName + " must be one of: full|incremental (actual: " + rawValue + ")");
        }
    }

    public record Source(String uriAlias) {
        public Source {
            uriAlias = requireText(uriAlias, "source.uriAlias");
        }

        static Source fromMap(final Map<String, Object> root, final String path) {
            return new Source(requireText((String) root.get("uriAlias"), path + ".uriAlias"));
        }

        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("uriAlias", uriAlias);
            return root;
        }
    }

    public record ProfileConfig(
            ScenarioProfile profile,
            RefreshMode refreshMode,
            FieldRules fieldRules,
            List<CollectionRule> collections) {
        public ProfileConfig {
            profile = Objects.requireNonNull(profile, "profile");
            refreshMode = Objects.requireNonNull(refreshMode, "refreshMode");
            fieldRules = Objects.requireNonNull(fieldRules, "fieldRules");
            collections = List.copyOf(Objects.requireNonNull(collections, "collections"));
        }

        static ProfileConfig fromMap(
                final ScenarioProfile profile,
                final Map<String, Object> root,
                final String path) {
            final String refreshRaw = (String) root.getOrDefault("refreshMode", RefreshMode.FULL.value());
            final RefreshMode refreshMode = RefreshMode.fromText(refreshRaw, path + ".refreshMode");
            final FieldRules fieldRules = FieldRules.fromObject(root.get("fieldRules"), path + ".fieldRules");
            final List<Object> collectionRaw = asList(root.get("collections"), path + ".collections");
            final List<CollectionRule> collections = new ArrayList<>(collectionRaw.size());
            for (int i = 0; i < collectionRaw.size(); i++) {
                collections.add(CollectionRule.fromMap(
                        asStringMap(collectionRaw.get(i), path + ".collections[" + i + "]"),
                        path + ".collections[" + i + "]"));
            }
            return new ProfileConfig(profile, refreshMode, fieldRules, collections);
        }

        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("refreshMode", refreshMode.value());
            if (!fieldRules.isEmpty()) {
                root.put("fieldRules", fieldRules.toMap());
            }
            final List<Map<String, Object>> collectionItems = new ArrayList<>(collections.size());
            for (final CollectionRule collection : collections) {
                collectionItems.add(collection.toMap());
            }
            root.put("collections", collectionItems);
            return root;
        }
    }

    public record CollectionRule(
            String database,
            String collection,
            Map<String, Object> filter,
            Map<String, Object> projection,
            Map<String, Object> sort,
            Integer limit,
            SampleSpec sample,
            FieldRules fieldRules) {
        public CollectionRule {
            database = requireText(database, "database");
            collection = requireText(collection, "collection");
            filter = canonicalizeMap(filter);
            projection = canonicalizeMap(projection);
            sort = canonicalizeMap(sort);
            fieldRules = Objects.requireNonNull(fieldRules, "fieldRules");
        }

        static CollectionRule fromMap(final Map<String, Object> root, final String path) {
            final String database = requireText((String) root.get("database"), path + ".database");
            final String collection = requireText((String) root.get("collection"), path + ".collection");
            final Map<String, Object> filter = asOptionalMap(root.get("filter"), path + ".filter");
            final Map<String, Object> projection = asOptionalMap(root.get("projection"), path + ".projection");
            final Map<String, Object> sort = asOptionalMap(root.get("sort"), path + ".sort");
            final Integer limit = optionalInt(root.get("limit"), path + ".limit");
            final SampleSpec sample = root.containsKey("sample")
                    ? SampleSpec.fromMap(asStringMap(root.get("sample"), path + ".sample"), path + ".sample")
                    : null;
            final FieldRules fieldRules = FieldRules.fromObject(root.get("fieldRules"), path + ".fieldRules");
            return new CollectionRule(database, collection, filter, projection, sort, limit, sample, fieldRules);
        }

        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("database", database);
            root.put("collection", collection);
            if (!filter.isEmpty()) {
                root.put("filter", filter);
            }
            if (!projection.isEmpty()) {
                root.put("projection", projection);
            }
            if (!sort.isEmpty()) {
                root.put("sort", sort);
            }
            if (limit != null) {
                root.put("limit", limit);
            }
            if (sample != null) {
                root.put("sample", sample.toMap());
            }
            if (!fieldRules.isEmpty()) {
                root.put("fieldRules", fieldRules.toMap());
            }
            return root;
        }

        String key() {
            return database + "." + collection;
        }
    }

    public record SampleSpec(int size, String seed) {
        public SampleSpec {
            seed = requireText(seed, "sample.seed");
        }

        static SampleSpec fromMap(final Map<String, Object> root, final String path) {
            final int size = requireInt(root.get("size"), path + ".size");
            final String seed = requireText((String) root.get("seed"), path + ".seed");
            return new SampleSpec(size, seed);
        }

        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            root.put("size", size);
            root.put("seed", seed);
            return root;
        }
    }

    public record FieldRules(List<String> include, List<String> exclude) {
        public FieldRules {
            include = normalizeFieldList(include, "include");
            exclude = normalizeFieldList(exclude, "exclude");
        }

        static FieldRules empty() {
            return new FieldRules(List.of(), List.of());
        }

        static FieldRules fromObject(final Object value, final String path) {
            if (value == null) {
                return empty();
            }
            final Map<String, Object> root = asStringMap(value, path);
            final List<String> include = optionalFieldList(root.get("include"), path + ".include");
            final List<String> exclude = optionalFieldList(root.get("exclude"), path + ".exclude");
            return new FieldRules(include, exclude);
        }

        boolean isEmpty() {
            return include.isEmpty() && exclude.isEmpty();
        }

        FieldRules mergeWith(final FieldRules overrideRules) {
            final FieldRules override = Objects.requireNonNull(overrideRules, "overrideRules");
            final List<String> mergedInclude = mergeOrdered(include, override.include());
            final List<String> mergedExclude = mergeOrdered(exclude, override.exclude());
            return new FieldRules(mergedInclude, mergedExclude);
        }

        Map<String, Object> toMap() {
            final Map<String, Object> root = new LinkedHashMap<>();
            if (!include.isEmpty()) {
                root.put("include", include);
            }
            if (!exclude.isEmpty()) {
                root.put("exclude", exclude);
            }
            return root;
        }

        private static List<String> mergeOrdered(final List<String> base, final List<String> override) {
            final LinkedHashSet<String> merged = new LinkedHashSet<>();
            merged.addAll(base);
            merged.addAll(override);
            return List.copyOf(merged);
        }

        private static List<String> normalizeFieldList(final List<String> values, final String fieldName) {
            final List<String> source = values == null ? List.of() : values;
            final LinkedHashSet<String> normalized = new LinkedHashSet<>();
            for (int i = 0; i < source.size(); i++) {
                final String value = requireText(source.get(i), fieldName + "[" + i + "]");
                normalized.add(value);
            }
            return List.copyOf(normalized);
        }
    }

    private static Map<String, Object> asOptionalMap(final Object value, final String fieldName) {
        if (value == null) {
            return Map.of();
        }
        return asStringMap(value, fieldName);
    }

    private static int requireInt(final Object value, final String fieldName) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(fieldName + " must be numeric");
        }
        return number.intValue();
    }

    private static Integer optionalInt(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }
        return requireInt(value, fieldName);
    }

    private static List<String> optionalFieldList(final Object value, final String fieldName) {
        if (value == null) {
            return List.of();
        }
        final List<Object> raw = asList(value, fieldName);
        final List<String> normalized = new ArrayList<>(raw.size());
        for (int i = 0; i < raw.size(); i++) {
            normalized.add(requireText(String.valueOf(raw.get(i)), fieldName + "[" + i + "]"));
        }
        return normalized;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asStringMap(final Object value, final String fieldName) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException(fieldName + " must be an object");
        }
        final Map<String, Object> normalized = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), canonicalizeValue(entry.getValue()));
        }
        return Collections.unmodifiableMap(normalized);
    }

    private static List<Object> asList(final Object value, final String fieldName) {
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException(fieldName + " must be an array");
        }
        final List<Object> normalized = new ArrayList<>(list.size());
        for (final Object item : list) {
            normalized.add(canonicalizeValue(item));
        }
        return Collections.unmodifiableList(normalized);
    }

    private static Map<String, Object> canonicalizeMap(final Map<String, Object> source) {
        final Map<String, Object> input = source == null ? Map.of() : source;
        final Map<String, Object> canonical = new LinkedHashMap<>();
        final TreeSet<String> keys = new TreeSet<>(input.keySet());
        for (final String key : keys) {
            canonical.put(key, canonicalizeValue(input.get(key)));
        }
        return Collections.unmodifiableMap(canonical);
    }

    private static Object canonicalizeValue(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> mapValue) {
            final Map<String, Object> canonical = new LinkedHashMap<>();
            final TreeSet<String> keys = new TreeSet<>();
            for (final Object key : mapValue.keySet()) {
                keys.add(String.valueOf(key));
            }
            for (final String key : keys) {
                canonical.put(key, canonicalizeValue(mapValue.get(key)));
            }
            return Collections.unmodifiableMap(canonical);
        }
        if (value instanceof Collection<?> collection) {
            final List<Object> canonical = new ArrayList<>(collection.size());
            for (final Object item : collection) {
                canonical.add(canonicalizeValue(item));
            }
            return Collections.unmodifiableList(canonical);
        }
        return value;
    }

    private static Map<ScenarioProfile, ProfileConfig> unmodifiableMapCopy(
            final Map<ScenarioProfile, ProfileConfig> source) {
        final Map<ScenarioProfile, ProfileConfig> copy = new LinkedHashMap<>();
        for (final ScenarioProfile profile : ScenarioProfile.values()) {
            if (source.containsKey(profile)) {
                copy.put(profile, source.get(profile));
            }
        }
        for (final Map.Entry<ScenarioProfile, ProfileConfig> entry : source.entrySet()) {
            if (!copy.containsKey(entry.getKey())) {
                copy.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(copy);
    }

    private static String requireText(final String value, final String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return trimmed;
    }
}
