package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic extraction plan for one scenario profile.
 */
public record FixtureExtractionPlan(
        String schemaVersion,
        String sourceUriAlias,
        FixtureManifest.ScenarioProfile profile,
        FixtureManifest.RefreshMode refreshMode,
        String fingerprint,
        List<CollectionPlan> collections) {
    public FixtureExtractionPlan {
        schemaVersion = requireText(schemaVersion, "schemaVersion");
        sourceUriAlias = requireText(sourceUriAlias, "sourceUriAlias");
        profile = Objects.requireNonNull(profile, "profile");
        refreshMode = Objects.requireNonNull(refreshMode, "refreshMode");
        fingerprint = requireText(fingerprint, "fingerprint");
        collections = List.copyOf(Objects.requireNonNull(collections, "collections"));
    }

    public String toJson() {
        return DiffSummaryGenerator.JsonEncoder.encode(toMap());
    }

    Map<String, Object> toMap() {
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("schemaVersion", schemaVersion);
        root.put("sourceUriAlias", sourceUriAlias);
        root.put("profile", profile.value());
        root.put("refreshMode", refreshMode.value());
        root.put("fingerprint", fingerprint);

        final List<Map<String, Object>> collectionItems = new ArrayList<>(collections.size());
        for (final CollectionPlan collection : collections) {
            collectionItems.add(collection.toMap());
        }
        root.put("collections", collectionItems);
        return root;
    }

    public record CollectionPlan(
            String database,
            String collection,
            Map<String, Object> filter,
            Map<String, Object> projection,
            Map<String, Object> sort,
            Integer limit,
            FixtureManifest.SampleSpec sample,
            List<String> includeFields,
            List<String> excludeFields) {
        public CollectionPlan {
            database = requireText(database, "database");
            collection = requireText(collection, "collection");
            filter = Map.copyOf(Objects.requireNonNull(filter, "filter"));
            projection = Map.copyOf(Objects.requireNonNull(projection, "projection"));
            sort = Map.copyOf(Objects.requireNonNull(sort, "sort"));
            if (limit != null && limit <= 0) {
                throw new IllegalArgumentException("limit must be > 0");
            }
            includeFields = List.copyOf(Objects.requireNonNull(includeFields, "includeFields"));
            excludeFields = List.copyOf(Objects.requireNonNull(excludeFields, "excludeFields"));
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
            if (!includeFields.isEmpty()) {
                root.put("includeFields", includeFields);
            }
            if (!excludeFields.isEmpty()) {
                root.put("excludeFields", excludeFields);
            }
            return root;
        }
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
