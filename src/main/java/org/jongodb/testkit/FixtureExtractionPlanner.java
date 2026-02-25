package org.jongodb.testkit;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds deterministic extraction plans from a manifest/profile pair.
 *
 * <p>By design, planning requires an explicit manifest. There is no planner API without one.
 */
public final class FixtureExtractionPlanner {
    private FixtureExtractionPlanner() {}

    public static FixtureExtractionPlan plan(
            final Path manifestPath,
            final FixtureManifest.ScenarioProfile profile) throws IOException {
        final FixtureManifest manifest = FixtureManifestLoader.load(manifestPath);
        return plan(manifest, profile);
    }

    public static FixtureExtractionPlan plan(
            final FixtureManifest manifest,
            final FixtureManifest.ScenarioProfile profile) {
        final FixtureManifest sourceManifest = Objects.requireNonNull(manifest, "manifest");
        final FixtureManifest.ScenarioProfile sourceProfile = Objects.requireNonNull(profile, "profile");

        FixtureManifestValidator.validateOrThrow(sourceManifest);
        final FixtureManifest.ProfileConfig profileConfig = sourceManifest.profile(sourceProfile);

        final List<FixtureExtractionPlan.CollectionPlan> collectionPlans =
                new ArrayList<>(profileConfig.collections().size());
        for (final FixtureManifest.CollectionRule rule : profileConfig.collections()) {
            final FixtureManifest.FieldRules effectiveRules =
                    profileConfig.fieldRules().mergeWith(rule.fieldRules());
            collectionPlans.add(new FixtureExtractionPlan.CollectionPlan(
                    rule.database(),
                    rule.collection(),
                    rule.filter(),
                    rule.projection(),
                    rule.sort(),
                    rule.limit(),
                    rule.sample(),
                    effectiveRules.include(),
                    effectiveRules.exclude()));
        }

        collectionPlans.sort((left, right) -> {
            final int dbCompare = left.database().compareTo(right.database());
            if (dbCompare != 0) {
                return dbCompare;
            }
            return left.collection().compareTo(right.collection());
        });

        final String fingerprint = computeFingerprint(
                sourceManifest.schemaVersion(),
                sourceManifest.source().uriAlias(),
                sourceProfile,
                profileConfig.refreshMode(),
                collectionPlans);

        return new FixtureExtractionPlan(
                sourceManifest.schemaVersion(),
                sourceManifest.source().uriAlias(),
                sourceProfile,
                profileConfig.refreshMode(),
                fingerprint,
                collectionPlans);
    }

    private static String computeFingerprint(
            final String schemaVersion,
            final String sourceUriAlias,
            final FixtureManifest.ScenarioProfile profile,
            final FixtureManifest.RefreshMode refreshMode,
            final List<FixtureExtractionPlan.CollectionPlan> collectionPlans) {
        final Map<String, Object> digestRoot = new LinkedHashMap<>();
        digestRoot.put("schemaVersion", schemaVersion);
        digestRoot.put("sourceUriAlias", sourceUriAlias);
        digestRoot.put("profile", profile.value());
        digestRoot.put("refreshMode", refreshMode.value());
        final List<Map<String, Object>> collectionItems = new ArrayList<>(collectionPlans.size());
        for (final FixtureExtractionPlan.CollectionPlan plan : collectionPlans) {
            collectionItems.add(plan.toMap());
        }
        digestRoot.put("collections", collectionItems);
        final String canonical = DiffSummaryGenerator.JsonEncoder.encode(digestRoot);
        return toUnsignedHex(fnv1a64(canonical));
    }

    private static long fnv1a64(final String value) {
        final String normalized = Objects.requireNonNull(value, "value");
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < normalized.length(); i++) {
            hash ^= normalized.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private static String toUnsignedHex(final long value) {
        String hex = Long.toUnsignedString(value, 16);
        if (hex.length() >= 16) {
            return hex;
        }
        final StringBuilder sb = new StringBuilder(16);
        for (int i = hex.length(); i < 16; i++) {
            sb.append('0');
        }
        sb.append(hex);
        return sb.toString();
    }
}
