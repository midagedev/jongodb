package org.jongodb.testkit;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Canonical complex-query certification pattern pack.
 */
public final class ComplexQueryPatternPack {
    public static final String PACK_VERSION = "complex-query-pack-v3";

    private static final List<PatternCase> PATTERNS = List.of(
            nestedLogicAndOrDotted(),
            nestedLogicNorAndFieldNot(),
            arrayScalarContainsAnd(),
            arrayElemMatchRange(),
            arrayAllElemMatchDocs(),
            pathDeepArrayDocument(),
            pathArrayIndexEq(),
            pathArrayIndexComparison(),
            exprBasicComparison(),
            exprLogicalComposition(),
            exprArrayIndexComparison(),
            regexCaseInsensitiveAndNe(),
            existsNullIn(),
            typeAndSize(),
            nestedArrayBranchComposition(),
            lookupLocalForeignUnwind(),
            lookupPipelineLetMatch(),
            aggregateFacetGroupSort(),
            aggregateSortByCountAfterProject(),
            aggregateUnionWithAndMatch(),
            unsupportedQueryMod(),
            unsupportedExprAdd(),
            unsupportedAggregateGraphLookup(),
            unsupportedQueryBitsAllSet());

    private ComplexQueryPatternPack() {}

    public static List<PatternCase> patterns() {
        return PATTERNS;
    }

    private static PatternCase nestedLogicAndOrDotted() {
        return pattern(
                "cq.nested.logic.and-or-dotted",
                "nested dotted path with mixed and/or",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Validates nested dotted path evaluation with logical composition.",
                "multi-criteria user search",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_nested_logic_and_or",
                                "documents",
                                List.of(
                                        payload(
                                                "_id",
                                                1,
                                                "profile",
                                                payload("city", "Seoul", "tier", "gold"),
                                                "active",
                                                true,
                                                "tags",
                                                List.of("core", "java")),
                                        payload(
                                                "_id",
                                                2,
                                                "profile",
                                                payload("city", "Busan", "tier", "silver"),
                                                "active",
                                                true,
                                                "tags",
                                                List.of("ops")),
                                        payload(
                                                "_id",
                                                3,
                                                "profile",
                                                payload("city", "Seoul", "tier", "bronze"),
                                                "active",
                                                false,
                                                "tags",
                                                List.of("ops"))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_nested_logic_and_or",
                                "filter",
                                payload(
                                        "$and",
                                        List.of(
                                                payload("profile.city", "Seoul"),
                                                payload(
                                                        "$or",
                                                        List.of(payload("active", true), payload("tags", "ops"))))))));
    }

    private static PatternCase nestedLogicNorAndFieldNot() {
        return pattern(
                "cq.nested.logic.nor-and-field-not",
                "compose nor with field-level not",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Covers negative condition composition without top-level $not.",
                "fraud suppression rule",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_nested_logic_nor",
                                "documents",
                                List.of(
                                        payload("_id", 1, "status", "active", "score", 82, "segment", "public"),
                                        payload("_id", 2, "status", "disabled", "score", 95, "segment", "public"),
                                        payload("_id", 3, "status", "active", "score", 30, "segment", "internal")))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_nested_logic_nor",
                                "filter",
                                payload(
                                        "$and",
                                        List.of(
                                                payload(
                                                        "$nor",
                                                        List.of(
                                                                payload("status", "disabled"),
                                                                payload("score", payload("$lt", 50)))),
                                                payload("segment", payload("$not", payload("$eq", "internal"))))))));
    }

    private static PatternCase arrayScalarContainsAnd() {
        return pattern(
                "cq.array.scalar-contains-and",
                "array scalar containment combined with and",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Ensures scalar containment on arrays works alongside additional predicates.",
                "feature-flag audience filtering",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_array_contains",
                                "documents",
                                List.of(
                                        payload("_id", 1, "tags", List.of("db", "core"), "active", true),
                                        payload("_id", 2, "tags", List.of("api"), "active", true),
                                        payload("_id", 3, "tags", List.of("db"), "active", false)))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_array_contains",
                                "filter",
                                payload("$and", List.of(payload("tags", "db"), payload("active", true))))));
    }

    private static PatternCase arrayElemMatchRange() {
        return pattern(
                "cq.array.elemmatch-range",
                "array elemMatch range",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Covers numeric range evaluation inside $elemMatch.",
                "scoring threshold query",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_array_elemmatch_range",
                                "documents",
                                List.of(
                                        payload("_id", 1, "scores", List.of(1, 3, 5)),
                                        payload("_id", 2, "scores", List.of(4, 7, 9)),
                                        payload("_id", 3, "scores", List.of(10, 12))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_array_elemmatch_range",
                                "filter",
                                payload("scores", payload("$elemMatch", payload("$gte", 4, "$lt", 9))))));
    }

    private static PatternCase arrayAllElemMatchDocs() {
        return pattern(
                "cq.array.all-elemmatch-docs",
                "all with elemMatch over document arrays",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Verifies nested document arrays with $all + $elemMatch combination.",
                "order line validation",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_array_all_elemmatch",
                                "documents",
                                List.of(
                                        payload(
                                                "_id",
                                                1,
                                                "items",
                                                List.of(payload("sku", "A", "qty", 2), payload("sku", "B", "qty", 1))),
                                        payload(
                                                "_id",
                                                2,
                                                "items",
                                                List.of(payload("sku", "A", "qty", 1), payload("sku", "C", "qty", 5))),
                                        payload("_id", 3, "items", List.of(payload("sku", "D", "qty", 3)))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_array_all_elemmatch",
                                "filter",
                                payload(
                                        "items",
                                        payload(
                                                "$all",
                                                List.of(payload(
                                                        "$elemMatch",
                                                        payload("sku", "A", "qty", payload("$gte", 2)))))))));
    }

    private static PatternCase pathDeepArrayDocument() {
        return pattern(
                "cq.path.deep-array-document",
                "deep dotted path across nested arrays",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Exercises nested dotted-path traversal through multi-level arrays.",
                "embedded order line lookup",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_path_deep_array",
                                "documents",
                                List.of(
                                        payload(
                                                "_id",
                                                1,
                                                "orders",
                                                List.of(
                                                        payload(
                                                                "id",
                                                                11,
                                                                "lines",
                                                                List.of(
                                                                        payload("sku", "A", "qty", 2),
                                                                        payload("sku", "B", "qty", 1))),
                                                        payload(
                                                                "id",
                                                                12,
                                                                "lines",
                                                                List.of(payload("sku", "C", "qty", 5))))),
                                        payload(
                                                "_id",
                                                2,
                                                "orders",
                                                List.of(payload(
                                                        "id",
                                                        21,
                                                        "lines",
                                                        List.of(payload("sku", "A", "qty", 1)))))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_path_deep_array",
                                "filter",
                                payload(
                                        "$and",
                                        List.of(
                                                payload("orders.lines.sku", "C"),
                                                payload("orders.lines.qty", payload("$gte", 5)))))));
    }

    private static PatternCase pathArrayIndexEq() {
        return pattern(
                "cq.path.array-index-eq",
                "array index path equality",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Covers positional path semantics (field.0.subfield).",
                "first-priority route lookup",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_path_array_index_eq",
                                "documents",
                                List.of(
                                        payload(
                                                "_id",
                                                1,
                                                "routes",
                                                List.of(
                                                        payload("region", "apac", "weight", 5),
                                                        payload("region", "emea", "weight", 3))),
                                        payload(
                                                "_id",
                                                2,
                                                "routes",
                                                List.of(
                                                        payload("region", "emea", "weight", 2),
                                                        payload("region", "apac", "weight", 1)))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_path_array_index_eq",
                                "filter",
                                payload("routes.0.region", "apac"))));
    }

    private static PatternCase pathArrayIndexComparison() {
        return pattern(
                "cq.path.array-index-comparison",
                "array index path comparison",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Validates numeric comparison against positional array path.",
                "second-reading threshold",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_path_array_index_cmp",
                                "documents",
                                List.of(
                                        payload("_id", 1, "samples", List.of(2, 9, 11)),
                                        payload("_id", 2, "samples", List.of(5, 4, 7)),
                                        payload("_id", 3, "samples", List.of(8, 6, 1))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_path_array_index_cmp",
                                "filter",
                                payload("samples.1", payload("$gt", 7)))));
    }

    private static PatternCase exprBasicComparison() {
        return pattern(
                "cq.expr.basic-comparison",
                "expr price greater than cost",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Ensures basic $expr comparison parity.",
                "margin-positive product query",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_expr_basic_cmp",
                                "documents",
                                List.of(payload("_id", 1, "price", 120, "cost", 80), payload("_id", 2, "price", 70, "cost", 80)))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_expr_basic_cmp",
                                "filter",
                                payload("$expr", payload("$gt", List.of("$price", "$cost"))))));
    }

    private static PatternCase exprLogicalComposition() {
        return pattern(
                "cq.expr.logical-composition",
                "expr nested and/or composition",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Covers composed boolean expression logic in $expr.",
                "segment eligibility rule",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_expr_logic",
                                "documents",
                                List.of(
                                        payload("_id", 1, "active", true, "qty", 1, "tier", "gold"),
                                        payload("_id", 2, "active", true, "qty", 4, "tier", "silver"),
                                        payload("_id", 3, "active", false, "qty", 10, "tier", "gold")))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_expr_logic",
                                "filter",
                                payload(
                                        "$expr",
                                        payload(
                                                "$and",
                                                List.of(
                                                        payload("$eq", List.of("$active", true)),
                                                        payload(
                                                                "$or",
                                                                List.of(
                                                                        payload("$gt", List.of("$qty", 2)),
                                                                        payload("$eq", List.of("$tier", "gold"))))))))));
    }

    private static PatternCase exprArrayIndexComparison() {
        return pattern(
                "cq.expr.array-index-comparison",
                "expr positional path comparison",
                SupportClass.PARTIAL,
                ExpectedOutcome.MATCH,
                "Validates $expr path resolution when arrays are indexed.",
                "time-series baseline check",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_expr_array_index",
                                "documents",
                                List.of(payload("_id", 1, "metrics", List.of(5, 2, 1)), payload("_id", 2, "metrics", List.of(3, 9, 4))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_expr_array_index",
                                "filter",
                                payload("$expr", payload("$eq", List.of("$metrics.0", 5))))));
    }

    private static PatternCase regexCaseInsensitiveAndNe() {
        return pattern(
                "cq.regex.case-insensitive-and-ne",
                "regex with options combined with ne",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Covers regex options plus additional inequality predicate.",
                "customer search with status exclusion",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_regex_and_ne",
                                "documents",
                                List.of(
                                        payload("_id", 1, "name", "Alice", "status", "active"),
                                        payload("_id", 2, "name", "ALINA", "status", "inactive"),
                                        payload("_id", 3, "name", "Bob", "status", "active")))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_regex_and_ne",
                                "filter",
                                payload(
                                        "$and",
                                        List.of(
                                                payload("name", payload("$regex", "^ali", "$options", "i")),
                                                payload("status", payload("$ne", "inactive")))))));
    }

    private static PatternCase existsNullIn() {
        return pattern(
                "cq.exists-null-in",
                "exists false with in predicate",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Ensures missing-field checks compose with set membership.",
                "optional profile routing",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_exists_null_in",
                                "documents",
                                List.of(
                                        payload("_id", 1, "region", "apac", "profile", payload("nickname", "neo")),
                                        payload("_id", 2, "region", "emea", "profile", payload()),
                                        payload("_id", 3, "region", "latam")))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_exists_null_in",
                                "filter",
                                payload(
                                        "$and",
                                        List.of(
                                                payload("profile.nickname", payload("$exists", false)),
                                                payload("region", payload("$in", List.of("apac", "emea"))))))));
    }

    private static PatternCase typeAndSize() {
        return pattern(
                "cq.type-and-size",
                "type and size predicate composition",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Validates type and array cardinality semantics in one filter.",
                "schema and payload guard",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_type_and_size",
                                "documents",
                                List.of(
                                        payload("_id", 1, "tags", List.of("a", "b"), "meta", payload("version", 1)),
                                        payload("_id", 2, "tags", List.of("a"), "meta", payload("version", 2L)),
                                        payload("_id", 3, "tags", List.of("x", "y"), "meta", payload("version", "3"))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_type_and_size",
                                "filter",
                                payload(
                                        "$and",
                                        List.of(
                                                payload("tags", payload("$size", 2)),
                                                payload("meta.version", payload("$type", "int")))))));
    }

    private static PatternCase nestedArrayBranchComposition() {
        return pattern(
                "cq.nested.array-branch-composition",
                "multiple predicates over array branches",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Covers combined dotted-path and elemMatch constraints on array documents.",
                "inventory reservation checks",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_nested_array_branch",
                                "documents",
                                List.of(
                                        payload(
                                                "_id",
                                                1,
                                                "items",
                                                List.of(payload("sku", "X", "qty", 5), payload("sku", "Y", "qty", 1))),
                                        payload(
                                                "_id",
                                                2,
                                                "items",
                                                List.of(payload("sku", "X", "qty", 2), payload("sku", "Y", "qty", 7)))))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_nested_array_branch",
                                "filter",
                                payload(
                                        "$and",
                                        List.of(
                                                payload("items.qty", payload("$gte", 5)),
                                                payload(
                                                        "items",
                                                        payload(
                                                                "$elemMatch",
                                                                payload("sku", "X", "qty", payload("$gte", 5)))))))));
    }

    private static PatternCase lookupLocalForeignUnwind() {
        return pattern(
                "cq.lookup.local-foreign-unwind",
                "lookup localField/foreignField with unwind",
                SupportClass.PARTIAL,
                ExpectedOutcome.MATCH,
                "Represents common join + unwind + post-filter pipeline.",
                "order/customer enrichment",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_lookup_local_orders",
                                "documents",
                                List.of(
                                        payload("_id", 1, "customerId", 11, "amount", 120),
                                        payload("_id", 2, "customerId", 12, "amount", 80),
                                        payload("_id", 3, "customerId", 13, "amount", 40)))),
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_lookup_local_customers",
                                "documents",
                                List.of(
                                        payload("_id", 11, "customerId", 11, "name", "alpha", "tier", "gold"),
                                        payload("_id", 12, "customerId", 12, "name", "beta", "tier", "silver")))),
                command(
                        "aggregate",
                        payload(
                                "collection",
                                "cq_lookup_local_orders",
                                "pipeline",
                                List.of(
                                        payload(
                                                "$lookup",
                                                payload(
                                                        "from",
                                                        "cq_lookup_local_customers",
                                                        "localField",
                                                        "customerId",
                                                        "foreignField",
                                                        "customerId",
                                                        "as",
                                                        "customer")),
                                        payload("$unwind", "$customer"),
                                        payload("$match", payload("customer.tier", "gold")),
                                        payload("$project", payload("_id", 1, "amount", 1, "customerName", "$customer.name")),
                                        payload("$sort", payload("_id", 1))),
                                "cursor",
                                payload())));
    }

    private static PatternCase lookupPipelineLetMatch() {
        return pattern(
                "cq.lookup.pipeline-let-match",
                "lookup pipeline with let and expr",
                SupportClass.PARTIAL,
                ExpectedOutcome.MATCH,
                "Covers lookup pipeline variable substitution and expr-based join matching.",
                "order-item scoped join",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_lookup_pipeline_orders",
                                "documents",
                                List.of(payload("_id", 1, "orderId", "O-1"), payload("_id", 2, "orderId", "O-2")))),
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_lookup_pipeline_items",
                                "documents",
                                List.of(
                                        payload("_id", 11, "orderId", "O-1", "sku", "A", "qty", 3),
                                        payload("_id", 12, "orderId", "O-1", "sku", "B", "qty", 1),
                                        payload("_id", 13, "orderId", "O-2", "sku", "A", "qty", 1)))),
                command(
                        "aggregate",
                        payload(
                                "collection",
                                "cq_lookup_pipeline_orders",
                                "pipeline",
                                List.of(
                                        payload(
                                                "$lookup",
                                                payload(
                                                        "from",
                                                        "cq_lookup_pipeline_items",
                                                        "let",
                                                        payload("orderIdVar", "$orderId"),
                                                        "pipeline",
                                                        List.of(
                                                                payload(
                                                                        "$match",
                                                                        payload(
                                                                                "$expr",
                                                                                payload(
                                                                                        "$eq",
                                                                                        List.of("$orderId", "$$orderIdVar")))),
                                                                payload("$match", payload("qty", payload("$gte", 2)))),
                                                        "as",
                                                        "items")),
                                        payload("$match", payload("items", payload("$elemMatch", payload("sku", "A"))))),
                                "cursor",
                                payload())));
    }

    private static PatternCase aggregateFacetGroupSort() {
        return pattern(
                "cq.aggregate.facet-group-sort",
                "facet with grouped and counted branches",
                SupportClass.PARTIAL,
                ExpectedOutcome.MATCH,
                "Covers multi-branch aggregation composition under $facet.",
                "dashboard summary query",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_facet_orders",
                                "documents",
                                List.of(
                                        payload("_id", 1, "status", "active", "region", "apac"),
                                        payload("_id", 2, "status", "active", "region", "emea"),
                                        payload("_id", 3, "status", "inactive", "region", "apac")))),
                command(
                        "aggregate",
                        payload(
                                "collection",
                                "cq_facet_orders",
                                "pipeline",
                                List.of(payload(
                                        "$facet",
                                        payload(
                                                "activeOnly",
                                                List.of(payload("$match", payload("status", "active")), payload("$count", "total")),
                                                "byRegion",
                                                List.of(
                                                        payload("$group", payload("_id", "$region", "total", payload("$sum", 1))),
                                                        payload("$sort", payload("_id", 1)))))),
                                "cursor",
                                payload())));
    }

    private static PatternCase aggregateSortByCountAfterProject() {
        return pattern(
                "cq.aggregate.sortbycount-after-project",
                "sortByCount after projection",
                SupportClass.PARTIAL,
                ExpectedOutcome.MATCH,
                "Represents projection + cardinality histogram path.",
                "city popularity view",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_sortbycount_users",
                                "documents",
                                List.of(
                                        payload("_id", 1, "profile", payload("city", "Seoul")),
                                        payload("_id", 2, "profile", payload("city", "Busan")),
                                        payload("_id", 3, "profile", payload("city", "Seoul"))))),
                command(
                        "aggregate",
                        payload(
                                "collection",
                                "cq_sortbycount_users",
                                "pipeline",
                                List.of(payload("$project", payload("city", "$profile.city")), payload("$sortByCount", "$city")),
                                "cursor",
                                payload())));
    }

    private static PatternCase aggregateUnionWithAndMatch() {
        return pattern(
                "cq.aggregate.unionwith-and-match",
                "unionWith plus post-union match",
                SupportClass.PARTIAL,
                ExpectedOutcome.MATCH,
                "Covers unionWith subset with downstream filtering.",
                "active feed consolidation",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_union_primary",
                                "documents",
                                List.of(payload("_id", 1, "status", "open"), payload("_id", 2, "status", "closed")))),
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_union_archive",
                                "documents",
                                List.of(payload("_id", 101, "status", "open"), payload("_id", 102, "status", "archived")))),
                command(
                        "aggregate",
                        payload(
                                "collection",
                                "cq_union_primary",
                                "pipeline",
                                List.of(
                                        payload("$unionWith", payload("coll", "cq_union_archive")),
                                        payload("$match", payload("status", "open")),
                                        payload("$sort", payload("_id", 1))),
                                "cursor",
                                payload())));
    }

    private static PatternCase unsupportedQueryMod() {
        return pattern(
                "cq.unsupported.query-mod",
                "query operator mod",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Validates modulo predicate semantics in query operator evaluation.",
                "legacy modulo filtering",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_unsupported_mod",
                                "documents",
                                List.of(payload("_id", 1, "qty", 2), payload("_id", 2, "qty", 3)))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_unsupported_mod",
                                "filter",
                                payload("qty", payload("$mod", List.of(2, 0))))));
    }

    private static PatternCase unsupportedExprAdd() {
        return pattern(
                "cq.unsupported.expr-add",
                "expr add arithmetic operator",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Validates certification subset for $expr arithmetic with $add.",
                "inline total-cost expression",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_unsupported_expr_add",
                                "documents",
                                List.of(payload("_id", 1, "price", 100, "tax", 20), payload("_id", 2, "price", 80, "tax", 10)))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_unsupported_expr_add",
                                "filter",
                                payload(
                                        "$expr",
                                        payload("$eq", List.of(payload("$add", List.of("$price", "$tax")), 120))))));
    }

    private static PatternCase unsupportedAggregateGraphLookup() {
        return pattern(
                "cq.unsupported.aggregate-graphlookup",
                "graphLookup stage (minimal subset)",
                SupportClass.PARTIAL,
                ExpectedOutcome.MATCH,
                "Validates minimal graph traversal subset for certification hierarchy expansion.",
                "hierarchy traversal",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_unsupported_graphlookup",
                                "documents",
                                List.of(
                                        payload("_id", "A", "parent", null),
                                        payload("_id", "B", "parent", "A"),
                                        payload("_id", "C", "parent", "B")))),
                command(
                        "aggregate",
                        payload(
                                "collection",
                                "cq_unsupported_graphlookup",
                                "pipeline",
                                List.of(payload(
                                        "$graphLookup",
                                        payload(
                                                "from",
                                                "cq_unsupported_graphlookup",
                                                "startWith",
                                                "$parent",
                                                "connectFromField",
                                                "parent",
                                                "connectToField",
                                                "_id",
                                                "as",
                                                "graph"))),
                                "cursor",
                                payload())));
    }

    private static PatternCase unsupportedQueryBitsAllSet() {
        return pattern(
                "cq.unsupported.query-bitsallset",
                "bitwise query operator bitsAllSet",
                SupportClass.SUPPORTED,
                ExpectedOutcome.MATCH,
                "Covers bitwise mask predicate subset for feature-flag style filtering.",
                "feature-flag bitmask query",
                command(
                        "insert",
                        payload(
                                "collection",
                                "cq_unsupported_bits",
                                "documents",
                                List.of(payload("_id", 1, "flags", 3), payload("_id", 2, "flags", 4)))),
                command(
                        "find",
                        payload(
                                "collection",
                                "cq_unsupported_bits",
                                "filter",
                                payload("flags", payload("$bitsAllSet", 1)))));
    }

    private static PatternCase pattern(
            final String id,
            final String title,
            final SupportClass supportClass,
            final ExpectedOutcome expectedOutcome,
            final String rationale,
            final String sampleUseCase,
            final ScenarioCommand... commands) {
        return new PatternCase(
                id,
                title,
                supportClass,
                expectedOutcome,
                rationale,
                sampleUseCase,
                new Scenario(id, title, List.of(commands)));
    }

    private static ScenarioCommand command(final String commandName, final Map<String, Object> payload) {
        return new ScenarioCommand(commandName, payload);
    }

    private static Map<String, Object> payload(final Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues length must be even");
        }
        final Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            final Object key = keyValues[i];
            if (!(key instanceof String keyString)) {
                throw new IllegalArgumentException("keyValues keys must be strings");
            }
            values.put(keyString, keyValues[i + 1]);
        }
        return Collections.unmodifiableMap(values);
    }

    public enum SupportClass {
        SUPPORTED,
        PARTIAL,
        EXPLICITLY_UNSUPPORTED
    }

    public enum ExpectedOutcome {
        MATCH,
        UNSUPPORTED_POLICY
    }

    public record PatternCase(
            String id,
            String title,
            SupportClass supportClass,
            ExpectedOutcome expectedOutcome,
            String rationale,
            String sampleUseCase,
            Scenario scenario) {
        public PatternCase {
            id = requireText(id, "id");
            title = requireText(title, "title");
            supportClass = Objects.requireNonNull(supportClass, "supportClass");
            expectedOutcome = Objects.requireNonNull(expectedOutcome, "expectedOutcome");
            rationale = requireText(rationale, "rationale");
            sampleUseCase = requireText(sampleUseCase, "sampleUseCase");
            scenario = Objects.requireNonNull(scenario, "scenario");
            if (!id.equals(scenario.id())) {
                throw new IllegalArgumentException("pattern id must match scenario id");
            }
        }

        public boolean approvedUnsupportedPolicy() {
            return expectedOutcome == ExpectedOutcome.UNSUPPORTED_POLICY;
        }
    }

    private static String requireText(final String value, final String fieldName) {
        final String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
