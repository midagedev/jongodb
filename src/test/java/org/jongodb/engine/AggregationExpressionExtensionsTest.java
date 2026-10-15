package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

class AggregationExpressionExtensionsTest {
    private static final Date SAMPLE = Date.from(Instant.parse("2014-01-01T08:15:39.736Z"));
    private static final Date SUMMER = Date.from(Instant.parse("2026-07-21T09:37:42.987Z"));

    @Test
    void dateToStringFormatsTimezoneAndOnNull() {
        final Document formatted = project(
                new Document("createdAt", SAMPLE),
                new Document("all", specifiers(SAMPLE, null))
                        .append("ny", zoned(SAMPLE, "%H:%M:%S:%L%z %Z %b %B", "America/New_York"))
                        .append("offset", zoned(SAMPLE, "%H:%M:%S:%L%z|%Z", "+04:30"))
                        .append("compact", zoned(SAMPLE, "%Y-%m-%d %H:%M:%S %z %Z", "-0530"))
                        .append("plusHour", zoned(SAMPLE, "%z %Z", "+09"))
                        .append("utc", zoned(SAMPLE, "%z %Z", "UTC"))
                        .append("default", new Document("$dateToString", new Document("date", "$createdAt"))));

        assertEquals(
                "2014 01 01 08 15 39 736 001 4 3 00 01 2014 0 +0000 % Jan January",
                formatted.getString("all"));
        assertEquals("03:15:39:736-0500 -300 Jan January", formatted.getString("ny"));
        assertEquals("12:45:39:736+0430|270", formatted.getString("offset"));
        assertEquals("2014-01-01 02:45:39 -0530 -330", formatted.getString("compact"));
        assertEquals("+0900 540", formatted.getString("plusHour"));
        assertEquals("+0000 0", formatted.getString("utc"));
        assertEquals("2014-01-01T08:15:39.736Z", formatted.getString("default"));

        final Document seoul = project(
                new Document("createdAt", SUMMER),
                new Document(
                        "text",
                        zoned(
                                SUMMER,
                                "%Y-%m-%d %H:%M:%S.%L %z %Z %w %u %j %U %V %G",
                                "Asia/Seoul")));
        assertEquals("2026-07-21 18:37:42.987 +0900 540 3 2 202 29 30 2026", seoul.getString("text"));

        final List<Document> grouped = AggregationPipeline.execute(
                List.of(
                        new Document("_id", 1).append("createdAt", SUMMER),
                        new Document("_id", 2).append("createdAt", Date.from(Instant.parse("2026-07-21T00:30:00Z"))),
                        new Document("_id", 3).append("createdAt", Date.from(Instant.parse("2026-07-21T16:00:00Z")))),
                List.of(new Document(
                        "$group",
                        new Document("_id", zoned(null, "%Y-%m-%d", "Asia/Seoul"))
                                .append("n", new Document("$sum", 1)))));
        assertEquals(2, grouped.size());
        assertEquals("2026-07-21", grouped.get(0).get("_id"));
        assertEquals(2, grouped.get(0).get("n"));
        assertEquals("2026-07-22", grouped.get(1).get("_id"));

        final Document nulls = project(
                new Document("createdAt", null),
                new Document("fallback", new Document(
                                "$dateToString",
                                new Document("date", "$createdAt").append("onNull", "missing")))
                        .append("explicit", new Document(
                                "$dateToString",
                                new Document("date", "$absent").append("onNull", new Document("$concat", List.of("n", "one")))))
                        .append("plain", new Document("$dateToString", new Document("date", "$createdAt"))));
        assertEquals("missing", nulls.getString("fallback"));
        assertEquals("none", nulls.getString("explicit"));
        assertNull(nulls.get("plain"));

        final IllegalArgumentException badFormat = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document("createdAt", null),
                        new Document("text", new Document(
                                "$dateToString",
                                new Document("format", "%Q").append("date", "$createdAt")))));
        assertTrue(badFormat.getMessage().contains("Invalid format character '%Q'"));
        assertFalse(badFormat.getMessage().contains("unsupported"));

        final IllegalArgumentException trailing = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document("createdAt", SAMPLE),
                        new Document("text", new Document(
                                "$dateToString",
                                new Document("format", "%Y%").append("date", "$createdAt")))));
        assertTrue(trailing.getMessage().contains("Unmatched '%'"));

        final IllegalArgumentException badZone = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document("createdAt", SAMPLE),
                        new Document("text", zoned(SAMPLE, "%Y", "Not/AZone"))));
        assertTrue(badZone.getMessage().contains("unrecognized time zone identifier: \"Not/AZone\""));
    }

    @Test
    void datePartOperatorsWithTimezone() {
        final Document parts = project(new Document("d", SUMMER), seoulParts());
        assertEquals(2026, parts.get("year"));
        assertEquals(Integer.class, parts.get("year").getClass());
        assertEquals(7, parts.get("month"));
        assertEquals(21, parts.get("day"));
        assertEquals(18, parts.get("hour"));
        assertEquals(37, parts.get("minute"));
        assertEquals(42, parts.get("second"));
        assertEquals(987, parts.get("millisecond"));
        assertEquals(3, parts.get("dow"));
        assertEquals(202, parts.get("doy"));
        assertEquals(29, parts.get("week"));
        assertEquals(30, parts.get("isoWeek"));
        assertEquals(2026L, parts.get("isoYear"));
        assertEquals(Long.class, parts.get("isoYear").getClass());
        assertEquals(2, parts.get("isoDow"));
        assertEquals(2026, parts.get("plainYear"));

        final Date newYear = Date.from(Instant.parse("2016-01-01T00:00:00Z"));
        final Document edge = project(
                new Document("d", newYear),
                new Document("year", part("$year"))
                        .append("dow", part("$dayOfWeek"))
                        .append("doy", part("$dayOfYear"))
                        .append("week", part("$week"))
                        .append("isoWeek", part("$isoWeek"))
                        .append("isoYear", part("$isoWeekYear"))
                        .append("isoDow", part("$isoDayOfWeek")));
        assertEquals(2016, edge.get("year"));
        assertEquals(6, edge.get("dow"));
        assertEquals(1, edge.get("doy"));
        assertEquals(0, edge.get("week"));
        assertEquals(53, edge.get("isoWeek"));
        assertEquals(2015L, edge.get("isoYear"));
        assertEquals(5, edge.get("isoDow"));

        final Document sources = project(
                new Document("id", new ObjectId("5ab9c3da31c2ab715d421285"))
                        .append("ts", new BsonTimestamp(1400000000, 1)),
                new Document("fromId", new Document("$year", "$id"))
                        .append("fromTs", new Document("$year", "$ts")));
        assertEquals(2018, sources.get("fromId"));
        assertEquals(2014, sources.get("fromTs"));

        final Date beforeEpoch = Date.from(Instant.parse("1969-12-31T23:59:59.001Z"));
        final Document early = project(
                new Document("d", beforeEpoch),
                new Document("text", new Document("$dateToString", new Document("date", "$d")))
                        .append("ms", part("$millisecond"))
                        .append("second", part("$second")));
        assertEquals("1969-12-31T23:59:59.001Z", early.getString("text"));
        assertEquals(1, early.get("ms"));
        assertEquals(59, early.get("second"));

        final IllegalArgumentException badType = assertThrows(
                IllegalArgumentException.class,
                () -> project(new Document("d", "2014-01-01"), new Document("year", part("$year"))));
        assertTrue(badType.getMessage().contains("can't convert from BSON type string to Date"));
        assertFalse(badType.getMessage().contains("unsupported"));

        final IllegalArgumentException badOption = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document("d", SUMMER),
                        new Document("year", new Document(
                                "$year",
                                new Document("date", "$d").append("timezone", "UTC").append("foo", 1)))));
        assertTrue(badOption.getMessage().contains("unrecognized option to $year: \"foo\""));
    }

    @Test
    void dateTruncHonorsNonUtcTimezone() {
        final Document truncated = project(
                new Document("d", SUMMER)
                        .append("spring", Date.from(Instant.parse("2020-03-08T06:30:00Z")))
                        .append("fall", Date.from(Instant.parse("2020-11-01T05:50:00Z"))),
                new Document("seoulDay", trunc("$d", "day", null, "Asia/Seoul", null))
                        .append("seoulHour", trunc("$d", "hour", null, "Asia/Seoul", null))
                        .append("seoulWeek", trunc("$d", "week", null, "Asia/Seoul", "monday"))
                        .append("offsetHour", trunc("$d", "hour", 2, "+05:30", null))
                        .append("westDay", trunc("$d", "day", null, "-0530", null))
                        .append("nyDay", trunc("$spring", "day", null, "America/New_York", null))
                        .append("nyHour", trunc("$spring", "hour", null, "America/New_York", null))
                        .append("nyBin", trunc("$fall", "hour", 3, "America/New_York", null))
                        .append("millis", trunc("$d", "millisecond", 10, "UTC", null)));

        assertEquals(1784559600000L, truncated.getDate("seoulDay").getTime());
        assertEquals(1784624400000L, truncated.getDate("seoulHour").getTime());
        assertEquals(1784473200000L, truncated.getDate("seoulWeek").getTime());
        assertEquals(1784622600000L, truncated.getDate("offsetHour").getTime());
        assertEquals(1784611800000L, truncated.getDate("westDay").getTime());
        assertEquals(1583643600000L, truncated.getDate("nyDay").getTime());
        assertEquals(1583647200000L, truncated.getDate("nyHour").getTime());
        assertEquals(1604206800000L, truncated.getDate("nyBin").getTime());
        assertEquals(Date.from(Instant.parse("2026-07-21T09:37:42.980Z")), truncated.getDate("millis"));

        final IllegalArgumentException badZone = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document("d", SUMMER),
                        new Document("day", trunc("$d", "day", null, "Not/AZone", null))));
        assertTrue(badZone.getMessage().contains("unrecognized time zone identifier"));
        assertFalse(badZone.getMessage().contains("unsupported"));
    }

    @Test
    void stringAndArrayExpressions() {
        final Document values = project(
                new Document("name", "AbC")
                        .append("empty", "")
                        .append("items", List.of("a", "b", "c"))
                        .append("none", null)
                        .append("flag", true)
                        .append("whole", 2)
                        .append("wide", -9223372036854775808L)
                        .append("decimal", Decimal128.parse("1.50"))
                        .append("scaled", Decimal128.parse("1.0"))
                        .append("id", new ObjectId("5ab9c3da31c2ab715d421285"))
                        .append("createdAt", SAMPLE)
                        .append("big", 1234567.25d)
                        .append("million", 1000000.0d)
                        .append("tiny", 0.000123d)
                        .append("sci", 1.0e21d)
                        .append("boundary", 1.0e16d)
                        .append("justPlain", 1.0e15d)
                        .append("milli", 1.0e-4d)
                        .append("smaller", 1.0e-5d)
                        .append("negZero", -0.0d)
                        .append("inf", Double.POSITIVE_INFINITY)
                        .append("nan", Double.NaN)
                        .append("cafe", "cafétéria")
                        .append("sushi", "寿司sushi")
                        .append("dashed", "a--b-")
                        .append("padded", "  \n good  bye \t  ")
                        .append("letters", " ggggoodbyeeeee")
                        .append("nbsp", "\u00A0a\u00A0")
                        .append("ideographic", "\u3000a\u3000"),
                new Document("joined", new Document("$concat", List.of("$name", "-", "x")))
                        .append("one", new Document("$concat", List.of("only")))
                        .append("blank", new Document("$concat", List.of()))
                        .append("missing", new Document("$concat", List.of("$name", "$absent")))
                        .append("nullish", new Document("$concat", List.of("$name", "$none")))
                        .append("lower", new Document("$toLower", "$name"))
                        .append("lowerList", new Document("$toLower", List.of("$name")))
                        .append("lowerNull", new Document("$toLower", "$none"))
                        .append("lowerMissing", new Document("$toLower", "$absent"))
                        .append("upper", new Document("$toUpper", "$name"))
                        .append("lowerDate", new Document("$toLower", "$createdAt"))
                        .append("lowerBig", new Document("$toLower", "$big"))
                        .append("lowerMillion", new Document("$toLower", "$million"))
                        .append("lowerTiny", new Document("$toLower", "$tiny"))
                        .append("upperSci", new Document("$toUpper", "$sci"))
                        .append("lowerInf", new Document("$toLower", "$inf"))
                        .append("upperInf", new Document("$toUpper", "$inf"))
                        .append("lowerNan", new Document("$toLower", "$nan"))
                        .append("text", new Document("$toString", "$name"))
                        .append("textNull", new Document("$toString", "$none"))
                        .append("textMissing", new Document("$toString", "$absent"))
                        .append("textFlag", new Document("$toString", "$flag"))
                        .append("textWhole", new Document("$toString", "$whole"))
                        .append("textWide", new Document("$toString", "$wide"))
                        .append("textDecimal", new Document("$toString", "$decimal"))
                        .append("textScaled", new Document("$toString", "$scaled"))
                        .append("textId", new Document("$toString", "$id"))
                        .append("textDate", new Document("$toString", "$createdAt"))
                        .append("textBig", new Document("$toString", "$big"))
                        .append("textBoundary", new Document("$toString", "$boundary"))
                        .append("textPlain", new Document("$toString", "$justPlain"))
                        .append("textMilli", new Document("$toString", "$milli"))
                        .append("textSmaller", new Document("$toString", "$smaller"))
                        .append("textNegZero", new Document("$toString", "$negZero"))
                        .append("textInf", new Document("$toString", "$inf"))
                        .append("textNan", new Document("$toString", "$nan"))
                        .append("count", new Document("$size", "$items"))
                        .append("countWrapped", new Document("$size", List.of("$items")))
                        .append("slice", new Document("$substrCP", List.of("$cafe", 0, 5)))
                        .append("sliceWhole", new Document("$substrCP", List.of("hello", 1.0d, 2.0d)))
                        .append("slicePast", new Document("$substrCP", List.of("hello", 20, 1)))
                        .append("sliceNull", new Document("$substrCP", List.of("$none", 0, 1)))
                        .append("parts", new Document("$split", List.of("$dashed", "-")))
                        .append("splitNull", new Document("$split", List.of("$none", "-")))
                        .append("length", new Document("$strLenCP", "$sushi"))
                        .append("trimmed", new Document("$trim", new Document("input", "$padded")))
                        .append("trimmedChars", new Document(
                                "$trim",
                                new Document("input", "$letters").append("chars", "ge")))
                        .append("trimmedNbsp", new Document("$trim", new Document("input", "$nbsp")))
                        .append("keptSpace", new Document("$trim", new Document("input", "$ideographic")))
                        .append("trimNull", new Document("$trim", new Document("input", "$none")))
                        .append("trimNullChars", new Document(
                                "$trim", new Document("input", "$name").append("chars", "$none"))));

        assertEquals("AbC-x", values.getString("joined"));
        assertEquals("only", values.getString("one"));
        assertEquals("", values.getString("blank"));
        assertNull(values.get("missing"));
        assertNull(values.get("nullish"));
        assertEquals("abc", values.getString("lower"));
        assertEquals("abc", values.getString("lowerList"));
        assertEquals("", values.getString("lowerNull"));
        assertEquals("", values.getString("lowerMissing"));
        assertEquals("ABC", values.getString("upper"));
        assertEquals("2014-01-01t08:15:39.736z", values.getString("lowerDate"));
        assertEquals("1.23457e+06", values.getString("lowerBig"));
        assertEquals("1e+06", values.getString("lowerMillion"));
        assertEquals("0.000123", values.getString("lowerTiny"));
        assertEquals("1E+21", values.getString("upperSci"));
        assertEquals("inf", values.getString("lowerInf"));
        assertEquals("INF", values.getString("upperInf"));
        assertEquals("nan", values.getString("lowerNan"));
        assertEquals("AbC", values.getString("text"));
        assertNull(values.get("textNull"));
        assertNull(values.get("textMissing"));
        assertEquals("true", values.getString("textFlag"));
        assertEquals("2", values.getString("textWhole"));
        assertEquals("-9223372036854775808", values.getString("textWide"));
        assertEquals("1.50", values.getString("textDecimal"));
        assertEquals("1.0", values.getString("textScaled"));
        assertEquals("5ab9c3da31c2ab715d421285", values.getString("textId"));
        assertEquals("2014-01-01T08:15:39.736Z", values.getString("textDate"));
        assertEquals("1234567.25", values.getString("textBig"));
        assertEquals("1e+16", values.getString("textBoundary"));
        assertEquals("1000000000000000", values.getString("textPlain"));
        assertEquals("0.0001", values.getString("textMilli"));
        assertEquals("1e-05", values.getString("textSmaller"));
        assertEquals("-0", values.getString("textNegZero"));
        assertEquals("Infinity", values.getString("textInf"));
        assertEquals("NaN", values.getString("textNan"));
        assertEquals(3, values.get("count"));
        assertEquals(Integer.class, values.get("count").getClass());
        assertEquals(3, values.get("countWrapped"));
        assertEquals("cafét", values.getString("slice"));
        assertEquals("el", values.getString("sliceWhole"));
        assertEquals("", values.getString("slicePast"));
        assertEquals("", values.getString("sliceNull"));
        assertEquals(List.of("a", "", "b", ""), values.get("parts"));
        assertNull(values.get("splitNull"));
        assertEquals(7, values.get("length"));
        assertEquals(Integer.class, values.get("length").getClass());
        assertEquals("good  bye", values.getString("trimmed"));
        assertEquals(" ggggoodby", values.getString("trimmedChars"));
        assertEquals("a", values.getString("trimmedNbsp"));
        assertEquals("\u3000a\u3000", values.getString("keptSpace"));
        assertNull(values.get("trimNull"));
        assertNull(values.get("trimNullChars"));

        final IllegalArgumentException concatType = assertThrows(
                IllegalArgumentException.class,
                () -> project(new Document("n", 1), new Document("text", new Document("$concat", List.of("$n")))));
        assertTrue(concatType.getMessage().contains("$concat only supports strings, not int"));
        assertFalse(concatType.getMessage().contains("unsupported"));

        final IllegalArgumentException lowerArgs = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document(),
                        new Document("text", new Document("$toLower", List.of("Ab", "C")))));
        assertTrue(lowerArgs.getMessage().contains("takes exactly 1 arguments"));

        final IllegalArgumentException objectId = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document("id", new ObjectId()),
                        new Document("text", new Document("$toLower", "$id"))));
        assertTrue(objectId.getMessage().contains("can't convert from BSON type objectId to String"));

        final IllegalArgumentException size = assertThrows(
                IllegalArgumentException.class,
                () -> project(new Document("v", "nope"), new Document("n", new Document("$size", "$v"))));
        assertTrue(size.getMessage().contains("array"));
        assertFalse(size.getMessage().contains("unsupported"));

        final IllegalArgumentException length = assertThrows(
                IllegalArgumentException.class,
                () -> project(new Document(), new Document("n", new Document("$strLenCP", "$absent"))));
        assertTrue(length.getMessage().contains("string"));
        assertFalse(length.getMessage().contains("unsupported"));

        final IllegalArgumentException separator = assertThrows(
                IllegalArgumentException.class,
                () -> project(new Document("v", "ab"), new Document("parts", new Document("$split", List.of("$v", "")))));
        assertTrue(separator.getMessage().contains("non-empty separator"));

        final IllegalArgumentException fraction = assertThrows(
                IllegalArgumentException.class,
                () -> project(
                        new Document("v", "hello"),
                        new Document("text", new Document("$substrCP", List.of("$v", 1.5d, 1)))));
        assertTrue(fraction.getMessage().contains("non-negative integer"));
    }

    @Test
    void nowAndClusterTimeAreStableWithinExecution() {
        final List<Document> result = AggregationPipeline.execute(
                List.of(new Document("_id", 1), new Document("_id", 2)),
                List.of(
                        new Document("$set", new Document("a", "$$NOW").append("b", "$$CLUSTER_TIME")),
                        new Document("$set", new Document("c", "$$NOW"))));

        assertEquals(2, result.size());
        assertInstanceOf(Date.class, result.get(0).get("a"));
        assertEquals(result.get(0).get("a"), result.get(0).get("c"));
        assertEquals(result.get(0).get("a"), result.get(1).get("a"));
        assertEquals(result.get(0).get("b"), result.get(1).get("b"));
        final BsonTimestamp stamp = assertInstanceOf(BsonTimestamp.class, result.get(0).get("b"));
        assertEquals(1, stamp.getInc());
        assertEquals(
                ((Date) result.get(0).get("a")).getTime() / 1000L,
                Integer.toUnsignedLong(stamp.getTime()));
    }

    @Test
    void pipelineUpdateEvaluatesStringDateAndNow() {
        final InMemoryCollectionStore store = new InMemoryCollectionStore();
        store.insertMany(List.of(
                new Document("_id", 1).append("name", "Alpha").append("createdAt", SUMMER),
                new Document("_id", 2).append("name", "Beta").append("createdAt", SUMMER)));

        store.updatePipeline(
                new Document(),
                List.of(new Document(
                        "$set",
                        new Document("name", new Document("$concat", List.of("$name", "-deleted")))
                                .append("lower", new Document("$toLower", "$name"))
                                .append("day", zoned(null, "%Y-%m-%d", "Asia/Seoul"))
                                .append("hour", new Document(
                                        "$hour",
                                        new Document("date", "$createdAt").append("timezone", "Asia/Seoul")))
                                .append("at", "$$NOW")
                                .append("stamp", "$$CLUSTER_TIME"))),
                true,
                false);

        final List<Document> found = store.findAll();
        assertEquals(2, found.size());
        assertEquals("Alpha-deleted", found.get(0).getString("name"));
        assertEquals("Beta-deleted", found.get(1).getString("name"));
        assertEquals("alpha", found.get(0).getString("lower"));
        assertEquals("beta", found.get(1).getString("lower"));
        assertEquals("2026-07-21", found.get(0).getString("day"));
        assertEquals(18, found.get(0).get("hour"));
        assertEquals(found.get(0).get("at"), found.get(1).get("at"));
        assertEquals(found.get(0).get("stamp"), found.get(1).get("stamp"));
        assertInstanceOf(Date.class, found.get(0).get("at"));
        final BsonTimestamp stamp = assertInstanceOf(BsonTimestamp.class, found.get(0).get("stamp"));
        assertEquals(1, stamp.getInc());
    }

    @Test
    void groupCollapsesNumericallyEqualTypes() {
        final List<Document> doubleFirst = groupBy(List.of(
                new Document("v", 1.0d),
                new Document("v", 1),
                new Document("v", 1L),
                new Document("v", Decimal128.parse("1.0"))));
        assertEquals(1, doubleFirst.size());
        assertEquals(Double.class, doubleFirst.get(0).get("_id").getClass());
        assertEquals(1.0d, doubleFirst.get(0).get("_id"));
        assertEquals(4, doubleFirst.get(0).get("n"));

        final List<Document> longFirst = groupBy(List.of(
                new Document("v", 1L),
                new Document("v", 1),
                new Document("v", 1.0d)));
        assertEquals(1, longFirst.size());
        assertEquals(Long.class, longFirst.get(0).get("_id").getClass());

        final List<Document> decimalFirst = groupBy(List.of(
                new Document("v", Decimal128.parse("1")),
                new Document("v", 1)));
        assertEquals(1, decimalFirst.size());
        assertEquals(Decimal128.class, decimalFirst.get(0).get("_id").getClass());

        final List<Document> wide = groupBy(List.of(
                new Document("v", 3000000000.0d),
                new Document("v", 3000000000L)));
        assertEquals(1, wide.size());
        assertEquals(Double.class, wide.get(0).get("_id").getClass());

        final List<Document> fraction = groupBy(List.of(
                new Document("v", 1.5d),
                new Document("v", Decimal128.parse("1.5"))));
        assertEquals(1, fraction.size());
        assertEquals(Double.class, fraction.get(0).get("_id").getClass());

        final List<Document> compound = AggregationPipeline.execute(
                List.of(
                        new Document("a", 1).append("b", "x"),
                        new Document("a", 1.0d).append("b", "x")),
                List.of(new Document(
                        "$group",
                        new Document("_id", new Document("a", "$a").append("b", "$b"))
                                .append("n", new Document("$sum", 1)))));
        assertEquals(1, compound.size());
        assertEquals(2, compound.get(0).get("n"));
        assertEquals(Integer.class, compound.get(0).get("_id", Document.class).get("a").getClass());

        final List<Document> distinct = groupBy(List.of(new Document("v", 1), new Document("v", 2)));
        assertEquals(2, distinct.size());
    }

    private static List<Document> groupBy(final List<Document> source) {
        return AggregationPipeline.execute(
                source,
                List.of(new Document(
                        "$group",
                        new Document("_id", "$v").append("n", new Document("$sum", 1)))));
    }

    private static Document project(final Document source, final Document fields) {
        return AggregationPipeline.execute(List.of(source), List.of(new Document("$project", fields))).get(0);
    }

    private static Document specifiers(final Date ignored, final String ignoredZone) {
        return new Document(
                "$dateToString",
                new Document(
                        "format",
                        "%Y %m %d %H %M %S %L %j %w %u %U %V %G %Z %z %% %b %B")
                        .append("date", "$createdAt"));
    }

    private static Document zoned(final Date ignored, final String format, final String timezone) {
        return new Document(
                "$dateToString",
                new Document("format", format).append("date", "$createdAt").append("timezone", timezone));
    }

    private static Document part(final String operator) {
        return new Document(operator, new Document("date", "$d").append("timezone", "UTC"));
    }

    private static Document seoulParts() {
        return new Document("year", new Document("$year", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("month", new Document("$month", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("day", new Document("$dayOfMonth", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("hour", new Document("$hour", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("minute", new Document("$minute", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("second", new Document("$second", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("millisecond", new Document(
                        "$millisecond", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("dow", new Document("$dayOfWeek", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("doy", new Document("$dayOfYear", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("week", new Document("$week", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("isoWeek", new Document("$isoWeek", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("isoYear", new Document(
                        "$isoWeekYear", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("isoDow", new Document(
                        "$isoDayOfWeek", new Document("date", "$d").append("timezone", "Asia/Seoul")))
                .append("plainYear", new Document("$year", "$d"));
    }

    private static Document trunc(
            final String date,
            final String unit,
            final Integer binSize,
            final String timezone,
            final String startOfWeek) {
        final Document definition = new Document("date", date).append("unit", unit).append("timezone", timezone);
        if (binSize != null) {
            definition.append("binSize", binSize);
        }
        if (startOfWeek != null) {
            definition.append("startOfWeek", startOfWeek);
        }
        return new Document("$dateTrunc", definition);
    }
}
