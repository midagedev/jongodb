package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.BsonDocument;
import org.jongodb.server.WireCommandIngress;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

/**
 * Repeatable benchmark runner for R1 performance gates.
 */
public class R1BenchmarkRunner {
    private static final BsonDocument PING_COMMAND = BsonDocument.parse("{\"ping\":1,\"$db\":\"admin\"}");
    private static final BsonDocument SEED_COMMAND = BsonDocument.parse(
        "{\"insert\":\"users\",\"$db\":\"bench\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"},{\"_id\":3,\"name\":\"c\"}]}"
    );
    private static final List<BsonDocument> CRUD_COMMANDS = List.of(
        BsonDocument.parse("{\"find\":\"users\",\"$db\":\"bench\",\"filter\":{\"name\":\"a\"}}"),
        BsonDocument.parse(
            "{\"update\":\"users\",\"$db\":\"bench\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"aa\"}},\"multi\":false}]}"
        ),
        BsonDocument.parse("{\"find\":\"users\",\"$db\":\"bench\",\"filter\":{\"_id\":1}}"),
        BsonDocument.parse("{\"delete\":\"users\",\"$db\":\"bench\",\"deletes\":[{\"q\":{\"_id\":3},\"limit\":1}]}"),
        BsonDocument.parse("{\"insert\":\"users\",\"$db\":\"bench\",\"documents\":[{\"_id\":3,\"name\":\"c\"}]}")
    );

    public BenchmarkResult run(BenchmarkConfig config) {
        Objects.requireNonNull(config, "config");
        List<Double> coldStartSamples = measureColdStartMillis(config.coldStartSamples());
        List<Double> resetSamples = measureResetMillis(config.resetSamples());
        CrudPathMeasurement crudMeasurement = measureCrudPath(config.warmupOperations(), config.measuredOperations());

        double coldStartP50Millis = percentile(coldStartSamples, 0.50d);
        double resetP50Millis = percentile(resetSamples, 0.50d);
        double crudP95LatencyMillis = percentile(crudMeasurement.latencySamplesMillis(), 0.95d);
        double throughputOpsPerSecond = throughputOpsPerSecond(
            config.measuredOperations(),
            crudMeasurement.totalDurationNanos()
        );

        return new BenchmarkResult(
            coldStartSamples,
            resetSamples,
            crudMeasurement.latencySamplesMillis(),
            config.warmupOperations(),
            config.measuredOperations(),
            coldStartP50Millis,
            resetP50Millis,
            crudP95LatencyMillis,
            throughputOpsPerSecond
        );
    }

    static double percentile(List<Double> samples, double percentile) {
        Objects.requireNonNull(samples, "samples");
        if (percentile <= 0.0d || percentile > 1.0d || !Double.isFinite(percentile)) {
            throw new IllegalArgumentException("percentile must be in range (0.0, 1.0]");
        }
        if (samples.isEmpty()) {
            return 0.0d;
        }

        List<Double> sorted = new ArrayList<>(samples.size());
        for (Double sample : samples) {
            if (sample == null || !Double.isFinite(sample) || sample < 0.0d) {
                throw new IllegalArgumentException("samples must contain finite non-negative numbers only");
            }
            sorted.add(sample);
        }
        sorted.sort(Double::compareTo);

        int index = (int) Math.ceil(sorted.size() * percentile) - 1;
        int boundedIndex = Math.max(0, Math.min(sorted.size() - 1, index));
        return sorted.get(boundedIndex);
    }

    static double throughputOpsPerSecond(int operationCount, long durationNanos) {
        if (operationCount <= 0) {
            throw new IllegalArgumentException("operationCount must be > 0");
        }
        if (durationNanos < 0L) {
            throw new IllegalArgumentException("durationNanos must be >= 0");
        }
        long normalizedDurationNanos = Math.max(1L, durationNanos);
        return (double) operationCount / (normalizedDurationNanos / 1_000_000_000.0d);
    }

    private static List<Double> measureColdStartMillis(int sampleCount) {
        List<Double> samples = new ArrayList<>(sampleCount);
        OpMsgCodec codec = new OpMsgCodec();
        int requestId = 1;
        for (int i = 0; i < sampleCount; i++) {
            long startedAtNanos = System.nanoTime();
            WireCommandIngress ingress = WireCommandIngress.inMemory();
            send(ingress, codec, requestId++, PING_COMMAND);
            samples.add(toMillis(System.nanoTime() - startedAtNanos));
        }
        return List.copyOf(samples);
    }

    private static List<Double> measureResetMillis(int sampleCount) {
        List<Double> samples = new ArrayList<>(sampleCount);
        OpMsgCodec codec = new OpMsgCodec();
        int requestId = 10_000;
        for (int i = 0; i < sampleCount; i++) {
            WireCommandIngress dirtyIngress = WireCommandIngress.inMemory();
            send(dirtyIngress, codec, requestId++, SEED_COMMAND);
            send(dirtyIngress, codec, requestId++, CRUD_COMMANDS.get(i % CRUD_COMMANDS.size()));

            long startedAtNanos = System.nanoTime();
            WireCommandIngress resetIngress = WireCommandIngress.inMemory();
            send(resetIngress, codec, requestId++, PING_COMMAND);
            samples.add(toMillis(System.nanoTime() - startedAtNanos));
        }
        return List.copyOf(samples);
    }

    private static CrudPathMeasurement measureCrudPath(int warmupOperations, int measuredOperations) {
        WireCommandIngress ingress = WireCommandIngress.inMemory();
        OpMsgCodec codec = new OpMsgCodec();
        int requestId = 20_000;

        send(ingress, codec, requestId++, SEED_COMMAND);
        for (int i = 0; i < warmupOperations; i++) {
            send(ingress, codec, requestId++, CRUD_COMMANDS.get(i % CRUD_COMMANDS.size()));
        }

        List<Double> latencySamples = new ArrayList<>(measuredOperations);
        long startedAtNanos = System.nanoTime();
        for (int i = 0; i < measuredOperations; i++) {
            BsonDocument command = CRUD_COMMANDS.get(i % CRUD_COMMANDS.size());
            long opStartNanos = System.nanoTime();
            send(ingress, codec, requestId++, command);
            latencySamples.add(toMillis(System.nanoTime() - opStartNanos));
        }
        long totalDurationNanos = System.nanoTime() - startedAtNanos;
        return new CrudPathMeasurement(List.copyOf(latencySamples), Math.max(0L, totalDurationNanos));
    }

    private static void send(WireCommandIngress ingress, OpMsgCodec codec, int requestId, BsonDocument commandBody) {
        OpMsg request = new OpMsg(requestId, 0, 0, commandBody.clone());
        codec.decode(ingress.handle(codec.encode(request)));
    }

    private static double toMillis(long nanos) {
        return Math.max(0L, nanos) / 1_000_000.0d;
    }

    public static final class BenchmarkConfig {
        private final int coldStartSamples;
        private final int resetSamples;
        private final int warmupOperations;
        private final int measuredOperations;

        public BenchmarkConfig(int coldStartSamples, int resetSamples, int warmupOperations, int measuredOperations) {
            if (coldStartSamples <= 0) {
                throw new IllegalArgumentException("coldStartSamples must be > 0");
            }
            if (resetSamples <= 0) {
                throw new IllegalArgumentException("resetSamples must be > 0");
            }
            if (warmupOperations < 0) {
                throw new IllegalArgumentException("warmupOperations must be >= 0");
            }
            if (measuredOperations <= 0) {
                throw new IllegalArgumentException("measuredOperations must be > 0");
            }
            this.coldStartSamples = coldStartSamples;
            this.resetSamples = resetSamples;
            this.warmupOperations = warmupOperations;
            this.measuredOperations = measuredOperations;
        }

        public int coldStartSamples() {
            return coldStartSamples;
        }

        public int resetSamples() {
            return resetSamples;
        }

        public int warmupOperations() {
            return warmupOperations;
        }

        public int measuredOperations() {
            return measuredOperations;
        }
    }

    public static final class BenchmarkResult {
        private final List<Double> coldStartSamplesMillis;
        private final List<Double> resetSamplesMillis;
        private final List<Double> crudLatencySamplesMillis;
        private final int warmupOperations;
        private final int measuredOperations;
        private final double coldStartMillis;
        private final double resetMillis;
        private final double crudP95LatencyMillis;
        private final double throughputOpsPerSecond;

        BenchmarkResult(
            List<Double> coldStartSamplesMillis,
            List<Double> resetSamplesMillis,
            List<Double> crudLatencySamplesMillis,
            int warmupOperations,
            int measuredOperations,
            double coldStartMillis,
            double resetMillis,
            double crudP95LatencyMillis,
            double throughputOpsPerSecond
        ) {
            this.coldStartSamplesMillis = copySamples(coldStartSamplesMillis, "coldStartSamplesMillis");
            this.resetSamplesMillis = copySamples(resetSamplesMillis, "resetSamplesMillis");
            this.crudLatencySamplesMillis = copySamples(crudLatencySamplesMillis, "crudLatencySamplesMillis");
            if (warmupOperations < 0) {
                throw new IllegalArgumentException("warmupOperations must be >= 0");
            }
            if (measuredOperations <= 0) {
                throw new IllegalArgumentException("measuredOperations must be > 0");
            }
            this.warmupOperations = warmupOperations;
            this.measuredOperations = measuredOperations;
            this.coldStartMillis = requireFiniteNonNegative(coldStartMillis, "coldStartMillis");
            this.resetMillis = requireFiniteNonNegative(resetMillis, "resetMillis");
            this.crudP95LatencyMillis = requireFiniteNonNegative(crudP95LatencyMillis, "crudP95LatencyMillis");
            this.throughputOpsPerSecond = requireFiniteNonNegative(throughputOpsPerSecond, "throughputOpsPerSecond");
        }

        public List<Double> coldStartSamplesMillis() {
            return coldStartSamplesMillis;
        }

        public List<Double> resetSamplesMillis() {
            return resetSamplesMillis;
        }

        public List<Double> crudLatencySamplesMillis() {
            return crudLatencySamplesMillis;
        }

        public int warmupOperations() {
            return warmupOperations;
        }

        public int measuredOperations() {
            return measuredOperations;
        }

        public double coldStartMillis() {
            return coldStartMillis;
        }

        public double resetMillis() {
            return resetMillis;
        }

        public double crudP95LatencyMillis() {
            return crudP95LatencyMillis;
        }

        public double throughputOpsPerSecond() {
            return throughputOpsPerSecond;
        }

        private static List<Double> copySamples(List<Double> source, String fieldName) {
            Objects.requireNonNull(source, fieldName);
            List<Double> copy = new ArrayList<>(source.size());
            for (Double value : source) {
                copy.add(requireFiniteNonNegative(Objects.requireNonNull(value, fieldName + " item"), fieldName + " item"));
            }
            return List.copyOf(copy);
        }
    }

    private static final class CrudPathMeasurement {
        private final List<Double> latencySamplesMillis;
        private final long totalDurationNanos;

        private CrudPathMeasurement(List<Double> latencySamplesMillis, long totalDurationNanos) {
            this.latencySamplesMillis = Objects.requireNonNull(latencySamplesMillis, "latencySamplesMillis");
            if (totalDurationNanos < 0L) {
                throw new IllegalArgumentException("totalDurationNanos must be >= 0");
            }
            this.totalDurationNanos = totalDurationNanos;
        }

        private List<Double> latencySamplesMillis() {
            return latencySamplesMillis;
        }

        private long totalDurationNanos() {
            return totalDurationNanos;
        }
    }

    private static double requireFiniteNonNegative(double value, String fieldName) {
        if (!Double.isFinite(value) || value < 0.0d) {
            throw new IllegalArgumentException(fieldName + " must be finite and >= 0.0");
        }
        return value;
    }
}
