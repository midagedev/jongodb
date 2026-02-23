package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Flake-rate evaluator based on repeated differential runs.
 */
public class R1FlakeRateEvaluator {
    public FlakeEvaluation run(DifferentialHarness harness, List<Scenario> scenarios, int rerunCount) {
        Objects.requireNonNull(harness, "harness");
        Objects.requireNonNull(scenarios, "scenarios");
        if (rerunCount < 0) {
            throw new IllegalArgumentException("rerunCount must be >= 0");
        }

        DifferentialReport baseline = harness.run(scenarios);
        List<DifferentialReport> reruns = new ArrayList<>(rerunCount);
        for (int i = 0; i < rerunCount; i++) {
            reruns.add(harness.run(scenarios));
        }

        FlakeSummary summary = evaluate(baseline, reruns);
        return new FlakeEvaluation(baseline, reruns, summary);
    }

    public FlakeSummary evaluate(DifferentialReport baselineReport, List<DifferentialReport> rerunReports) {
        Objects.requireNonNull(baselineReport, "baselineReport");
        Objects.requireNonNull(rerunReports, "rerunReports");

        Map<String, String> baselineFingerprints = new LinkedHashMap<>();
        for (DiffResult result : baselineReport.results()) {
            baselineFingerprints.put(result.scenarioId(), fingerprint(result));
        }

        int observations = 0;
        int flakyObservations = 0;
        for (DifferentialReport rerunReport : rerunReports) {
            Objects.requireNonNull(rerunReport, "rerunReport");
            for (DiffResult result : rerunReport.results()) {
                observations++;
                String baselineFingerprint = baselineFingerprints.get(result.scenarioId());
                if (baselineFingerprint == null) {
                    flakyObservations++;
                    continue;
                }
                if (!baselineFingerprint.equals(fingerprint(result))) {
                    flakyObservations++;
                }
            }
        }

        double rate = observations == 0 ? 0.0d : (double) flakyObservations / (double) observations;
        return new FlakeSummary(rerunReports.size(), observations, flakyObservations, rate);
    }

    private static String fingerprint(DiffResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append(result.status().name());
        result.errorMessage().ifPresent(message -> sb.append("|error=").append(message));
        for (DiffEntry entry : result.entries()) {
            sb.append("|path=").append(entry.path());
            sb.append("|left=").append(QualityGateArtifactRenderer.JsonEncoder.encode(entry.leftValue()));
            sb.append("|right=").append(QualityGateArtifactRenderer.JsonEncoder.encode(entry.rightValue()));
            sb.append("|note=").append(entry.note());
        }
        return sb.toString();
    }

    public static final class FlakeEvaluation {
        private final DifferentialReport baselineReport;
        private final List<DifferentialReport> rerunReports;
        private final FlakeSummary summary;

        FlakeEvaluation(
            DifferentialReport baselineReport,
            List<DifferentialReport> rerunReports,
            FlakeSummary summary
        ) {
            this.baselineReport = Objects.requireNonNull(baselineReport, "baselineReport");
            this.rerunReports = copyReports(rerunReports);
            this.summary = Objects.requireNonNull(summary, "summary");
        }

        public DifferentialReport baselineReport() {
            return baselineReport;
        }

        public List<DifferentialReport> rerunReports() {
            return rerunReports;
        }

        public FlakeSummary summary() {
            return summary;
        }

        private static List<DifferentialReport> copyReports(List<DifferentialReport> source) {
            Objects.requireNonNull(source, "rerunReports");
            List<DifferentialReport> copy = new ArrayList<>(source.size());
            for (DifferentialReport report : source) {
                copy.add(Objects.requireNonNull(report, "rerunReport"));
            }
            return List.copyOf(copy);
        }
    }

    public static final class FlakeSummary {
        private final int runs;
        private final int observations;
        private final int flakyObservations;
        private final double rate;

        FlakeSummary(int runs, int observations, int flakyObservations, double rate) {
            if (runs < 0) {
                throw new IllegalArgumentException("runs must be >= 0");
            }
            if (observations < 0) {
                throw new IllegalArgumentException("observations must be >= 0");
            }
            if (flakyObservations < 0 || flakyObservations > observations) {
                throw new IllegalArgumentException("flakyObservations must be in range [0, observations]");
            }
            if (!Double.isFinite(rate) || rate < 0.0d || rate > 1.0d) {
                throw new IllegalArgumentException("rate must be finite and in range [0.0, 1.0]");
            }
            this.runs = runs;
            this.observations = observations;
            this.flakyObservations = flakyObservations;
            this.rate = rate;
        }

        public int runs() {
            return runs;
        }

        public int observations() {
            return observations;
        }

        public int flakyObservations() {
            return flakyObservations;
        }

        public double rate() {
            return rate;
        }
    }
}
