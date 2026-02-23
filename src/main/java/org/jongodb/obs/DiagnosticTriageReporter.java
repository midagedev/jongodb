package org.jongodb.obs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Builds a deterministic failure triage report from command journal state.
 */
public final class DiagnosticTriageReporter {
    public BsonDocument report(final CommandJournal journal) {
        final CommandJournalInvariantChecker checker = new CommandJournalInvariantChecker();
        return report(journal, checker.check(journal));
    }

    public BsonDocument report(
            final CommandJournal journal, final CommandJournalInvariantChecker.InvariantReport invariantReport) {
        Objects.requireNonNull(journal, "journal");
        Objects.requireNonNull(invariantReport, "invariantReport");

        final List<CommandJournal.Entry> entries = journal.entries();
        final int failedCommandCount = countFailed(entries);
        final CommandJournalInvariantChecker.Violation anchorViolation =
                invariantReport.violations().isEmpty() ? null : invariantReport.violations().get(0);
        final CommandJournal.Entry anchorEntry = resolveAnchorEntry(entries, anchorViolation);

        final BsonDocument summary = new BsonDocument()
                .append("status", new BsonString(anchorEntry == null ? "clean" : "failed"))
                .append("invariantViolationCount", new BsonInt32(invariantReport.violations().size()))
                .append("failedCommandCount", new BsonInt32(failedCommandCount));

        if (anchorViolation != null) {
            summary.append("rootCauseType", new BsonString("invariant_violation"));
            summary.append(
                    "likelyRootCause",
                    new BsonString("Invariant " + anchorViolation.code() + ": " + anchorViolation.message()));
            summary.append("violationCode", new BsonString(anchorViolation.code()));
            summary.append("violationCategory", new BsonString(anchorViolation.category()));
        } else if (anchorEntry != null) {
            summary.append("rootCauseType", new BsonString("command_failure"));
            summary.append(
                    "likelyRootCause",
                    new BsonString(anchorEntry.error() == null ? "command failure" : anchorEntry.error()));
        } else {
            summary.append("rootCauseType", new BsonString("none"));
            summary.append("likelyRootCause", new BsonString("no failures or invariant violations observed"));
        }

        if (anchorEntry != null) {
            final CorrelationContext anchorCorrelation = anchorEntry.correlationContext();
            summary.append("anchorSequence", new BsonInt64(anchorEntry.sequence()));
            summary.append("anchorRequestId", new BsonString(anchorCorrelation.requestId()));
            summary.append("anchorCommandName", new BsonString(anchorCorrelation.commandName()));
            anchorCorrelation.sessionId().ifPresent(sessionId -> summary.append("anchorSessionId", new BsonString(sessionId)));
            anchorCorrelation.txnNumber().ifPresent(txnNumber -> summary.append("anchorTxnNumber", new BsonInt64(txnNumber)));
        }

        final BsonArray relatedCommands = new BsonArray();
        for (final CommandJournal.Entry related : relatedCommands(entries, anchorEntry)) {
            relatedCommands.add(toCommandSummary(related));
        }

        final BsonDocument triage = new BsonDocument()
                .append("summary", summary)
                .append("relatedCommands", relatedCommands);

        final BsonDocument sessionPath = sessionPath(entries, anchorEntry);
        if (sessionPath != null) {
            triage.append("sessionPath", sessionPath);
        }
        return triage;
    }

    private static int countFailed(final List<CommandJournal.Entry> entries) {
        int count = 0;
        for (final CommandJournal.Entry entry : entries) {
            if (entry.failed()) {
                count++;
            }
        }
        return count;
    }

    private static CommandJournal.Entry resolveAnchorEntry(
            final List<CommandJournal.Entry> entries, final CommandJournalInvariantChecker.Violation anchorViolation) {
        if (anchorViolation != null) {
            for (final CommandJournal.Entry entry : entries) {
                if (entry.sequence() == anchorViolation.sequence()) {
                    return entry;
                }
            }
        }
        for (int i = entries.size() - 1; i >= 0; i--) {
            final CommandJournal.Entry candidate = entries.get(i);
            if (candidate.failed()) {
                return candidate;
            }
        }
        return null;
    }

    private static List<CommandJournal.Entry> relatedCommands(
            final List<CommandJournal.Entry> entries, final CommandJournal.Entry anchorEntry) {
        if (anchorEntry == null) {
            return List.of();
        }

        final String sessionId = anchorEntry.correlationContext().sessionId().orElse(null);
        if (sessionId != null) {
            final List<CommandJournal.Entry> related = new ArrayList<>();
            for (final CommandJournal.Entry entry : entries) {
                if (sessionId.equals(entry.correlationContext().sessionId().orElse(null))) {
                    related.add(entry);
                }
            }
            return List.copyOf(related);
        }

        int anchorIndex = -1;
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).sequence() == anchorEntry.sequence()) {
                anchorIndex = i;
                break;
            }
        }
        if (anchorIndex < 0) {
            return List.of(anchorEntry);
        }

        final int from = Math.max(0, anchorIndex - 2);
        final int toExclusive = Math.min(entries.size(), anchorIndex + 3);
        return List.copyOf(entries.subList(from, toExclusive));
    }

    private static BsonDocument sessionPath(
            final List<CommandJournal.Entry> entries, final CommandJournal.Entry anchorEntry) {
        if (anchorEntry == null) {
            return null;
        }
        final String sessionId = anchorEntry.correlationContext().sessionId().orElse(null);
        if (sessionId == null) {
            return null;
        }

        final List<CommandJournal.Entry> sessionEntries = new ArrayList<>();
        for (final CommandJournal.Entry entry : entries) {
            if (sessionId.equals(entry.correlationContext().sessionId().orElse(null))) {
                sessionEntries.add(entry);
            }
        }
        if (sessionEntries.isEmpty()) {
            return null;
        }

        final BsonArray commands = new BsonArray(sessionEntries.size());
        for (final CommandJournal.Entry entry : sessionEntries) {
            final BsonDocument command = toCommandSummary(entry);
            command.append("phase", new BsonString(phase(entry)));
            commands.add(command);
        }

        return new BsonDocument()
                .append("sessionId", new BsonString(sessionId))
                .append("terminalState", new BsonString(terminalState(sessionEntries)))
                .append("commandCount", new BsonInt32(sessionEntries.size()))
                .append("commands", commands);
    }

    private static BsonDocument toCommandSummary(final CommandJournal.Entry entry) {
        final CorrelationContext correlation = entry.correlationContext();
        final BsonDocument summary = new BsonDocument()
                .append("sequence", new BsonInt64(entry.sequence()))
                .append("requestId", new BsonString(correlation.requestId()))
                .append("commandName", new BsonString(correlation.commandName()))
                .append("failed", BsonBoolean.valueOf(entry.failed()));

        correlation.sessionId().ifPresent(sessionId -> summary.append("sessionId", new BsonString(sessionId)));
        correlation.txnNumber().ifPresent(txnNumber -> summary.append("txnNumber", new BsonInt64(txnNumber)));

        final String error = entry.error();
        if (error != null) {
            summary.append("error", new BsonString(error));
        }
        return summary;
    }

    private static String phase(final CommandJournal.Entry entry) {
        final String commandName = entry.correlationContext().commandName();
        if ("committransaction".equals(commandName)) {
            return "commit";
        }
        if ("aborttransaction".equals(commandName)) {
            return "abort";
        }

        final BsonDocument input = entry.commandInput();
        final BsonValue startTransaction = input.get("startTransaction");
        if (startTransaction != null && startTransaction.isBoolean() && startTransaction.asBoolean().getValue()) {
            return "start";
        }

        if (input.containsKey("txnNumber")) {
            return "in_transaction";
        }
        return "session";
    }

    private static String terminalState(final List<CommandJournal.Entry> sessionEntries) {
        String state = "observed";
        for (final CommandJournal.Entry entry : sessionEntries) {
            if (!isSuccess(entry)) {
                continue;
            }

            final String commandName = entry.correlationContext().commandName();
            if ("committransaction".equals(commandName)) {
                state = "committed";
                continue;
            }
            if ("aborttransaction".equals(commandName)) {
                state = "aborted";
                continue;
            }
            final BsonValue startTransaction = entry.commandInput().get("startTransaction");
            if (startTransaction != null
                    && startTransaction.isBoolean()
                    && startTransaction.asBoolean().getValue()) {
                state = "in_progress";
            }
        }
        return state;
    }

    private static boolean isSuccess(final CommandJournal.Entry entry) {
        if (entry.failed()) {
            return false;
        }
        final BsonDocument output = entry.commandOutput();
        if (output == null) {
            return false;
        }
        final BsonValue okValue = output.get("ok");
        return okValue != null && okValue.isNumber() && okValue.asNumber().doubleValue() == 1.0d;
    }
}
