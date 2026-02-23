package org.jongodb.obs;

import java.util.Objects;

/**
 * Exports a replayable JSON-lines stream from recorded command inputs.
 */
public final class ReproExporter {
    public String exportJsonLines(final CommandJournal journal) {
        Objects.requireNonNull(journal, "journal");

        final StringBuilder lines = new StringBuilder();
        boolean first = true;
        for (CommandJournal.Entry entry : journal.entries()) {
            if (!first) {
                lines.append('\n');
            }
            first = false;
            lines.append(entry.commandInput().toJson());
        }
        return lines.toString();
    }
}
