package org.jongodb.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class ChangeEventLogTest {
    private static final Namespace NS = Namespace.of("app", "c");

    @Test
    void evictedPositionsFailWithHistoryLostWhileRetainedOnesStillRead() throws InterruptedException {
        final ChangeEventLog log = new ChangeEventLog(Clock.systemUTC(), 2);
        for (int id = 1; id <= 5; id++) {
            log.recordInsert(NS, new Document("_id", id));
        }

        assertThrows(ChangeEventLog.HistoryLostException.class, () -> log.readAfter(0, 10, 0));
        final ChangeEventLog.Read read = log.readAfter(3, 10, 0);
        assertEquals(List.of(4L, 5L), read.events().stream().map(ChangeEventLog.Event::sequence).toList());
        assertEquals(5L, read.position());
    }

    @Test
    void emptyReadKeepsPositionAfterAwaitTimeout() throws InterruptedException {
        final ChangeEventLog log = new ChangeEventLog(Clock.systemUTC());
        log.recordInsert(NS, new Document("_id", 1));

        final ChangeEventLog.Read read = log.readAfter(1, 10, 20);
        assertEquals(List.of(), read.events());
        assertEquals(1L, read.position());
    }

    @Test
    void snapshotWritesStaySilentUntilMergedIntoTheLiveStore() throws InterruptedException {
        final InMemoryEngineStore live = new InMemoryEngineStore();
        live.collection(NS).insertMany(List.of(new Document("_id", 1).append("v", 1)));
        final long before = live.changeEventLog().lastSequence();

        final InMemoryEngineStore baseline = live.snapshot();
        final InMemoryEngineStore transaction = baseline.snapshot();
        transaction.collection(NS).update(new Document("_id", 1), new Document("$set", new Document("v", 2)), false, false);
        transaction.collection(NS).insertMany(List.of(new Document("_id", 2)));
        transaction.collection(NS).deleteMany(new Document("_id", 1));
        assertEquals(before, live.changeEventLog().lastSequence());

        live.mergeTransactionSnapshot(baseline, transaction);
        final List<String> operations = live.changeEventLog().readAfter(before, 10, 0).events().stream()
                .map(ChangeEventLog.Event::operationType)
                .sorted()
                .toList();
        assertEquals(List.of("delete", "insert"), operations);
    }
}
