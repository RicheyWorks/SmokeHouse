package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The tail: every committed mutation is streamed exactly once, in sequence order, and a late
 * subscriber replays the history the ring still holds. Delivery is asynchronous (a tail thread), so
 * the tests wait on a latch rather than assuming synchronous callbacks.
 */
class TailTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    /** Collects events in arrival order; a latch fires once the expected count arrives. */
    private static final class Collector implements TailListener<Long, String> {
        final List<TailEvent<Long, String>> events = new CopyOnWriteArrayList<>();
        final CountDownLatch done;
        volatile boolean gapped;

        Collector(int expected) {
            this.done = new CountDownLatch(expected);
        }

        @Override
        public void onEvent(TailEvent<Long, String> e) {
            events.add(e);
            done.countDown();
        }

        @Override
        public void onGap() {
            gapped = true;
        }
    }

    @Test
    void tailStreamsEveryMutationInOrder(@TempDir Path dir) throws Exception {
        int n = 500;
        Collector c = new Collector(n);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             AutoCloseable sub = store.tail(0, c)) {
            for (int i = 0; i < n; i++) {
                if (i % 5 == 4) {
                    store.delete((long) i);                // a tombstone (of an absent key) is still a mutation
                } else {
                    store.put((long) i, "v" + i);
                }
            }
            assertTrue(c.done.await(10, TimeUnit.SECONDS), "every event should be delivered");
        }

        assertEquals(n, c.events.size());
        for (int i = 0; i < n; i++) {
            TailEvent<Long, String> e = c.events.get(i);
            assertEquals((long) i, e.sequence(), "sequences arrive in order, no gaps");
            assertEquals((long) i, (long) e.key());
            if (i % 5 == 4) {
                assertTrue(e.deleted());
                assertNull(e.value());
            } else {
                assertFalse(e.deleted());
                assertEquals("v" + i, e.value());
            }
        }
        assertFalse(c.gapped, "a consumer that keeps up never gaps");
    }

    @Test
    void lateSubscriberReplaysRingHistory(@TempDir Path dir) throws Exception {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (int i = 0; i < 10; i++) {
                store.put((long) i, "v" + i);              // sequences 0..9, before anyone subscribes
            }
            assertEquals(10L, store.tailSequence());

            Collector c = new Collector(7);                // expect the tail of the ring: sequences 3..9
            try (AutoCloseable sub = store.tail(3, c)) {
                assertTrue(c.done.await(10, TimeUnit.SECONDS), "ring history replays from seq 3");
            }
            assertEquals(7, c.events.size());
            assertEquals(3L, c.events.get(0).sequence());
            assertEquals(9L, c.events.get(6).sequence());
            assertFalse(c.gapped, "seq 3 is still within the ring, so no gap");
        }
    }
}
