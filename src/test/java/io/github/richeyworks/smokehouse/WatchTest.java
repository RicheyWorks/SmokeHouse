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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Watchers: a {@code watch(key)} sees only that key's mutations, a {@code watchRange(lo, hi)} sees
 * only in-range keys — both filtered views of the tail, delivered off the store lock.
 */
class WatchTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    private static final class Collector implements TailListener<Long, String> {
        final List<TailEvent<Long, String>> events = new CopyOnWriteArrayList<>();
        final CountDownLatch done;

        Collector(int expected) {
            this.done = new CountDownLatch(expected);
        }

        @Override
        public void onEvent(TailEvent<Long, String> e) {
            events.add(e);
            done.countDown();
        }
    }

    @Test
    void watchDeliversOnlyTheWatchedKey(@TempDir Path dir) throws Exception {
        Collector c = new Collector(3);                    // key 42: two puts + a delete
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             AutoCloseable w = store.watch(42L, c)) {
            store.put(1L, "a");                            // other keys — filtered out
            store.put(42L, "x");
            store.put(2L, "b");
            store.put(42L, "y");
            store.delete(42L);
            store.put(3L, "c");
            assertTrue(c.done.await(10, TimeUnit.SECONDS), "the three key-42 events arrive");
        }
        assertEquals(3, c.events.size());
        for (TailEvent<Long, String> e : c.events) {
            assertEquals(42L, (long) e.key(), "only the watched key");
        }
        assertEquals("x", c.events.get(0).value());
        assertEquals("y", c.events.get(1).value());
        assertTrue(c.events.get(2).deleted());
    }

    @Test
    void watchRangeDeliversOnlyInRange(@TempDir Path dir) throws Exception {
        Collector c = new Collector(11);                   // keys 10..20 inclusive
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             AutoCloseable w = store.watchRange(10L, 20L, c)) {
            for (long k = 0; k < 30; k++) {
                store.put(k, "v" + k);
            }
            assertTrue(c.done.await(10, TimeUnit.SECONDS), "the eleven in-range events arrive");
        }
        assertEquals(11, c.events.size());
        for (TailEvent<Long, String> e : c.events) {
            long k = e.key();
            assertTrue(k >= 10 && k <= 20, "only in-range keys, got " + k);
        }
        assertEquals(10L, (long) c.events.get(0).key());
        assertEquals(20L, (long) c.events.get(10).key());
    }
}
