package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The interval index (Phase 4.2) against the required brute-force oracle: a plain list filter
 * over the live {@code (start, end)} spans. Stabbing and overlap queries must agree on every
 * regime — duplicate starts, duplicate whole spans, updates that move a span, deletes, and a
 * reopen that rebuilds the tree from the log.
 */
class IntervalIndexTest {

    private static final String IDX = "span";

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    /** Values look like {@code "17:42:v3"} — the span is [17, 42]. */
    private static String value(int start, int end, int stamp) {
        return start + ":" + end + ":v" + stamp;
    }

    private static int startOf(String v) {
        return Integer.parseInt(v.substring(0, v.indexOf(':')));
    }

    private static int endOf(String v) {
        int a = v.indexOf(':');
        return Integer.parseInt(v.substring(a + 1, v.indexOf(':', a + 1)));
    }

    private static IndexedStore<Long, String> openIndexed(Path dir) throws IOException {
        return IndexedStore.open(dir, opts())
                .interval(IDX, IntervalIndexTest::startOf, IntervalIndexTest::endOf)
                .build();
    }

    /** Brute-force oracle: filter the live map, order by (start, end, key). */
    private static List<Long> expectedOverlap(TreeMap<Long, String> live, int qlo, int qhi) {
        List<long[]> hits = new ArrayList<>();                        // [start, end, key]
        for (var e : live.entrySet()) {
            int s = startOf(e.getValue());
            int t = endOf(e.getValue());
            if (s <= qhi && t >= qlo) {
                hits.add(new long[]{s, t, e.getKey()});
            }
        }
        hits.sort(Comparator.<long[]>comparingLong(h -> h[0])
                .thenComparingLong(h -> h[1]).thenComparingLong(h -> h[2]));
        List<Long> keys = new ArrayList<>(hits.size());
        for (long[] h : hits) {
            keys.add(h[2]);
        }
        return keys;
    }

    @Test
    void stabAndOverlapAgreeWithTheBruteForceOracle(@TempDir Path dir) throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> live = new TreeMap<>();
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            for (int i = 0; i < 2_000; i++) {
                long key = rnd.nextInt(250);
                if (rnd.nextInt(4) == 0) {
                    store.delete(key);
                    live.remove(key);
                } else {
                    int s = rnd.nextInt(200);                          // dense: duplicate starts guaranteed
                    String v = value(s, s + rnd.nextInt(30), i);
                    store.put(key, v);
                    live.put(key, v);
                }
                if (i % 250 == 0) {
                    int p = rnd.nextInt(240) - 5;
                    assertEquals(expectedOverlap(live, p, p), store.stab(IDX, p),
                            "mid-run stab(" + p + ") at op " + i);
                }
            }
            for (int i = 0; i < 80; i++) {
                int p = rnd.nextInt(260) - 10;
                assertEquals(expectedOverlap(live, p, p), store.stab(IDX, p), "stab(" + p + ")");
            }
            for (int i = 0; i < 80; i++) {
                int a = rnd.nextInt(260) - 10;
                int b = rnd.nextInt(260) - 10;
                int lo = Math.min(a, b);
                int hi = Math.max(a, b);
                assertEquals(expectedOverlap(live, lo, hi), store.overlapping(IDX, lo, hi),
                        "overlapping[" + lo + ", " + hi + "]");
            }
        }
    }

    @Test
    void duplicateStartsAndDuplicateSpansAllResolve(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            store.put(1L, value(10, 20, 1));                           // three spans share start=10
            store.put(2L, value(10, 15, 2));
            store.put(3L, value(10, 20, 3));                           // exact duplicate of key 1's span
            store.put(4L, value(12, 13, 4));
            assertEquals(List.of(1L, 3L), store.stab(IDX, 16),
                    "start=10: only ends >= 16 hit — end=15 misses; (start,end,key) order");
            assertEquals(List.of(2L, 1L, 3L, 4L), store.stab(IDX, 12));
            store.delete(3L);
            assertEquals(List.of(1L), store.stab(IDX, 16), "duplicate span retracts only its key");
        }
    }

    @Test
    void updateMovesTheSpan(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            store.put(7L, value(100, 200, 1));
            assertEquals(List.of(7L), store.stab(IDX, 150));
            store.put(7L, value(300, 400, 2));                         // same key, new span
            assertTrue(store.stab(IDX, 150).isEmpty(), "stale span must be retracted");
            assertEquals(List.of(7L), store.stab(IDX, 350));
        }
    }

    @Test
    void invertedSpanIsRejectedWithTheStoreUntouched(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            store.put(5L, value(10, 20, 1));
            assertThrows(IllegalArgumentException.class, () -> store.put(5L, value(30, 20, 2)),
                    "start > end must be rejected");
            assertEquals(value(10, 20, 1), store.get(5L),
                    "extractors run before the primary write: the store must be untouched");
            assertEquals(List.of(5L), store.stab(IDX, 15), "the index must be untouched too");
        }
    }

    @Test
    void reopenRebuildsTheIntervalTreeIdentically(@TempDir Path dir) throws IOException {
        Random rnd = new Random(7);
        TreeMap<Long, String> live = new TreeMap<>();
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            for (int i = 0; i < 700; i++) {
                long key = rnd.nextInt(150);
                if (rnd.nextInt(5) == 0) {
                    store.delete(key);
                    live.remove(key);
                } else {
                    int s = rnd.nextInt(120);
                    String v = value(s, s + rnd.nextInt(25), i);
                    store.put(key, v);
                    live.put(key, v);
                }
            }
        }
        try (IndexedStore<Long, String> reopened = openIndexed(dir)) {
            for (int i = 0; i < 50; i++) {
                int a = rnd.nextInt(150) - 5;
                int b = rnd.nextInt(150) - 5;
                int lo = Math.min(a, b);
                int hi = Math.max(a, b);
                assertEquals(expectedOverlap(live, lo, hi), reopened.overlapping(IDX, lo, hi),
                        "rebuilt overlapping[" + lo + ", " + hi + "]");
                assertEquals(expectedOverlap(live, lo, lo), reopened.stab(IDX, lo),
                        "rebuilt stab(" + lo + ")");
            }
        }
    }

    @Test
    void intervalCoexistsWithAPlainSecondary(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = IndexedStore.open(dir, opts())
                .interval(IDX, IntervalIndexTest::startOf, IntervalIndexTest::endOf)
                .secondary("end", Comparator.<Integer>naturalOrder(), IntervalIndexTest::endOf)
                .build()) {
            store.put(1L, value(5, 50, 1));
            store.put(2L, value(30, 40, 2));
            assertEquals(List.of(1L, 2L), store.stab(IDX, 35));
            assertEquals(List.of(2L, 1L), store.byAttribute("end", 40, 50), "attr order: 40 then 50");
        }
    }

    @Test
    void retentionRefusesIntervalIndexes(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class, () -> IndexedStore
                .open(dir, opts().retainNewest(5))
                .interval(IDX, IntervalIndexTest::startOf, IntervalIndexTest::endOf)
                .build());
    }

    @Test
    void unknownIntervalNameAndBadRangeFailLoudly(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            assertThrows(IllegalArgumentException.class, () -> store.stab("nope", 1));
            assertThrows(IllegalArgumentException.class, () -> store.overlapping(IDX, 9, 3));
        }
    }
}
