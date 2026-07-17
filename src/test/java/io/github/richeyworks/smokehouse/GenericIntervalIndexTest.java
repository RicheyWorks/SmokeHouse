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
 * The TYPED-endpoint interval index (Phase 7) against the required brute-force oracle — the
 * same regimes {@code IntervalIndexTest} runs for int spans (duplicate starts, duplicate whole
 * spans, updates that move a span, deletes, reopen-rebuild), but over {@code Long} epoch-millis
 * endpoints far outside int range, which is the whole point of the generalization.
 */
class GenericIntervalIndexTest {

    private static final String IDX = "when";
    /** Base far above Integer.MAX_VALUE so any int truncation would fail loudly. */
    private static final long EPOCH = 1_720_000_000_000L;

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    /** Values look like {@code "1720000000017:1720000000042:v3"} — the span is [a, b]. */
    private static String value(long start, long end, int stamp) {
        return start + ":" + end + ":v" + stamp;
    }

    private static long startOf(String v) {
        return Long.parseLong(v.substring(0, v.indexOf(':')));
    }

    private static long endOf(String v) {
        int a = v.indexOf(':');
        return Long.parseLong(v.substring(a + 1, v.indexOf(':', a + 1)));
    }

    private static IndexedStore<Long, String> openIndexed(Path dir) throws IOException {
        return IndexedStore.open(dir, opts())
                .interval(IDX, Comparator.<Long>naturalOrder(),
                        GenericIntervalIndexTest::startOf, GenericIntervalIndexTest::endOf)
                .build();
    }

    /** Brute-force oracle: filter the live map, order by (start, end, key). */
    private static List<Long> expectedOverlap(TreeMap<Long, String> live, long qlo, long qhi) {
        List<long[]> hits = new ArrayList<>();                        // [start, end, key]
        for (var e : live.entrySet()) {
            long s = startOf(e.getValue());
            long t = endOf(e.getValue());
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
                long key = rnd.nextInt(250);                          // collisions = moved spans
                long start = EPOCH + rnd.nextInt(10_000);             // duplicate starts likely
                long end = start + rnd.nextInt(500);                  // duplicate whole spans too
                String v = value(start, end, i);
                store.put(key, v);
                live.put(key, v);
                if (rnd.nextInt(10) == 0) {
                    long dead = rnd.nextInt(250);
                    store.delete(dead);
                    live.remove(dead);
                }
            }
            for (int q = 0; q < 200; q++) {
                long p = EPOCH + rnd.nextInt(10_500);
                assertEquals(expectedOverlap(live, p, p), store.stab(IDX, p), "stab@" + p);
                long qlo = EPOCH + rnd.nextInt(10_500);
                long qhi = qlo + rnd.nextInt(1_000);
                assertEquals(expectedOverlap(live, qlo, qhi), store.overlapping(IDX, qlo, qhi),
                        "overlap[" + qlo + "," + qhi + "]");
            }
            assertTrue(live.size() > 0, "the mix must leave live records");
        }
    }

    @Test
    void reopenRebuildsTheTypedIndexFromTheLog(@TempDir Path dir) throws IOException {
        TreeMap<Long, String> live = new TreeMap<>();
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            Random rnd = new Random(7);
            for (int i = 0; i < 300; i++) {
                long key = rnd.nextInt(80);
                long start = EPOCH + rnd.nextInt(2_000);
                String v = value(start, start + rnd.nextInt(300), i);
                store.put(key, v);
                live.put(key, v);
            }
        }
        try (IndexedStore<Long, String> reopened = openIndexed(dir)) {   // memory-only: rebuilt
            for (long p = EPOCH; p <= EPOCH + 2_300; p += 97) {
                assertEquals(expectedOverlap(live, p, p), reopened.stab(IDX, p), "stab@" + p);
            }
            assertEquals(expectedOverlap(live, EPOCH, EPOCH + 2_300),
                    reopened.overlapping(IDX, EPOCH, EPOCH + 2_300));
        }
    }

    @Test
    void invertedSpanRejectsThePutWithTheStoreUntouched(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            store.put(1L, value(EPOCH + 10, EPOCH + 20, 0));
            assertThrows(IllegalArgumentException.class,
                    () -> store.put(2L, value(EPOCH + 30, EPOCH + 5, 1)),   // start > end
                    "extractors validate BEFORE the primary write");
            assertEquals(1, store.primary().size(), "rejected put must leave the store untouched");
            assertEquals(List.of(1L), store.stab(IDX, EPOCH + 15));
        }
    }

    @Test
    void typedAndIntLookupsCrossReferenceEachOtherInErrors(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            // Boxed-Long routes to the typed overload; an int call must fail loudly AND
            // point at the typed index that does exist.
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> store.stab(IDX, 5));                            // int overload
            assertTrue(e.getMessage().contains(IDX), "error must name the typed index: " + e);
            IllegalArgumentException g = assertThrows(IllegalArgumentException.class,
                    () -> store.stab("nope", EPOCH));                     // typed overload
            assertTrue(g.getMessage().contains(IDX), "error must list declared typed: " + g);
        }
    }

    @Test
    void retentionStillRefusesTypedIndexes(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class,
                () -> IndexedStore.open(dir, opts().retainNewest(10))
                        .interval(IDX, Comparator.<Long>naturalOrder(),
                                GenericIntervalIndexTest::startOf, GenericIntervalIndexTest::endOf)
                        .build(),
                "retention evictions bypass IndexedStore; typed indexes refuse it like int ones");
    }
}
