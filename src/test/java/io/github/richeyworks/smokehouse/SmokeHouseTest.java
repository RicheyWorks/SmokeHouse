package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store against a {@code TreeMap} oracle: every operation, every reopen, every regime of
 * damage must leave SmokeHouse agreeing with the reference — the log is truth, and recovery
 * (scan → last-writer-wins → SuperBeefSort sort → O(n) build) must reconstruct it exactly.
 */
class SmokeHouseTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)                                   // tiny: exercise rolling
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);       // deterministic for oracle runs
    }

    @Test
    void randomOpsAgreeWithTheOracle(@TempDir Path dir) throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (int i = 0; i < 3_000; i++) {
                long key = rnd.nextInt(400);
                switch (rnd.nextInt(4)) {
                    case 0, 1 -> {                                    // put (incl. upserts)
                        String v = "v" + i;
                        store.put(key, v);
                        oracle.put(key, v);
                    }
                    case 2 -> assertEquals(oracle.remove(key) != null, store.delete(key),
                            "delete disagreement at op " + i);
                    default -> assertEquals(oracle.get(key), store.get(key),
                            "get disagreement at op " + i);
                }
            }
            assertEquals(oracle.size(), store.size());
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), store.get(e.getKey()));
            }
        }
    }

    @Test
    void reopenRecoversTheExactStore(@TempDir Path dir) throws IOException {
        Random rnd = new Random(7);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (int i = 0; i < 2_000; i++) {
                long key = rnd.nextInt(300);
                if (rnd.nextInt(5) == 0) {
                    store.delete(key);
                    oracle.remove(key);
                } else {
                    store.put(key, "gen-" + i);
                    oracle.put(key, "gen-" + i);
                }
            }
        }
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts())) {
            assertEquals(oracle.size(), reopened.size(), "recovery must rebuild the exact live set");
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), reopened.get(e.getKey()),
                        "key " + e.getKey() + " diverged across reopen");
            }
            assertNull(reopened.get(999_999L));
            // Deleted keys stay deleted: tombstones are durable.
            for (long k = 0; k < 300; k++) {
                if (!oracle.containsKey(k)) {
                    assertNull(reopened.get(k), "tombstone for " + k + " did not survive reopen");
                }
            }
        }
    }

    @Test
    void rangeStreamsInKeyOrderAndMatchesTheOracle(@TempDir Path dir) throws IOException {
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (long k = 0; k < 500; k += 3) {
                store.put(k, "val-" + k);
                oracle.put(k, "val-" + k);
            }
            List<Long> keys = new ArrayList<>();
            List<String> vals = new ArrayList<>();
            store.range(100L, 200L, (k, v) -> {
                keys.add(k);
                vals.add(v);
            });
            var expected = oracle.subMap(100L, true, 200L, true);
            assertEquals(new ArrayList<>(expected.keySet()), keys, "range keys, in order");
            assertEquals(new ArrayList<>(expected.values()), vals);
        }
    }

    @Test
    void upsertReplacesAndSizeHolds(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (int gen = 0; gen < 5; gen++) {
                for (long k = 0; k < 50; k++) {
                    store.put(k, "gen" + gen);
                }
            }
            assertEquals(50, store.size(), "upserts must not grow the key count");
            assertEquals("gen4", store.get(7L), "newest write wins");
        }
    }

    @Test
    void adaptiveTierOpensPilotsAndCloses(@TempDir Path dir) throws IOException {
        var adaptive = SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .indexTier(SmokeHouseOptions.IndexTier.ADAPTIVE)
                .pilotCadence(java.time.Duration.ofHours(1));         // scheduler never fires in-test
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, adaptive)) {
            for (long k = 0; k < 200; k++) {
                store.put(k, "v" + k);
            }
            assertTrue(store.get(100L) != null);
            String stats = store.stats();
            assertTrue(stats.contains("tier=ADAPTIVE"), stats);
            assertFalse(stats.contains("stats unavailable"), stats);
        }
        // And the adaptive store recovers like the static one.
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, adaptive)) {
            assertEquals(200, reopened.size());
            assertEquals("v100", reopened.get(100L));
        }
    }
}
