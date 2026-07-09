package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2: hints accelerate warm starts but are never trusted (corrupt one → full scan);
 * compaction reclaims dead bytes without ever changing what the store contains — including
 * across the recovery scan, where dropped tombstones must not resurrect anything; retention
 * keeps exactly the newest-written N through sessions; interval fsync just works.
 */
class Phase2Test {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    private static TreeMap<Long, String> churn(SmokeHouse<Long, String> store, long seed, int ops)
            throws IOException {
        Random rnd = new Random(seed);
        TreeMap<Long, String> oracle = new TreeMap<>();
        for (int i = 0; i < ops; i++) {
            long key = rnd.nextInt(250);
            if (rnd.nextInt(4) == 0) {
                store.delete(key);
                oracle.remove(key);
            } else {
                store.put(key, "v" + i);
                oracle.put(key, "v" + i);
            }
        }
        return oracle;
    }

    private static void assertAgrees(SmokeHouse<Long, String> store, TreeMap<Long, String> oracle)
            throws IOException {
        assertEquals(oracle.size(), store.size());
        for (var e : oracle.entrySet()) {
            assertEquals(e.getValue(), store.get(e.getKey()), "key " + e.getKey());
        }
        for (long k = 0; k < 250; k++) {
            if (!oracle.containsKey(k)) {
                assertNull(store.get(k), "ghost key " + k);
            }
        }
    }

    @Test
    void hintAcceleratedWarmStartsStayTruthful(@TempDir Path dir) throws IOException {
        TreeMap<Long, String> oracle;
        try (SmokeHouse<Long, String> s1 = SmokeHouse.open(dir, opts())) {
            oracle = churn(s1, 1L, 2_000);
        }
        assertTrue(Files.exists(dir.resolve(SegmentLog.HINT_FILE)), "clean close writes the hint");

        // Session 2: warm start off the hint, keep writing (the next hint covers the delta).
        try (SmokeHouse<Long, String> s2 = SmokeHouse.open(dir, opts())) {
            assertAgrees(s2, oracle);
            Random rnd = new Random(2);
            for (int i = 0; i < 500; i++) {
                long key = rnd.nextInt(250);
                s2.put(key, "s2-" + i);
                oracle.put(key, "s2-" + i);
            }
        }
        // Session 3: warm start over checkpoint + delta.
        try (SmokeHouse<Long, String> s3 = SmokeHouse.open(dir, opts())) {
            assertAgrees(s3, oracle);
        }
    }

    @Test
    void corruptHintFallsBackToTheFullScan(@TempDir Path dir) throws IOException {
        TreeMap<Long, String> oracle;
        try (SmokeHouse<Long, String> s1 = SmokeHouse.open(dir, opts())) {
            oracle = churn(s1, 3L, 1_500);
        }
        Path hint = dir.resolve(SegmentLog.HINT_FILE);
        byte[] bytes = Files.readAllBytes(hint);
        bytes[bytes.length / 2] ^= 0x55;                          // damage the checkpoint
        Files.write(hint, bytes);

        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts())) {
            assertAgrees(reopened, oracle);                       // truth never depended on the hint
        }
    }

    @Test
    void compactionReclaimsWithoutChangingTruth(@TempDir Path dir) throws IOException {
        TreeMap<Long, String> oracle;
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            oracle = churn(store, 4L, 4_000);                     // heavy overwrites + deletes
            long garbageBefore = store.garbageBytes();
            assertTrue(garbageBefore > 0, "churn must have produced dead bytes");

            long reclaimed = store.compact();
            assertTrue(reclaimed > 0, "compaction must reclaim bytes; got " + reclaimed);
            assertTrue(store.garbageBytes() < garbageBefore);
            assertAgrees(store, oracle);                          // live truth untouched
            assertFalse(Files.exists(dir.resolve(SegmentLog.COMPACT_TMP)));
            assertFalse(Files.exists(dir.resolve(SegmentLog.COMPACT_READY)));
        }
        // The critical case: FULL-SCAN recovery after compaction (drop the hint the clean close
        // just wrote, so the scan is genuine). Dropped tombstones must not resurrect deleted
        // keys — safe because compaction only ever covers a full prefix of closed segments, so
        // nothing older than a dropped tombstone can exist to resurrect.
        Files.deleteIfExists(dir.resolve(SegmentLog.HINT_FILE));
        try (SmokeHouse<Long, String> coldScan = SmokeHouse.open(dir, opts())) {
            assertAgrees(coldScan, oracle);
        }
        // And the warm path too: reopen once more off the fresh hint that close just wrote.
        try (SmokeHouse<Long, String> warm = SmokeHouse.open(dir, opts())) {
            assertAgrees(warm, oracle);
        }
    }

    @Test
    void uncommittedCompactionScratchIsDiscardedOnOpen(@TempDir Path dir) throws IOException {
        TreeMap<Long, String> oracle;
        try (SmokeHouse<Long, String> s1 = SmokeHouse.open(dir, opts())) {
            oracle = churn(s1, 5L, 1_000);
        }
        // Simulate a crash mid-copy: a scratch file exists, but no ready marker was ever written.
        Files.write(dir.resolve(SegmentLog.COMPACT_TMP), new byte[]{1, 2, 3});
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts())) {
            assertFalse(Files.exists(dir.resolve(SegmentLog.COMPACT_TMP)), "scratch discarded");
            assertAgrees(reopened, oracle);
        }
    }

    @Test
    void retentionKeepsExactlyTheNewestWrittenAcrossSessions(@TempDir Path dir) throws IOException {
        var retained = opts().retainNewest(100);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, retained)) {
            for (long k = 0; k < 150; k++) {
                store.put(k, "a" + k);            // writes 0..149
            }
            for (long k = 0; k < 50; k++) {
                store.put(k, "b" + k);            // re-write 0..49: they move to the newest end
            }
            for (long k = 150; k < 200; k++) {
                store.put(k, "c" + k);            // writes 150..199
            }
            assertEquals(100, store.size(), "window bound holds");
            // Newest 100 distinct writes: re-written 0..49 and fresh 150..199.
            assertEquals("b7", store.get(7L));
            assertEquals("c175", store.get(175L));
            assertNull(store.get(120L), "middle-aged keys evicted");
            assertNull(store.get(75L), "old keys evicted");
            assertTrue(store.garbageBytes() > 0, "evictions fund the garbage ledger");
        }
        // Recovery re-derives newest-N from log order (log order IS write order).
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, retained)) {
            assertEquals(100, reopened.size());
            assertEquals("b7", reopened.get(7L));
            assertEquals("c175", reopened.get(175L));
            assertNull(reopened.get(120L));
        }
    }

    @Test
    void intervalFsyncIsTheQuietDefaultAndSurvivesReopen(@TempDir Path dir) throws IOException {
        var interval = opts().fsync(Fsync.INTERVAL).fsyncIntervalMillis(10);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, interval)) {
            for (long k = 0; k < 300; k++) {
                store.put(k, "v" + k);
            }
            assertEquals("v42", store.get(42L));
        }
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, interval)) {
            assertEquals(300, reopened.size());
            assertEquals("v299", reopened.get(299L));
        }
    }
}
