package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.csrbt.AccessPolicy;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import io.github.richeyworks.superbeefsort.source.CsvSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3 "CSRBT at full extent" surfaces, against the {@link TreeMap} oracle (the required
 * pattern for new store behavior): born-optimal strategy selection, the morph-family clamp,
 * order statistics, and the ENSEMBLE tier — every one must agree with the reference and
 * survive a reopen, because the log stays the only truth no matter how clever the index gets.
 */
class CsrbtUnlockTest {

    private static SmokeHouseOptions<Long, String> base(SmokeHouseOptions.IndexTier tier) {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)                                   // tiny: exercise rolling
                .indexTier(tier)
                .pilotCadence(Duration.ofHours(1));                   // scheduler never fires in-test
    }

    // ── Born optimal: the access policy shapes the index at construction ───────────────────────

    @Test
    void staticTierIsBornWithTheAdvisedStrategy(@TempDir Path dir) throws IOException {
        record Case(AccessPolicy policy, String expected) { }
        List<Case> cases = List.of(
                new Case(AccessPolicy.BALANCED, "RedBlackStrategy"),
                new Case(AccessPolicy.READ_HEAVY, "AVLStrategy"),
                new Case(AccessPolicy.SKEWED, "SplayStrategy"),
                new Case(AccessPolicy.WRITE_HEAVY, "WeightBalancedStrategy"));
        for (Case c : cases) {
            Path sub = dir.resolve(c.policy().name());
            var opts = base(SmokeHouseOptions.IndexTier.STATIC).accessPolicy(c.policy());
            try (SmokeHouse<Long, String> store = SmokeHouse.open(sub, opts)) {
                for (long k = 0; k < 50; k++) {
                    store.put(k, "v" + k);
                }
                assertTrue(store.stats().contains("strategy=" + c.expected()),
                        c.policy() + " should be born " + c.expected() + ": " + store.stats());
            }
            // Reopen (warm start): the advised shape is a property of the options, not of luck.
            try (SmokeHouse<Long, String> reopened = SmokeHouse.open(sub, opts)) {
                assertTrue(reopened.stats().contains("strategy=" + c.expected()),
                        "reopen must keep the advised shape: " + reopened.stats());
                assertEquals(50, reopened.size());
            }
        }
    }

    @Test
    void adaptiveTierClampsWriteHeavyIntoTheMorphFamily(@TempDir Path dir) throws IOException {
        // WRITE_HEAVY advises WeightBalanced — a static target outside CSRBT's morph family, which
        // WorkloadAdaptation.attach rejects loudly. The ADAPTIVE tier must clamp to Red-Black and
        // open cleanly rather than explode (or silently drop adaptation).
        var opts = base(SmokeHouseOptions.IndexTier.ADAPTIVE).accessPolicy(AccessPolicy.WRITE_HEAVY);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts)) {
            for (long k = 0; k < 50; k++) {
                store.put(k, "v" + k);
            }
            String stats = store.stats();
            assertTrue(stats.contains("strategy=RedBlackStrategy"), stats);
            assertTrue(stats.contains("tier=ADAPTIVE"), stats);
            assertTrue(stats.contains("adaptation="), "the control plane must be attached: " + stats);
        }
    }

    @Test
    void adaptiveBornAvlServesAndRecovers(@TempDir Path dir) throws IOException {
        // READ_HEAVY + ADAPTIVE: born AVL (a health-gated fromSorted build on reopen) with the
        // control plane attached — the full born-optimal + wired-to-adapt path, oracle-checked.
        var opts = base(SmokeHouseOptions.IndexTier.ADAPTIVE).accessPolicy(AccessPolicy.READ_HEAVY);
        TreeMap<Long, String> oracle = new TreeMap<>();
        Random rnd = new Random(11);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts)) {
            for (int i = 0; i < 1_000; i++) {
                long key = rnd.nextInt(200);
                String v = "v" + i;
                store.put(key, v);
                oracle.put(key, v);
            }
            assertTrue(store.stats().contains("strategy=AVLStrategy"), store.stats());
        }
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts)) {
            assertTrue(reopened.stats().contains("strategy=AVLStrategy"), reopened.stats());
            assertEquals(oracle.size(), reopened.size());
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), reopened.get(e.getKey()));
            }
        }
    }

    @Test
    void coldRecoveryPrimesAdaptationFromTheSortProfile(@TempDir Path dir) throws IOException {
        // importInto is the guaranteed-cold path: no hint, every entry scanned, so the recovery
        // sort RUNS and its DataProfile + metrics must flow into attachProfileGuided without
        // upsetting recovery itself. The observable contract: the store opens adaptive, born in
        // the advised shape, agreeing with last-writer-wins.
        Path csv = dir.resolve("in.csv");
        List<String> lines = new ArrayList<>();
        TreeMap<Long, String> oracle = new TreeMap<>();
        Random rnd = new Random(23);
        for (int i = 0; i < 800; i++) {
            long key = rnd.nextInt(300);
            String v = "v" + i;
            lines.add(key + "," + v);
            oracle.put(key, v);
        }
        Files.write(csv, lines, StandardCharsets.UTF_8);

        var opts = base(SmokeHouseOptions.IndexTier.ADAPTIVE).accessPolicy(AccessPolicy.READ_HEAVY);
        Path store = dir.resolve("store");
        try (SmokeHouse<Long, String> s = SmokeHouse.importInto(store, opts,
                CsvSource.of(csv, 0, 1, false, Long::parseLong, v -> v))) {
            String stats = s.stats();
            assertTrue(stats.contains("strategy=AVLStrategy"), stats);
            assertTrue(stats.contains("adaptation="), stats);
            assertEquals(oracle.size(), s.size());
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), s.get(e.getKey()));
            }
        }
    }

    // ── Order statistics: CSRBT's RankedSet face, against the oracle ───────────────────────────

    @Test
    void orderStatisticsAgreeWithTheOracle(@TempDir Path dir) throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> oracle = new TreeMap<>();
        var opts = base(SmokeHouseOptions.IndexTier.STATIC);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts)) {
            for (int i = 0; i < 2_000; i++) {
                long key = rnd.nextInt(500);
                if (rnd.nextInt(4) == 0) {
                    oracle.remove(key);
                    store.delete(key);
                } else {
                    oracle.put(key, "v" + i);
                    store.put(key, "v" + i);
                }
            }
            assertOrderStatistics(store, oracle, rnd);
        }
        // And again after recovery: fromSorted's bulk build maintains subtree sizes intrinsically,
        // so order statistics must be correct immediately on a reopened store.
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts)) {
            assertOrderStatistics(reopened, oracle, new Random(43));
        }
    }

    private static void assertOrderStatistics(SmokeHouse<Long, String> store,
                                              TreeMap<Long, String> oracle, Random rnd) {
        List<Long> keys = new ArrayList<>(oracle.keySet());           // ascending
        int n = keys.size();
        assertEquals(n, store.size());
        assertEquals(oracle.firstKey(), store.firstKey());
        assertEquals(oracle.lastKey(), store.lastKey());
        assertEquals(keys.get((n + 1) / 2 - 1), store.medianKey(), "lower median");
        assertEquals(store.medianKey(), store.percentileKey(50), "percentile(50) == median");
        assertEquals(oracle.lastKey(), store.percentileKey(100));
        assertEquals(keys.get(0), store.nthKey(1), "nthKey is 1-indexed");
        assertEquals(keys.get(n - 1), store.nthKey(n));
        assertThrows(IndexOutOfBoundsException.class, () -> store.nthKey(0));
        assertThrows(IndexOutOfBoundsException.class, () -> store.nthKey(n + 1));
        for (int i = 0; i < 50; i++) {
            int r = 1 + rnd.nextInt(n);
            assertEquals(keys.get(r - 1), store.nthKey(r), "nthKey(" + r + ")");
            assertEquals(r, store.rankOf(keys.get(r - 1)), "rankOf(nthKey(r)) == r");
        }
        assertEquals(0, store.rankOf(-1L), "absent key ranks 0");
        for (int i = 0; i < 50; i++) {
            long a = rnd.nextInt(600) - 50;
            long b = rnd.nextInt(600) - 50;
            long lo = Math.min(a, b);
            long hi = Math.max(a, b);
            assertEquals(oracle.subMap(lo, true, hi, true).size(), store.countRange(lo, hi),
                    "countRange[" + lo + ", " + hi + "]");
        }
    }

    @Test
    void orderStatisticsOnAnEmptyStore(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, base(SmokeHouseOptions.IndexTier.STATIC))) {
            assertNull(store.firstKey());
            assertNull(store.lastKey());
            assertNull(store.medianKey());
            assertEquals(0, store.countRange(0L, 100L));
            assertEquals(0, store.rankOf(1L));
        }
    }

    // ── ENSEMBLE tier ───────────────────────────────────────────────────────────────────────────

    @Test
    void ensembleTierAgreesWithTheOracleAndRecovers(@TempDir Path dir) throws IOException {
        Random rnd = new Random(77);
        TreeMap<Long, String> oracle = new TreeMap<>();
        var opts = base(SmokeHouseOptions.IndexTier.ENSEMBLE);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts)) {
            for (int i = 0; i < 1_500; i++) {
                long key = rnd.nextInt(300);
                switch (rnd.nextInt(4)) {
                    case 0, 1 -> {
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
            String stats = store.stats();
            assertTrue(stats.contains("tier=ENSEMBLE"), stats);
            assertTrue(stats.contains("promotion="), "the promotion control plane must be attached: " + stats);

            // Range walk + order statistics served by the ensemble's primary.
            List<Long> seen = new ArrayList<>();
            store.range(50L, 150L, (k, v) -> seen.add(k));
            assertEquals(new ArrayList<>(oracle.subMap(50L, true, 150L, true).keySet()), seen);
            assertEquals(oracle.firstKey(), store.firstKey());
            assertEquals(oracle.subMap(50L, true, 150L, true).size(), store.countRange(50L, 150L));
            assertThrows(IllegalStateException.class, store::index,
                    "the single-tree accessor must refuse in the ENSEMBLE tier");
        }
        // Recovery: the log is still the only truth; the mirrored trio rebuilds from it.
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts)) {
            assertEquals(oracle.size(), reopened.size());
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), reopened.get(e.getKey()));
            }
        }
    }

    @Test
    void evolutionTierAgreesWithTheOracleAndRecovers(@TempDir Path dir) throws IOException {
        // The observability tier: a laboratory member trials policies while the advised primary
        // serves. Whatever the bandit does, the STORE must stay boring — oracle agreement,
        // range/order-stats parity, clean recovery.
        Random rnd = new Random(99);
        TreeMap<Long, String> oracle = new TreeMap<>();
        var opts = base(SmokeHouseOptions.IndexTier.EVOLUTION);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts)) {
            for (int i = 0; i < 1_200; i++) {
                long key = rnd.nextInt(250);
                switch (rnd.nextInt(4)) {
                    case 0, 1 -> {
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
            String stats = store.stats();
            assertTrue(stats.contains("tier=EVOLUTION"), stats);
            assertTrue(stats.contains("evolution=BANDIT_SEARCH"), stats);
            assertEquals(oracle.firstKey(), store.firstKey());
            assertEquals(oracle.subMap(20L, true, 120L, true).size(), store.countRange(20L, 120L));
            List<Long> seen = new ArrayList<>();
            store.range(20L, 120L, (k, v) -> seen.add(k));
            assertEquals(new ArrayList<>(oracle.subMap(20L, true, 120L, true).keySet()), seen);
            assertThrows(IllegalStateException.class, store::index);
        }
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts)) {
            assertEquals(oracle.size(), reopened.size());
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), reopened.get(e.getKey()));
            }
        }
    }

    @Test
    void evolutionPilotRunsRealCyclesWithoutKillingTheStore(@TempDir Path dir) throws Exception {
        // Fire the pilot for real (fast cadence): trials open and close against live traffic,
        // the verdict lands in stats(), and the data plane never wavers. Any uncaught throw in
        // the pilot would surface here as a stuck "not yet evaluated" verdict.
        var opts = base(SmokeHouseOptions.IndexTier.EVOLUTION)
                .pilotCadence(Duration.ofMillis(25));
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts)) {
            Random rnd = new Random(5);
            for (int i = 0; i < 3_000; i++) {
                long key = rnd.nextInt(400);
                store.put(key, "v" + i);
                oracle.put(key, "v" + i);
                if (i % 500 == 0) {
                    Thread.sleep(30);                              // let a few cycles land mid-traffic
                }
            }
            Thread.sleep(150);
            String stats = store.stats();
            assertFalse(stats.contains("not yet evaluated"), "the pilot must have cycled: " + stats);
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), store.get(e.getKey()));
            }
        }
    }

    @Test
    void autoCompactionReclaimsGarbageFromThePilot(@TempDir Path dir) throws Exception {
        // Phase 4.3: churn the same keys past the garbage threshold and let the PILOT call
        // compact() — copy off the lock, re-entry guarded. The observable outcomes: the garbage
        // ledger visibly deflates, and the store still agrees with the oracle afterwards.
        var opts = base(SmokeHouseOptions.IndexTier.ADAPTIVE)
                .pilotCadence(Duration.ofMillis(25))
                .compactWhenGarbageAbove(0.3);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts)) {
            for (int i = 0; i < 3_000; i++) {                      // 100 keys x 30 overwrites: mostly garbage
                long key = i % 100;
                store.put(key, "v" + i);
                oracle.put(key, "v" + i);
            }
            long before = store.garbageBytes();
            long deadline = System.currentTimeMillis() + 8_000;
            while (store.garbageBytes() > before / 4 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(store.garbageBytes() <= before / 4,
                    "the pilot should have auto-compacted: garbage " + before
                            + " -> " + store.garbageBytes() + "; " + store.stats());
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), store.get(e.getKey()), "post-compaction value");
            }
        }
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(dir, opts)) {
            assertEquals(oracle.size(), reopened.size());
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), reopened.get(e.getKey()));
            }
        }
    }

    @Test
    void evolutionTierRejectsRetention(@TempDir Path dir) {
        var opts = base(SmokeHouseOptions.IndexTier.EVOLUTION).retainNewest(10);
        assertThrows(IllegalArgumentException.class, () -> SmokeHouse.open(dir, opts));
    }

    @Test
    void ensembleTierRejectsRetention(@TempDir Path dir) {
        var opts = base(SmokeHouseOptions.IndexTier.ENSEMBLE).retainNewest(10);
        assertThrows(IllegalArgumentException.class, () -> SmokeHouse.open(dir, opts),
                "no Evict events from the ensemble => the retention garbage ledger cannot be funded");
    }
}
