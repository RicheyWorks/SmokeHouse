package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Engine 2, observed (2026-09-02). SuperBeefSort is the recovery engine, and until this the
 * profile it measured and the metrics of its sort were used to prime the control plane and then
 * dropped -- nothing a caller could read said whether recovery sorted anything, by what, or how
 * disordered the feed was. {@link SmokeHouse#recovery()} is that report, and {@link
 * SmokeHouse#abandon()} is the crash a drill can call: release the handles without the checkpoint,
 * so the next open must walk the road every real crash takes.
 */
class RecoveryReportTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(2048)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    private static TreeMap<Long, String> scan(SmokeHouse<Long, String> store) throws IOException {
        TreeMap<Long, String> out = new TreeMap<>();
        if (store.size() > 0) {
            store.range(store.firstKey(), store.lastKey(), out::put);
        }
        return out;
    }

    @Test
    void aColdOpenSortsAndSaysSoAWarmOpenDoesNotAndSaysThatToo(@TempDir Path dir) throws IOException {
        Random rnd = new Random(31);
        TreeMap<Long, String> oracle = new TreeMap<>();
        SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
        SmokeHouse.RecoveryReport born = store.recovery();
        assertEquals(0, born.entries(), "an empty dir recovers nothing");
        assertFalse(born.hintUsed());
        assertFalse(born.sorted(), "one or zero entries are not sorted");
        for (int i = 0; i < 400; i++) {
            long k = rnd.nextInt(1000);                    // arrives in random key order
            String v = "v" + i;
            store.put(k, v);
            oracle.put(k, v);
        }
        store.abandon();                                   // the process dies: no checkpoint

        try (SmokeHouse<Long, String> cold = SmokeHouse.open(dir, opts())) {
            SmokeHouse.RecoveryReport r = cold.recovery();
            assertEquals(oracle.size(), r.entries(), "every live key came back from the log alone");
            assertFalse(r.hintUsed(), "no hint was ever written");
            assertFalse(r.bounded());
            assertTrue(r.sorted(), "a cold scan is sorted by SuperBeefSort");
            assertFalse(r.sortStrategy().isEmpty(), "and the report names the strategy: " + r);
            assertTrue(r.comparisons() > r.entries(), "at a cost on the record, more than a pass: " + r);
            // The first run of this test read sortedness 1.0, zero inversions, an insertion sort of
            // n-1 comparisons: recovery was handing the sort a TreeMap's iteration. The profile
            // must describe the log's arrival order, which for random keys is disordered.
            assertTrue(r.sortednessRatio() >= 0.0 && r.sortednessRatio() < 0.9,
                    "the feed's disorder is the log's, not a map's: " + r);
            assertFalse(r.nearlySorted(), "random arrival is not nearly sorted: " + r);
            assertTrue(r.inversions() > 0, "and the inversions are counted: " + r);
            assertFalse(r.bornStrategy().isEmpty(), "and the tree the index was born as: " + r);
            assertEquals("STATIC", r.tier());
            assertEquals(oracle, scan(cold), "recovery is correct, not just reported");
        }                                                  // a clean close: the checkpoint is written

        try (SmokeHouse<Long, String> warm = SmokeHouse.open(dir, opts())) {
            SmokeHouse.RecoveryReport r = warm.recovery();
            assertEquals(oracle.size(), r.entries());
            assertTrue(r.hintUsed(), "the clean close's checkpoint was used");
            assertFalse(r.sorted(), "and nothing needed sorting: the hint covered the log");
            assertEquals("", r.sortStrategy());
            assertEquals(0, r.comparisons());
            assertFalse(r.bornStrategy().isEmpty(), "the index is still born as something");
            assertEquals(oracle, scan(warm));
            warm.put(5000L, "late");
            oracle.put(5000L, "late");
            warm.abandon();                                // dies again, one append past the checkpoint
        }

        try (SmokeHouse<Long, String> delta = SmokeHouse.open(dir, opts())) {
            SmokeHouse.RecoveryReport r = delta.recovery();
            assertTrue(r.hintUsed(), "the old checkpoint is still good for what it covered");
            assertTrue(r.sorted(), "and the delta past it made the sort run again");
            assertEquals(oracle.size(), r.entries());
            assertEquals(oracle, scan(delta));
        }
    }

    @Test
    void abandonIsIdempotentWithClose(@TempDir Path dir) throws IOException {
        SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
        store.put(1L, "a");
        store.abandon();
        store.abandon();
        store.close();                                     // a second burial is a no-op
        try (SmokeHouse<Long, String> again = SmokeHouse.open(dir, opts())) {
            assertEquals("a", again.get(1L));
            assertFalse(again.recovery().hintUsed());
        }
    }
}
