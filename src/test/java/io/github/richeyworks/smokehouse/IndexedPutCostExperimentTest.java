package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PRE-REGISTERED EXPERIMENT (2026-08-21) — the indexed-put cost, measured (ADR-022 methodology:
 * seeded, warmup discarded, median of 3).
 *
 * <p><b>Question.</b> {@link IndexedStore#put} does one extra primary read to retract the previous
 * value's index entries — the class javadoc calls this "the documented indexed-put cost" — plus a
 * fan-out to every index. But {@link SmokeHouse#get} short-circuits (an index lookup, no log read)
 * when the key is absent, so the retract-read only touches the log for a key that already exists.
 * So: what does an indexed put cost over a plain one, and is the log read paid on <em>every</em>
 * put or only on an overwrite?</p>
 *
 * <p><b>Four configurations</b>, each fresh per round, put cost measured over {@code n} puts:
 * <ul>
 *   <li><b>A plain-insert</b> — a plain SmokeHouse, {@code n} puts of new keys;</li>
 *   <li><b>B plain-overwrite</b> — a plain SmokeHouse pre-loaded with {@code n} keys, then {@code n}
 *       overwrites (only the overwrites timed);</li>
 *   <li><b>C indexed-insert</b> — an IndexedStore (one secondary + one interval), {@code n} new
 *       keys;</li>
 *   <li><b>D indexed-overwrite</b> — that IndexedStore pre-loaded, then {@code n} overwrites.</li>
 * </ul>
 *
 * <p><b>Pre-registered decision rule.</b> The retract-read is a per-put cost paid <em>only on
 * overwrite</em> if the indexed overwrite overhead (D − B) is clearly larger than the indexed
 * insert overhead (C − A) — because the only extra work an overwrite does over an insert is the log
 * read of the old value. If instead (C − A) ≈ (D − B), the read is being paid on inserts too
 * ({@code get} not short-circuiting), and a "skip the retract-read when the key is absent"
 * optimization is worth building. The verdict from the canonical run lives in
 * {@code docs/EXPERIMENT-2026-08-21-indexed-put-cost.md}; this test asserts only structural truths
 * (fan-out correctness, every phase measured), never wall-clock, so it cannot flake.</p>
 */
class IndexedPutCostExperimentTest {

    private static final int[] SIZES = {50_000, 150_000};
    private static final int ROUNDS = 4;                       // 1 warmup discarded + median of 3

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(1 << 20)                         // large: keep compaction out of the measurement
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    // Values are "attr:start:end"; the indexed store buckets on attr and spans [start,end].
    private static String value(long k, int gen) {
        int attr = (int) (k % 64);
        int start = (int) (k % 1000);
        return attr + ":" + start + ":" + (start + 50) + ":" + gen;   // gen keeps overwrites distinct
    }

    private static int attrOf(String v) {
        return Integer.parseInt(v.substring(0, v.indexOf(':')));
    }

    private static int startOf(String v) {
        int a = v.indexOf(':');
        return Integer.parseInt(v.substring(a + 1, v.indexOf(':', a + 1)));
    }

    private static int endOf(String v) {
        int a = v.indexOf(':', v.indexOf(':') + 1);
        return Integer.parseInt(v.substring(a + 1, v.indexOf(':', a + 1)));
    }

    @FunctionalInterface
    private interface PutFn {
        void put(long key, String value) throws IOException;
    }

    /** Median per-put nanos over {@code n} puts of keys [0,n), one warmup round discarded. */
    private long medianPerPutNanos(Path root, String tag, int n, boolean indexed, boolean overwrite)
            throws IOException {
        long[] samples = new long[ROUNDS - 1];
        for (int round = 0; round < ROUNDS; round++) {
            Path dir = root.resolve(tag + "-" + n + "-r" + round);
            java.io.Closeable handle;
            PutFn put;
            SmokeHouse<Long, String> plain = null;
            IndexedStore<Long, String> idx = null;
            if (indexed) {
                idx = IndexedStore.open(dir, opts())
                        .secondary("attr", Comparator.<Integer>naturalOrder(),
                                IndexedPutCostExperimentTest::attrOf)
                        .interval("span", IndexedPutCostExperimentTest::startOf,
                                IndexedPutCostExperimentTest::endOf)
                        .build();
                handle = idx;
                final IndexedStore<Long, String> f = idx;
                put = f::put;
            } else {
                plain = SmokeHouse.open(dir, opts());
                handle = plain;
                final SmokeHouse<Long, String> f = plain;
                put = f::put;
            }
            try {
                if (overwrite) {
                    for (long k = 0; k < n; k++) {             // pre-load (not timed)
                        put.put(k, value(k, 0));
                    }
                }
                long t0 = System.nanoTime();
                for (long k = 0; k < n; k++) {                 // the timed run
                    put.put(k, value(k, 1));
                }
                long elapsed = System.nanoTime() - t0;
                if (round > 0) {                               // discard the warmup round
                    samples[round - 1] = elapsed / n;
                }
            } finally {
                handle.close();
            }
        }
        java.util.Arrays.sort(samples);
        return samples[samples.length / 2];                    // median of 3
    }

    @Test
    void indexedPutOverheadIsPaidOnlyOnOverwrite(@TempDir Path root) throws IOException {
        StringBuilder report = new StringBuilder(
                "\nINDEXED-PUT COST EXPERIMENT (per-put ns, median of 3, 1 warmup discarded)\n"
                        + String.format(Locale.ROOT, "%-10s %12s %12s %12s %12s %14s %14s%n",
                        "n", "A plainIns", "B plainOvr", "C idxIns", "D idxOvr", "insOvh(C-A)",
                        "ovrOvh(D-B)"));
        for (int n : SIZES) {
            long a = medianPerPutNanos(root, "A", n, false, false);
            long b = medianPerPutNanos(root, "B", n, false, true);
            long c = medianPerPutNanos(root, "C", n, true, false);
            long d = medianPerPutNanos(root, "D", n, true, true);
            report.append(String.format(Locale.ROOT,
                    "%-10d %12d %12d %12d %12d %14d %14d%n", n, a, b, c, d, c - a, d - b));

            // Structural truths only — never assert wall-clock, so the experiment cannot flake.
            assertTrue(a > 0 && b > 0 && c > 0 && d > 0, "every configuration was measured");

            // Fan-out fidelity: an indexed store's secondary/interval reads must agree with the data.
            Path check = root.resolve("check-" + n);
            try (IndexedStore<Long, String> store = IndexedStore.open(check, opts())
                    .secondary("attr", Comparator.<Integer>naturalOrder(),
                            IndexedPutCostExperimentTest::attrOf)
                    .interval("span", IndexedPutCostExperimentTest::startOf,
                            IndexedPutCostExperimentTest::endOf)
                    .build()) {
                for (long k = 0; k < 500; k++) {
                    store.put(k, value(k, 1));
                }
                int oldAttr = attrOf(value(7L, 1));
                store.put(7L, value(7L, 2));                   // same key, same derived attr, new gen
                int attr7 = attrOf(value(7L, 2));
                assertEquals(oldAttr, attr7, "the value shape keeps the attr stable across gens");
                long timesIndexed = store.byAttribute("attr", attr7, attr7).stream()
                        .filter(k -> k == 7L).count();
                assertEquals(1L, timesIndexed,
                        "the overwritten key is indexed under its attribute exactly once (retract worked)");
            }
        }
        System.out.println(report);
    }
}
