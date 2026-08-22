package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PRE-REGISTERED EXPERIMENT (2026-08-21) — range-scan read amplification vs. segment count
 * (ADR-022 methodology: seeded, warmup discarded, median of 3).
 *
 * <p><b>Question.</b> A full-keyspace {@code range} scan reads every live record positionally from
 * whichever segment holds it, over per-segment read channels the log caches. Compaction consolidates
 * many closed segments into one. Is that consolidation a <em>read-speed</em> win — does the same
 * data cost more per key to scan when it is spread across many segments — or only a <em>space</em>
 * win (garbage reclaim), with scan cost flat regardless of how the log is fragmented?</p>
 *
 * <p><b>Method.</b> The same {@code n} records, built into 1, ~10, and ~100 segments by varying
 * {@code segmentBytes} alone (STATIC tier, no pilot, no auto-compaction — the measurement is of the
 * read path only). Per-key {@code range} cost is timed over a full-keyspace scan, median of 3, one
 * warmup discarded.</p>
 *
 * <p><b>Pre-registered decision rule.</b> If the per-key scan cost at ~100 segments exceeds
 * <b>2×</b> the single-segment cost, compaction's consolidation speeds reads (fragmentation is a
 * real read tax, so compaction earns its keep on the read path as well as on space). If it stays
 * within 2×, scan cost is segment-count-insensitive and compaction's value for reads is negligible —
 * its payoff is space. The verdict from the canonical run lives in
 * {@code docs/EXPERIMENT-2026-08-21-range-scan-segments.md}; this test asserts only structural truths
 * (every configuration scanned all n keys, the segment counts landed in distinct bands), never
 * wall-clock, so it cannot flake.</p>
 */
class RangeScanSegmentCountExperimentTest {

    private static final int N = 100_000;
    private static final int ROUNDS = 4;                       // 1 warmup discarded + median of 3

    // Three segment sizes chosen to land the same data in roughly 1, ~10, ~100 segments.
    private static final long[] SEGMENT_BYTES = {64L << 20, 512L << 10, 48L << 10};

    private static SmokeHouseOptions<Long, String> opts(long segmentBytes) {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(segmentBytes)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);   // no pilot → no auto-compaction
    }

    private static String value(long k) {
        return "val-" + k + "-padding-padding";                // fixed shape, ~30 bytes
    }

    @Test
    void rangeScanCostVersusSegmentCount(@TempDir Path root) throws IOException {
        StringBuilder report = new StringBuilder(
                "\nRANGE-SCAN vs SEGMENT-COUNT EXPERIMENT (per-key ns, median of 3, 1 warmup discarded)\n"
                        + String.format(Locale.ROOT, "%-14s %-10s %-16s%n",
                        "segmentBytes", "segments", "per-key ns"));
        long single = -1;
        long many = -1;
        int manySegs = -1;
        for (long segBytes : SEGMENT_BYTES) {
            Path dir = root.resolve("store-" + segBytes);
            int segments;
            try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts(segBytes))) {
                for (long k = 0; k < N; k++) {
                    store.put(k, value(k));
                }
                segments = store.segmentStats().size();

                long[] samples = new long[ROUNDS - 1];
                long first = store.firstKey();
                long last = store.lastKey();
                for (int round = 0; round < ROUNDS; round++) {
                    AtomicLong seen = new AtomicLong();
                    long t0 = System.nanoTime();
                    store.range(first, last, (k, v) -> seen.incrementAndGet());
                    long elapsed = System.nanoTime() - t0;
                    assertEquals(N, seen.get(), "every key was scanned at " + segments + " segments");
                    if (round > 0) {
                        samples[round - 1] = elapsed / N;
                    }
                }
                java.util.Arrays.sort(samples);
                long perKey = samples[samples.length / 2];     // median of 3
                report.append(String.format(Locale.ROOT, "%-14d %-10d %-16d%n", segBytes, segments, perKey));
                if (segments == 1) {
                    single = perKey;
                }
                if (segByteIsSmallest(segBytes)) {
                    many = perKey;
                    manySegs = segments;
                }
            }
        }

        // Structural truths only — never assert wall-clock, so the experiment cannot flake.
        assertTrue(single > 0 && many > 0, "the single-segment and many-segment bands were measured");
        assertTrue(manySegs >= 20, "the smallest segments fragmented the log into many pieces (got "
                + manySegs + ")");
        report.append(String.format(Locale.ROOT,
                "many/single per-key ratio = %.2fx  (%d segments vs 1)%n",
                (double) many / single, manySegs));
        System.out.println(report);
    }

    private static boolean segByteIsSmallest(long segBytes) {
        long min = Long.MAX_VALUE;
        for (long s : SEGMENT_BYTES) {
            min = Math.min(min, s);
        }
        return segBytes == min;
    }
}
