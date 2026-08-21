package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Eleventh-pass probes (2026-08-21) — the SmokeHouse-core hunt. Each shows a defect the hunt
 * confirmed, now fixed.
 */
class EleventhPassProbeTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(1024)                            // tiny: many segments, frequent compaction
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    @Test
    void rangeSurvivesAConcurrentCompactionCommit(@TempDir Path dir) throws Exception {
        // SH-1: get() retries when a compaction commit repoints/reclaims its entry mid-read, but
        // range() read its under-lock snapshot off-lock with NO such retry. A compaction that
        // committed between the snapshot and a read deletes the old segment the snapshot names, so
        // range() threw "index pointed at an unreadable record" on a perfectly healthy store. The
        // reader below must always see every live key and never throw, while a single writer
        // overwrites every key (making garbage) and compacts, over and over.
        final int n = 400;
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (long k = 0; k < n; k++) {
                store.put(k, "v0-" + k);
            }

            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean stop = new AtomicBoolean(false);

            // The single writer: overwrite the whole keyset (garbage piles up), then compact the
            // closed segments into one — repeatedly, so a commit is almost always in flight.
            Thread writer = new Thread(() -> {
                try {
                    for (int gen = 1; gen <= 60 && failure.get() == null; gen++) {
                        for (long k = 0; k < n; k++) {
                            store.put(k, "v" + gen + "-" + k);
                        }
                        store.compact();
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    stop.set(true);
                }
            }, "sh-writer");

            // The reader: range over the whole keyspace. The keyset never changes (overwrites only),
            // so every range must return exactly n keys — and must never throw.
            Thread reader = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        List<Long> seen = new ArrayList<>();
                        store.range(0L, (long) (n - 1), (k, v) -> seen.add(k));
                        if (seen.size() != n) {
                            throw new AssertionError("range saw " + seen.size()
                                    + " live keys, expected " + n + " (a compaction dropped one)");
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }, "sh-reader");

            reader.start();
            writer.start();
            writer.join(60_000);
            stop.set(true);
            reader.join(60_000);

            Throwable t = failure.get();
            if (t != null) {
                throw new AssertionError("a range() overlapping a compaction commit must not fail", t);
            }
        }
    }
}
