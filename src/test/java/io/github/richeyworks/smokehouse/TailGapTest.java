package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The gap is real (2026-08-20): with the tail ring configurable, the drop-oldest contract is
 * finally testable instead of unreachable behind a constant. A deliberately slow subscriber on
 * a tiny ring MUST be told via {@code onGap()} that events were dropped — and a fast subscriber
 * on the same churn must see every event, gap-free. Bounded awaits; no wall-clock assertions.
 */
class TailGapTest {

    private static SmokeHouseOptions<Long, String> tinyRing() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(1 << 16)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC)
                .tailRing(8);                                  // the smallest honest ring
    }

    @Test
    void aSlowConsumerOnATinyRingIsToldTheTruth(@TempDir Path dir) throws IOException {
        // The tiny ring bounds EVERY subscriber — that is its semantics, so the slow consumer
        // rides a ring-8 store and the control (a fast consumer, gap-free) rides a default
        // ring on the same churn. One knob, both sides of the contract.
        AtomicLong slowSeen = new AtomicLong();
        AtomicLong slowGaps = new AtomicLong();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir.resolve("tiny"), tinyRing())) {
            AutoCloseable slow = store.tail(0, new TailListener<>() {
                @Override public void onEvent(TailEvent<Long, String> event) {
                    sleep(3);                                  // the lag that earns the gap
                    slowSeen.incrementAndGet();
                }
                @Override public void onGap() {
                    slowGaps.incrementAndGet();
                }
            });
            for (long k = 0; k < 400; k++) {                   // far past an 8-slot bound
                store.put(k, "v" + k);
            }
            long deadline = System.currentTimeMillis() + 15_000;
            while (slowGaps.get() == 0 && System.currentTimeMillis() < deadline) {
                sleep(2);
            }
            assertTrue(slowGaps.get() > 0, "a slow consumer on a tiny ring must be told via onGap");
            assertTrue(slowSeen.get() < 400, "and it genuinely missed events — the gap is real");
            closeQuietly(slow);
        }

        // The control: same churn, default ring, fast consumer — every event, no gap.
        AtomicLong fastSeen = new AtomicLong();
        AtomicLong fastGaps = new AtomicLong();
        SmokeHouseOptions<Long, String> defaultRing =
                SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                        .segmentBytes(1 << 16)
                        .indexTier(SmokeHouseOptions.IndexTier.STATIC);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir.resolve("roomy"), defaultRing)) {
            AutoCloseable fast = store.tail(0, new TailListener<>() {
                @Override public void onEvent(TailEvent<Long, String> event) {
                    fastSeen.incrementAndGet();
                }
                @Override public void onGap() {
                    fastGaps.incrementAndGet();
                }
            });
            for (long k = 0; k < 400; k++) {
                store.put(k, "v" + k);
            }
            long deadline = System.currentTimeMillis() + 15_000;
            while (fastSeen.get() < 400 && System.currentTimeMillis() < deadline) {
                sleep(2);
            }
            assertEquals(400, fastSeen.get(), "on a roomy ring the fast subscriber sees everything");
            assertEquals(0, fastGaps.get(), "and is never lied to about a gap");
            closeQuietly(fast);
        }
    }

    private static void closeQuietly(AutoCloseable c) throws IOException {
        try {
            c.close();
        } catch (Exception e) {
            throw new IOException("closing subscriber", e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
