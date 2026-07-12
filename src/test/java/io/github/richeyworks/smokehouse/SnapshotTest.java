package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snapshot reads: a {@code snapshot()} is a frozen, isolated view — later overwrites and deletes
 * don't reach it (they append new records; the snapshot keeps reading the old ones), and while it is
 * open compaction is deferred so its segments are never reclaimed out from under it. Closing it lets
 * compaction reclaim the deferred garbage.
 */
class SnapshotTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)                        // small: real closed segments with garbage
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    @Test
    void snapshotIsolatesFromLaterWritesAndDefersCompaction(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (long k = 0; k < 500; k++) {
                store.put(k, "v" + k);
            }

            try (var snap = store.snapshot()) {
                // Mutate heavily AFTER the snapshot: overwrite everything, delete one, add one.
                for (long k = 0; k < 500; k++) {
                    store.put(k, "changed" + k);
                }
                store.delete(0L);
                store.put(1000L, "new");

                // The snapshot still sees the state as of when it was taken.
                assertEquals(500, snap.size());
                assertEquals("v5", snap.get(5L));
                assertEquals("v499", snap.get(499L));
                assertEquals("v0", snap.get(0L));           // the later delete didn't reach it
                assertNull(snap.get(1000L));                // and the later insert didn't either
                assertEquals(0L, (long) snap.firstKey());
                assertEquals(499L, (long) snap.lastKey());

                // The live store sees the new state.
                assertEquals("changed5", store.get(5L));
                assertNull(store.get(0L));
                assertEquals("new", store.get(1000L));

                // Compaction is deferred while the snapshot pins the segments.
                assertEquals(0L, store.compact());
                assertEquals("v5", snap.get(5L), "snapshot still valid — its segments weren't reclaimed");

                // Range over the snapshot.
                List<Long> keys = new ArrayList<>();
                snap.range(10L, 20L, (k, v) -> keys.add(k));
                assertEquals(11, keys.size());
                assertEquals(10L, (long) keys.get(0));
                assertEquals(20L, (long) keys.get(10));
            }

            // Once the snapshot closes, compaction runs again and reclaims the deferred garbage.
            assertTrue(store.compact() > 0, "garbage becomes reclaimable after the snapshot closes");
            assertEquals("changed5", store.get(5L));        // store truth intact across compaction
            assertNull(store.get(0L));
            assertEquals("new", store.get(1000L));
        }
    }
}
