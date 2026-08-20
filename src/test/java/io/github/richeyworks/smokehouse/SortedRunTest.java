package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The sorted run against the oracle (ADR scan-sidecar, 2026-08-20): what {@code exportSorted}
 * writes and {@code scanSorted} reads back must equal the store's live records exactly, in key
 * order, and a corrupted run must be refused whole rather than delivered wrong.
 */
class SortedRunTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(2048)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    @Test
    void theRunRoundTripsInKeyOrder(@TempDir Path dir) throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir.resolve("s"), opts())) {
            for (int i = 0; i < 700; i++) {                    // churn: overwrites + deletes
                long key = rnd.nextInt(150);
                if (rnd.nextInt(6) == 0) {
                    store.delete(key);
                    oracle.remove(key);
                } else {
                    String v = "v" + key + ":" + i;
                    store.put(key, v);
                    oracle.put(key, v);
                }
            }
            Path run = dir.resolve("scan.run");
            int exported = store.exportSorted(run);
            assertEquals(oracle.size(), exported, "every live record exported, nothing else");

            List<Long> keys = new ArrayList<>();
            TreeMap<Long, String> read = new TreeMap<>();
            int delivered = SmokeHouse.scanSorted(run, opts(), (k, v) -> {
                keys.add(k);
                read.put(k, v);
            });
            assertEquals(exported, delivered);
            assertEquals(oracle, read, "the run IS the live set");
            assertEquals(new ArrayList<>(oracle.keySet()), keys, "and arrives in key order");
        }
    }

    @Test
    void theRunSeedsAFreshFullyQueryableStore(@TempDir Path dir) throws IOException {
        Random rnd = new Random(11);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir.resolve("s"), opts())) {
            for (int i = 0; i < 400; i++) {
                long key = rnd.nextInt(120);
                if (rnd.nextInt(6) == 0) {
                    store.delete(key);
                    oracle.remove(key);
                } else {
                    String v = "v" + key + ":" + i;
                    store.put(key, v);
                    oracle.put(key, v);
                }
            }
            Path run = dir.resolve("scan.run");
            store.exportSorted(run);
        }

        // Seed a FRESH store from the run alone — no segments, no history, pure state.
        byte[] runBytes = Files.readAllBytes(dir.resolve("scan.run"));
        try (SmokeHouse<Long, String> seeded =
                     SmokeHouse.importSorted(dir.resolve("seeded"), opts(), runBytes)) {
            assertEquals(oracle.size(), seeded.size(), "the seed is the run's state");
            TreeMap<Long, String> scanned = new TreeMap<>();
            seeded.range(seeded.firstKey(), seeded.lastKey(), scanned::put);
            assertEquals(oracle, scanned, "every record, exactly");
            assertEquals(oracle.firstKey(), seeded.firstKey());
            assertEquals(oracle.lastKey(), seeded.lastKey());
            assertEquals(oracle.size(), seeded.countRange(seeded.firstKey(), seeded.lastKey()),
                    "order statistics work on the seed");
        }

        // Seeding into a populated directory is refused — the importInto guard holds.
        assertThrows(IllegalStateException.class,
                () -> SmokeHouse.importSorted(dir.resolve("s"), opts(), runBytes));
    }

    @Test
    void emptyStoresExportEmptyRuns(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir.resolve("s"), opts())) {
            Path run = dir.resolve("empty.run");
            assertEquals(0, store.exportSorted(run));
            assertEquals(0, SmokeHouse.scanSorted(run, opts(), (k, v) -> {
                throw new AssertionError("an empty run must deliver nothing");
            }));
        }
    }

    @Test
    void aCorruptRunIsRefusedWholeNeverDeliveredWrong(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir.resolve("s"), opts())) {
            store.put(1L, "one");
            store.put(2L, "two");
            Path run = dir.resolve("scan.run");
            store.exportSorted(run);

            byte[] bytes = Files.readAllBytes(run);
            bytes[bytes.length / 2] ^= 0x5A;                   // flip a mid-body bit
            Path bad = dir.resolve("bad.run");
            Files.write(bad, bytes);

            List<Long> delivered = new ArrayList<>();
            assertThrows(IOException.class,
                    () -> SmokeHouse.scanSorted(bad, opts(), (k, v) -> delivered.add(k)));
            assertTrue(delivered.isEmpty(), "CRC is checked BEFORE the first record is delivered");

            // Truncation and garbage are refused the same way.
            Files.write(dir.resolve("tiny.run"), new byte[]{1, 2, 3});
            assertThrows(IOException.class,
                    () -> SmokeHouse.scanSorted(dir.resolve("tiny.run"), opts(), (k, v) -> { }));
        }
    }
}
