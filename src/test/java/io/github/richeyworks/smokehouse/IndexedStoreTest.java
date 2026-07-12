package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link IndexedStore} (Phase 4.1) against the required double oracle: a {@code TreeMap} for the
 * primary and a manually maintained {@code TreeMap<attr, TreeSet<K>>} for the secondary. Every
 * byAttribute range must agree, deletes must retract, updates must move entries, and a reopen
 * must rebuild the secondaries identically — because secondaries are caches of the log too.
 */
class IndexedStoreTest {

    private static final String ATTR = "grade";

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);       // deterministic for oracle runs
    }

    /** Values look like {@code "017:v123"}; the attribute is the leading int. */
    private static String value(int attr, int stamp) {
        return String.format("%03d:v%d", attr, stamp);
    }

    private static int attrOf(String value) {
        return Integer.parseInt(value.substring(0, 3));
    }

    private static IndexedStore<Long, String> openIndexed(Path dir) throws IOException {
        return IndexedStore.open(dir, opts())
                .secondary(ATTR, Comparator.<Integer>naturalOrder(), IndexedStoreTest::attrOf)
                .build();
    }

    private static List<Long> expected(TreeMap<Integer, TreeSet<Long>> attrOracle, int lo, int hi) {
        List<Long> keys = new ArrayList<>();
        for (TreeSet<Long> set : attrOracle.subMap(lo, true, hi, true).values()) {
            keys.addAll(set);                                          // attr order, then key order
        }
        return keys;
    }

    private static void putOracle(TreeMap<Long, String> primary,
                                  TreeMap<Integer, TreeSet<Long>> attrOracle, long key, String v) {
        String old = primary.put(key, v);
        if (old != null) {
            retract(attrOracle, attrOf(old), key);
        }
        attrOracle.computeIfAbsent(attrOf(v), a -> new TreeSet<>()).add(key);
    }

    private static void deleteOracle(TreeMap<Long, String> primary,
                                     TreeMap<Integer, TreeSet<Long>> attrOracle, long key) {
        String old = primary.remove(key);
        if (old != null) {
            retract(attrOracle, attrOf(old), key);
        }
    }

    private static void retract(TreeMap<Integer, TreeSet<Long>> attrOracle, int attr, long key) {
        TreeSet<Long> set = attrOracle.get(attr);
        set.remove(key);
        if (set.isEmpty()) {
            attrOracle.remove(attr);
        }
    }

    @Test
    void randomOpsAgreeWithTheDoubleOracle(@TempDir Path dir) throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> primary = new TreeMap<>();
        TreeMap<Integer, TreeSet<Long>> attrOracle = new TreeMap<>();
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            for (int i = 0; i < 2_000; i++) {
                long key = rnd.nextInt(300);
                if (rnd.nextInt(4) == 0) {
                    assertEquals(primary.containsKey(key), store.delete(key), "delete at op " + i);
                    deleteOracle(primary, attrOracle, key);
                } else {
                    String v = value(rnd.nextInt(40), i);
                    store.put(key, v);
                    putOracle(primary, attrOracle, key, v);
                }
                if (i % 200 == 0) {
                    int a = rnd.nextInt(45);
                    int b = rnd.nextInt(45);
                    assertEquals(expected(attrOracle, Math.min(a, b), Math.max(a, b)),
                            store.byAttribute(ATTR, Math.min(a, b), Math.max(a, b)),
                            "mid-run byAttribute at op " + i);
                }
            }
            assertEquals(primary.size(), store.primary().size());
            for (var e : primary.entrySet()) {
                assertEquals(e.getValue(), store.get(e.getKey()));
            }
            for (int i = 0; i < 60; i++) {
                int a = rnd.nextInt(50) - 5;
                int b = rnd.nextInt(50) - 5;
                int lo = Math.min(a, b);
                int hi = Math.max(a, b);
                assertEquals(expected(attrOracle, lo, hi), store.byAttribute(ATTR, lo, hi),
                        "byAttribute[" + lo + ", " + hi + "]");
            }
        }
    }

    @Test
    void updateMovesTheSecondaryEntry(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            store.put(7L, value(5, 1));
            assertEquals(List.of(7L), store.byAttribute(ATTR, 5, 5));
            store.put(7L, value(9, 2));                                // same key, new attribute
            assertTrue(store.byAttribute(ATTR, 5, 5).isEmpty(), "stale entry must be retracted");
            assertEquals(List.of(7L), store.byAttribute(ATTR, 9, 9));
        }
    }

    @Test
    void deleteRetractsFromTheSecondary(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            store.put(1L, value(3, 1));
            store.put(2L, value(3, 2));
            assertEquals(List.of(1L, 2L), store.byAttribute(ATTR, 3, 3));
            assertTrue(store.delete(1L));
            assertEquals(List.of(2L), store.byAttribute(ATTR, 3, 3));
            assertFalse(store.delete(1L), "second delete is a no-op");
        }
    }

    @Test
    void reopenRebuildsSecondariesIdentically(@TempDir Path dir) throws IOException {
        Random rnd = new Random(7);
        TreeMap<Long, String> primary = new TreeMap<>();
        TreeMap<Integer, TreeSet<Long>> attrOracle = new TreeMap<>();
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            for (int i = 0; i < 800; i++) {
                long key = rnd.nextInt(200);
                if (rnd.nextInt(5) == 0) {
                    store.delete(key);
                    deleteOracle(primary, attrOracle, key);
                } else {
                    String v = value(rnd.nextInt(25), i);
                    store.put(key, v);
                    putOracle(primary, attrOracle, key, v);
                }
            }
        }
        // Rebuild from the log alone: the secondaries must come back exactly.
        try (IndexedStore<Long, String> reopened = openIndexed(dir)) {
            assertEquals(primary.size(), reopened.primary().size());
            for (int i = 0; i < 40; i++) {
                int a = rnd.nextInt(30);
                int b = rnd.nextInt(30);
                int lo = Math.min(a, b);
                int hi = Math.max(a, b);
                assertEquals(expected(attrOracle, lo, hi), reopened.byAttribute(ATTR, lo, hi),
                        "rebuilt byAttribute[" + lo + ", " + hi + "]");
            }
        }
    }

    @Test
    void retentionRefusesSecondaries(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class, () -> IndexedStore
                        .open(dir, opts().retainNewest(10))
                        .secondary(ATTR, Comparator.<Integer>naturalOrder(), IndexedStoreTest::attrOf)
                        .build(),
                "retention evictions bypass IndexedStore, so v1 must refuse the combination");
    }

    @Test
    void unknownSecondaryNameFailsLoudly(@TempDir Path dir) throws IOException {
        try (IndexedStore<Long, String> store = openIndexed(dir)) {
            assertThrows(IllegalArgumentException.class, () -> store.byAttribute("nope", 1, 2));
        }
    }
}
