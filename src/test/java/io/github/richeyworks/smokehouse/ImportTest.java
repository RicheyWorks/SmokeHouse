package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import io.github.richeyworks.superbeefsort.source.CsvSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
 * {@code importInto} — ingestion as recovery (Phase 3.4). An import appends every source record to
 * a bare log and lets {@code open()}'s recovery build the index; the result must match a
 * last-writer-wins {@link TreeMap} oracle, duplicate keys must resolve to the last source record,
 * and the store that comes back must reopen (warm start) to the exact same contents.
 */
class ImportTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)                                    // tiny: import spans many segments
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);        // deterministic for the oracle
    }

    private static CsvSource<Long, String> csv(Path file) throws IOException {
        return CsvSource.of(file, 0, 1, false, Long::parseLong, s -> s);
    }

    @Test
    void importFromCsvAgreesWithLastWriterWinsOracle(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("in.csv");
        Random rnd = new Random(9);
        List<String> lines = new ArrayList<>();
        TreeMap<Long, String> oracle = new TreeMap<>();
        for (int i = 0; i < 1_000; i++) {
            long key = rnd.nextInt(300);
            String value = "v" + i;                                    // no commas → CSV-safe
            lines.add(key + "," + value);
            oracle.put(key, value);                                    // line order = LWW, mirrors recovery
        }
        Files.write(csv, lines, StandardCharsets.UTF_8);

        Path store = dir.resolve("store");
        try (SmokeHouse<Long, String> s = SmokeHouse.importInto(store, opts(), csv(csv))) {
            assertEquals(oracle.size(), s.size(), "live key count");
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), s.get(e.getKey()), "value for key " + e.getKey());
            }
        }
    }

    @Test
    void duplicateKeysResolveToTheLastSourceRecord(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("dup.csv");
        Files.write(csv, List.of("5,first", "5,second", "5,third", "6,only"), StandardCharsets.UTF_8);
        Path store = dir.resolve("store");
        try (SmokeHouse<Long, String> s = SmokeHouse.importInto(store, opts(), csv(csv))) {
            assertEquals(2, s.size());
            assertEquals("third", s.get(5L), "last source record for a key wins");
            assertEquals("only", s.get(6L));
        }
    }

    @Test
    void reopenAfterImportIsAWarmStartWithIdenticalContents(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("in.csv");
        TreeMap<Long, String> oracle = new TreeMap<>();
        List<String> lines = new ArrayList<>();
        for (long k = 0; k < 200; k++) {
            String v = "row" + k;
            lines.add(k + "," + v);
            oracle.put(k, v);
        }
        Files.write(csv, lines, StandardCharsets.UTF_8);

        Path store = dir.resolve("store");
        try (SmokeHouse<Long, String> s = SmokeHouse.importInto(store, opts(), csv(csv))) {
            assertEquals(oracle.size(), s.size());
        }   // clean close writes the hint checkpoint → next open is a warm start
        assertTrue(Files.exists(store.resolve(SegmentLog.HINT_FILE)), "clean shutdown wrote a hint");

        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(store, opts())) {
            assertEquals(oracle.size(), reopened.size(), "warm start recovers the exact store");
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), reopened.get(e.getKey()));
            }
        }
    }

    @Test
    void importIntoAPopulatedDirectoryFailsLoudly(@TempDir Path dir) throws IOException {
        Path store = dir.resolve("store");
        try (SmokeHouse<Long, String> s = SmokeHouse.open(store, opts())) {
            s.put(1L, "already here");
        }
        Path csv = dir.resolve("more.csv");
        Files.write(csv, List.of("2,nope"), StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class,
                () -> SmokeHouse.importInto(store, opts(), csv(csv)));
    }
}
