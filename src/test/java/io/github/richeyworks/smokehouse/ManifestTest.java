package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The advisory segment manifest: names the live segments with per-segment CRCs, numbers generations
 * monotonically, verifies an intact set, and — being advisory — is simply absent (never wrong) when
 * corrupt.
 */
class ManifestTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)                        // tiny: churn produces several segments to name
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    @Test
    void manifestNamesLiveSegmentsAndVerifies(@TempDir Path dir) throws IOException {
        long gen;
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            Random rnd = new Random(1);
            for (int i = 0; i < 2_000; i++) {
                store.put((long) rnd.nextInt(500), "v" + i);
            }
            gen = store.writeManifest();
        }
        assertEquals(1L, gen);
        ManifestFile.Manifest m = ManifestFile.latest(dir);
        assertNotNull(m);
        assertEquals(1L, m.generation());
        assertFalse(m.segments().isEmpty(), "manifest should name the live segments");
        assertTrue(ManifestFile.verify(dir, m), "every named segment matches its recorded length + CRC");
    }

    @Test
    void generationsIncrementAndLatestWins(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            store.put(1L, "a");
            assertEquals(1L, store.writeManifest());
            store.put(2L, "b");
            assertEquals(2L, store.writeManifest());
        }
        assertEquals(List.of(1L, 2L), ManifestFile.generations(dir));
        ManifestFile.Manifest latest = ManifestFile.latest(dir);
        assertNotNull(latest);
        assertEquals(2L, latest.generation());
    }

    @Test
    void corruptManifestIsIgnoredAndCorruptSegmentFailsVerify(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            for (long k = 0; k < 300; k++) {
                store.put(k, "v" + k);
            }
            store.writeManifest();                          // generation 1
        }
        // Corrupt the manifest → load returns null; it is never trusted, only convenient.
        Path manifest = dir.resolve(ManifestFile.fileName(1));
        byte[] bytes = Files.readAllBytes(manifest);
        bytes[bytes.length / 2] ^= 0x5A;
        Files.write(manifest, bytes);
        assertNull(ManifestFile.load(manifest));

        // A fresh manifest still numbers monotonically (past the corrupt file's generation).
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            store.put(999L, "z");
            assertEquals(2L, store.writeManifest());
        }
        ManifestFile.Manifest m = ManifestFile.latest(dir);
        assertNotNull(m);
        assertEquals(2L, m.generation());
        assertTrue(ManifestFile.verify(dir, m));

        // Tamper with a named segment → verify must catch it.
        Path seg = dir.resolve(SegmentLog.segmentName(m.segments().get(0).segmentId()));
        byte[] segBytes = Files.readAllBytes(seg);
        segBytes[0] ^= 0x01;
        Files.write(seg, segBytes);
        assertFalse(ManifestFile.verify(dir, m), "a tampered segment must fail verify");
    }
}
