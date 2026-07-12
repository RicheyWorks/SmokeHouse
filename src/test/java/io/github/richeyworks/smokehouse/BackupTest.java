package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Backup/restore: a {@code backup(targetDir)} taken mid-churn captures exactly the instant it ran —
 * nothing written afterward leaks in — and {@code restore} (an {@code open} on the copy) rebuilds
 * that instant precisely, because a backup is just recovery's input relocated. The copy also carries
 * a verifiable manifest.
 */
class BackupTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(4096)                        // small: the backup spans several segments
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    @Test
    void backupCapturesTheInstantAndRestoresExactly(@TempDir Path root) throws IOException {
        Path live = root.resolve("live");
        Path backup = root.resolve("backup");
        TreeMap<Long, String> atBackup;
        long gen;
        try (SmokeHouse<Long, String> store = SmokeHouse.open(live, opts())) {
            TreeMap<Long, String> oracle = new TreeMap<>();
            Random rnd = new Random(7);
            for (int i = 0; i < 1_500; i++) {
                apply(store, oracle, rnd, i);
            }
            atBackup = new TreeMap<>(oracle);
            gen = store.backup(backup);
            for (int i = 1_500; i < 2_500; i++) {          // keep mutating: none may leak into the copy
                apply(store, oracle, rnd, i);
            }
        }
        assertEquals(1L, gen);

        try (SmokeHouse<Long, String> restored = SmokeHouse.restore(backup, opts())) {
            assertEquals(atBackup.size(), restored.size(), "restored size == the state at backup time");
            for (var e : atBackup.entrySet()) {
                assertEquals(e.getValue(), restored.get(e.getKey()), "key " + e.getKey());
            }
        }

        ManifestFile.Manifest m = ManifestFile.latest(backup);
        assertNotNull(m);
        assertTrue(ManifestFile.verify(backup, m), "the backup's manifest verifies against its segments");
    }

    private static void apply(SmokeHouse<Long, String> store, TreeMap<Long, String> oracle,
                              Random rnd, int i) throws IOException {
        if (!oracle.isEmpty() && rnd.nextInt(5) == 0) {
            long dk = new ArrayList<>(oracle.keySet()).get(rnd.nextInt(oracle.size()));
            store.delete(dk);
            oracle.remove(dk);
        } else {
            long k = rnd.nextInt(400);
            String v = "v" + i;
            store.put(k, v);
            oracle.put(k, v);
        }
    }
}
