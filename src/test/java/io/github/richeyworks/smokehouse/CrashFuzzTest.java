package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Phase 6 crash-fuzz harness (append + torn-tail). SmokeHouse has no fault-injection seam by design,
 * and it needs none: "the log is the only truth, and bytes at an offset never change once written."
 * A process crash therefore leaves the segment file truncated at <em>some</em> byte. So this writes a
 * seeded workload to a single-segment log and then proves recovery correct at <b>every</b> byte a
 * crash could stop at — each complete-record boundary (a clean prefix) and a partial write into the
 * next record (a torn tail). Recovery must always equal the last-writer-wins state of exactly the
 * records that survived: a crash loses only work never fully written, nothing durable.
 *
 * <p>Deterministic: the (seed, truncation-length) pair reproduces any failure. Compaction-commit
 * crash windows are covered by {@code Phase2Test} (the marker-protocol tests); hint corruption by
 * {@code Phase2Test.corruptHintFallsBackToTheFullScan}.</p>
 */
class CrashFuzzTest {

    private record Op(long key, String value, boolean delete) { }

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(1 << 30)                     // one segment: the whole log is seg-0
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    /** Seeded workload with a 1:1 op→record mapping: put (new or overwrite) or delete-of-present. */
    private static List<Op> workload(long seed, int n) {
        Random rnd = new Random(seed);
        List<Op> ops = new ArrayList<>(n);
        TreeMap<Long, String> live = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            if (!live.isEmpty() && rnd.nextInt(4) == 0) {              // delete a currently-present key
                long k = new ArrayList<>(live.keySet()).get(rnd.nextInt(live.size()));
                ops.add(new Op(k, null, true));
                live.remove(k);
            } else {                                                   // put a fresh or overwriting key
                long k = rnd.nextInt(40);                              // small space → overwrites + re-adds
                String v = "v" + i;
                ops.add(new Op(k, v, false));
                live.put(k, v);
            }
        }
        return ops;
    }

    @Test
    void recoveryIsCorrectAtEveryAppendCrashBoundary(@TempDir Path root) throws IOException {
        long seed = 424_242L;
        int n = 150;
        List<Op> ops = workload(seed, n);

        // Write the whole workload to one segment, capturing the LWW oracle after each op.
        Path liveDir = root.resolve("live");
        List<TreeMap<Long, String>> oracleAfter = new ArrayList<>();
        oracleAfter.add(new TreeMap<>());                              // after 0 ops
        try (SmokeHouse<Long, String> store = SmokeHouse.open(liveDir, opts())) {
            TreeMap<Long, String> oracle = new TreeMap<>();
            for (Op op : ops) {
                if (op.delete()) {
                    store.delete(op.key());
                    oracle.remove(op.key());
                } else {
                    store.put(op.key(), op.value());
                    oracle.put(op.key(), op.value());
                }
                oracleAfter.add(new TreeMap<>(oracle));
            }
        }

        byte[] log = Files.readAllBytes(liveDir.resolve(SegmentLog.segmentName(0)));
        int[] recordEnd = recordBoundaries(log);
        assertEquals(n, recordEnd.length, "expected exactly one record per op");

        Path snap = root.resolve("snap");

        // Clean crash after K ops: truncate to the end of record K-1, recover, expect oracleAfter[K].
        for (int i = -1; i < n; i++) {
            int len = (i < 0) ? 0 : recordEnd[i];
            recoverAndAssert(snap, log, len, oracleAfter.get(i + 1), "clean crash after " + (i + 1) + " ops");
        }

        // Torn tail: a partial write into record i must drop it → the state of the ops before it.
        for (int i = 0; i < n; i++) {
            int start = (i == 0) ? 0 : recordEnd[i - 1];
            int end = recordEnd[i];
            if (end - start > 2) {
                int tornLen = start + (end - start) / 2;              // stop mid-record i
                recoverAndAssert(snap, log, tornLen, oracleAfter.get(i), "torn tail mid-record " + i);
            }
        }
    }

    /** Byte offset at which each record ends, parsed straight from the log headers (no decode). */
    private static int[] recordBoundaries(byte[] log) {
        List<Integer> ends = new ArrayList<>();
        int off = 0;
        while (off + RecordCodec.HEADER_BYTES <= log.length) {
            int keyLen = intAt(log, off + 5);                          // crc(4) flags(1) keyLen(4) valLen(4)
            int valLen = intAt(log, off + 9);
            if (keyLen < 0 || valLen < 0) {
                break;
            }
            int recLen = RecordCodec.HEADER_BYTES + keyLen + valLen;
            if (off + recLen > log.length) {
                break;                                                 // trailing partial (not expected post-close)
            }
            off += recLen;
            ends.add(off);
        }
        int[] out = new int[ends.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = ends.get(i);
        }
        return out;
    }

    private static int intAt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    /** Write {@code log[0..len)} as the sole segment of a fresh dir, reopen, and assert it equals {@code oracle}. */
    private static void recoverAndAssert(Path dir, byte[] log, int len,
                                         TreeMap<Long, String> oracle, String what) throws IOException {
        wipe(dir);
        Files.createDirectories(dir);
        Files.write(dir.resolve(SegmentLog.segmentName(0)), java.util.Arrays.copyOf(log, len));
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            assertEquals(oracle.size(), store.size(), what + ": size");
            for (var e : oracle.entrySet()) {
                assertEquals(e.getValue(), store.get(e.getKey()), what + ": key " + e.getKey());
            }
        }
    }

    /** Delete the dir's contents (segment + any hint a prior reopen wrote) so each recovery is a cold scan. */
    private static void wipe(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best effort
                }
            });
        }
    }
}
