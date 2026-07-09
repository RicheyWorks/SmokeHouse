package io.github.richeyworks.smokehouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The body: appends roll, positional reads hit, scans survive crash tails. */
class SegmentLogTest {

    private static byte[] rec(String k, String v) {
        return RecordCodec.encode(k.getBytes(StandardCharsets.UTF_8),
                v.getBytes(StandardCharsets.UTF_8), false);
    }

    @Test
    void appendsRollAndPositionalReadsHit(@TempDir Path dir) throws IOException {
        try (SegmentLog log = SegmentLog.open(dir, 1024, Fsync.OS)) {   // tiny segments: force rolls
            List<SegmentLog.Location> locs = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                locs.add(log.append(rec("key-" + i, "value-" + i + "-padding-padding-padding")));
            }
            assertTrue(log.segmentCount() > 1, "1KB segments must have rolled");
            for (int i = 0; i < 100; i++) {
                RecordCodec.Rec r = log.read(locs.get(i));
                assertArrayEquals(("key-" + i).getBytes(StandardCharsets.UTF_8), r.key());
            }
        }
    }

    @Test
    void scanVisitsEverythingInOrderAcrossReopen(@TempDir Path dir) throws IOException {
        try (SegmentLog log = SegmentLog.open(dir, 512, Fsync.OS)) {
            for (int i = 0; i < 40; i++) {
                log.append(rec("k" + i, "v" + i));
            }
        }
        try (SegmentLog reopened = SegmentLog.open(dir, 512, Fsync.OS)) {
            List<String> seen = new ArrayList<>();
            reopened.scan((seg, off, r) -> seen.add(new String(r.key(), StandardCharsets.UTF_8)));
            assertEquals(40, seen.size());
            assertEquals("k0", seen.get(0));
            assertEquals("k39", seen.get(39));
        }
    }

    @Test
    void crashTailIsTruncatedNotFatal(@TempDir Path dir) throws IOException {
        Path victim;
        try (SegmentLog log = SegmentLog.open(dir, 1 << 20, Fsync.ALWAYS)) {
            for (int i = 0; i < 10; i++) {
                log.append(rec("k" + i, "v" + i));
            }
            victim = dir.resolve(String.format("seg-%08d.log", log.activeSegmentId()));
        }
        // Simulate a crash mid-append: garbage bytes on the tail of the newest segment.
        Files.write(victim, new byte[]{7, 7, 7, 7, 7, 7, 7}, StandardOpenOption.APPEND);

        try (SegmentLog reopened = SegmentLog.open(dir, 1 << 20, Fsync.OS)) {
            List<String> seen = new ArrayList<>();
            reopened.scan((seg, off, r) -> seen.add(new String(r.key(), StandardCharsets.UTF_8)));
            assertEquals(10, seen.size(), "everything before the tear survives; the tear is dropped");
        }
    }
}
