package io.github.richeyworks.smokehouse;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * The segment manifest (Phase 6): a generation-numbered file naming the live segments and a CRC over
 * each, written atomically (temp + rename). <b>Advisory, never truth</b> — like the hint. Recovery
 * never reads it; it always rebuilds from the segments themselves, so a missing or corrupt manifest
 * costs nothing but the convenience it provides. Its job is to hand backup and replication a
 * consistent, integrity-checkable set to copy, and to keep point-in-time restore points as retained
 * generations (an older generation still {@linkplain #verify verifies} as long as its segments'
 * recorded prefixes survive — the log is append-only, so those bytes never change).
 *
 * <pre>
 *   [magic:4 "SMMF"][version:4][generation:8][entryCount:4]
 *     entryCount × [segmentId:4][byteLength:8][crc32:8]
 *   [crc32:8 of everything above]
 * </pre>
 */
final class ManifestFile {

    private static final int MAGIC = 0x534D_4D46;   // "SMMF"
    private static final int VERSION = 1;
    static final String PREFIX = "manifest-";
    private static final Pattern NAME = Pattern.compile("manifest-(\\d+)");
    private static final int ENTRY_BYTES = 20;      // segmentId(4) + byteLength(8) + crc32(8)

    private ManifestFile() {
    }

    /** One live segment as the manifest names it: its id, its byte length, and a CRC32 over those bytes. */
    record SegmentEntry(int segmentId, long byteLength, long crc32) { }

    /** A generation of the segment set. */
    record Manifest(long generation, List<SegmentEntry> segments) { }

    static String fileName(long generation) {
        return String.format("%s%08d", PREFIX, generation);
    }

    /** CRC32 over the first {@code length} bytes of {@code file} (append-only ⇒ that prefix is immutable). */
    static long crcOf(Path file, long length) throws IOException {
        CRC32 crc = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(1 << 16);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long remaining = length;
            long pos = 0;
            while (remaining > 0) {
                buf.clear();
                if (remaining < buf.capacity()) {
                    buf.limit((int) remaining);
                }
                int n = ch.read(buf, pos);
                if (n < 0) {
                    break;
                }
                buf.flip();
                crc.update(buf);
                pos += n;
                remaining -= n;
            }
        }
        return crc.getValue();
    }

    /** Write {@code m} as {@code manifest-<gen>} in {@code dir}, atomically (temp + ATOMIC_MOVE). */
    static void write(Path dir, Manifest m) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(24 + m.segments().size() * ENTRY_BYTES);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(m.generation());
            out.writeInt(m.segments().size());
            for (SegmentEntry e : m.segments()) {
                out.writeInt(e.segmentId());
                out.writeLong(e.byteLength());
                out.writeLong(e.crc32());
            }
        }
        byte[] payload = bytes.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(payload);
        ByteBuffer whole = ByteBuffer.allocate(payload.length + 8);
        whole.put(payload).putLong(crc.getValue());

        Path target = dir.resolve(fileName(m.generation()));
        Path tmp = dir.resolve(fileName(m.generation()) + ".tmp");
        Files.write(tmp, whole.array());
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /** Load and validate; {@code null} on any doubt (missing, short, CRC, magic, version, insane count). */
    static Manifest load(Path manifestFile) {
        try {
            if (!Files.exists(manifestFile)) {
                return null;
            }
            byte[] whole = Files.readAllBytes(manifestFile);
            if (whole.length < 24) {
                return null;
            }
            ByteBuffer buf = ByteBuffer.wrap(whole);
            CRC32 crc = new CRC32();
            crc.update(whole, 0, whole.length - 8);
            if (buf.getLong(whole.length - 8) != crc.getValue()) {
                return null;
            }
            if (buf.getInt() != MAGIC || buf.getInt() != VERSION) {
                return null;
            }
            long generation = buf.getLong();
            int count = buf.getInt();
            if (count < 0 || (long) count * ENTRY_BYTES > buf.remaining()) {
                return null;
            }
            List<SegmentEntry> segments = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int segId = buf.getInt();
                long len = buf.getLong();
                long segCrc = buf.getLong();
                if (len < 0) {
                    return null;
                }
                segments.add(new SegmentEntry(segId, len, segCrc));
            }
            return new Manifest(generation, segments);
        } catch (Exception anyDoubt) {
            return null;                                   // a manifest is never worth an exception
        }
    }

    /** Generation numbers of every manifest present in {@code dir} (by filename, valid or not), ascending. */
    static List<Long> generations(Path dir) throws IOException {
        List<Long> gens = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return gens;
        }
        try (var stream = Files.list(dir)) {
            stream.forEach(p -> {
                Matcher m = NAME.matcher(p.getFileName().toString());
                if (m.matches()) {
                    gens.add(Long.parseLong(m.group(1)));
                }
            });
        }
        gens.sort(null);
        return gens;
    }

    /** The highest-generation valid manifest in {@code dir}, or {@code null} (a corrupt latest is skipped). */
    static Manifest latest(Path dir) throws IOException {
        List<Long> gens = generations(dir);
        for (int i = gens.size() - 1; i >= 0; i--) {
            Manifest m = load(dir.resolve(fileName(gens.get(i))));
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    /** True if every segment the manifest names exists in {@code dir} with the recorded length and CRC intact. */
    static boolean verify(Path dir, Manifest m) {
        try {
            for (SegmentEntry e : m.segments()) {
                Path seg = dir.resolve(SegmentLog.segmentName(e.segmentId()));
                if (!Files.exists(seg) || Files.size(seg) < e.byteLength()) {
                    return false;
                }
                if (crcOf(seg, e.byteLength()) != e.crc32()) {
                    return false;
                }
            }
            return true;
        } catch (IOException unreadable) {
            return false;
        }
    }
}
