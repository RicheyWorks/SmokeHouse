package io.github.richeyworks.smokehouse;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * The warm-start checkpoint (ADR D4): a clean shutdown writes the sorted live index (plus the
 * per-segment garbage ledger) so the next open loads it and scans only the segments created
 * afterwards — the delta — instead of the whole log. Hints are an <b>optimization, never
 * truth</b>: whole-file CRC, sanity-gated on load (ascending keys, referenced segments must
 * still exist), written to a temp file and atomically moved, and deleted outright by any
 * compaction (its locations reference replaced files). Anything suspicious → full scan, which
 * is always correct.
 *
 * <pre>
 *   [magic:4 "SMKH"][version:4][maxCoveredSegmentId:4][entryCount:4]
 *     entryCount × [keyLen:4][keyBytes][segmentId:4][offset:8][recordBytes:4]
 *   [garbageCount:4] × [segmentId:4][deadBytes:8]
 *   [crc32:8 of everything above]
 * </pre>
 */
final class HintFile {

    private static final int MAGIC = 0x534D_4B48;   // "SMKH"
    private static final int VERSION = 1;

    private HintFile() {
    }

    /** Everything a warm start needs: the checkpointed index, its coverage, and the garbage ledger. */
    record Hint<K>(List<IndexEntry<K>> entries, int maxCoveredSegmentId, Map<Integer, Long> garbage) { }

    /** Key-bytes seam: the store supplies its serializer both ways. */
    interface KeyCodec<K> {
        byte[] toBytes(K key) throws IOException;

        K fromBytes(byte[] bytes) throws IOException;
    }

    static <K> void write(Path hintPath, List<IndexEntry<K>> sortedEntries,
                          int maxCoveredSegmentId, Map<Integer, Long> garbage,
                          KeyCodec<K> keys) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(64 + sortedEntries.size() * 32);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(maxCoveredSegmentId);
            out.writeInt(sortedEntries.size());
            for (IndexEntry<K> e : sortedEntries) {
                byte[] kb = keys.toBytes(e.key());
                out.writeInt(kb.length);
                out.write(kb);
                out.writeInt(e.segmentId());
                out.writeLong(e.offset());
                out.writeInt(e.recordBytes());
            }
            out.writeInt(garbage.size());
            for (var g : garbage.entrySet()) {
                out.writeInt(g.getKey());
                out.writeLong(g.getValue());
            }
        }
        byte[] payload = bytes.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(payload);
        ByteBuffer whole = ByteBuffer.allocate(payload.length + 8);
        whole.put(payload).putLong(crc.getValue());

        Path tmp = hintPath.resolveSibling(hintPath.getFileName() + ".tmp");
        Files.write(tmp, whole.array());
        Files.move(tmp, hintPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Load and validate; {@code null} on any doubt (missing, CRC, version, non-ascending keys,
     * or locations referencing segments that no longer exist / are too short). Callers fall back
     * to the full scan — correctness never depends on a hint.
     */
    static <K> Hint<K> load(Path hintPath, KeyCodec<K> keys, Comparator<? super K> keyOrder,
                            SegmentLog log) {
        try {
            if (!Files.exists(hintPath)) {
                return null;
            }
            byte[] whole = Files.readAllBytes(hintPath);
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
            int maxCovered = buf.getInt();
            int count = buf.getInt();
            if (count < 0) {
                return null;
            }
            Map<Integer, Long> segmentSizes = new HashMap<>();
            for (int id : log.segmentIds()) {
                segmentSizes.put(id, log.segmentSize(id));
            }
            List<IndexEntry<K>> entries = new ArrayList<>(count);
            K prev = null;
            for (int i = 0; i < count; i++) {
                int keyLen = buf.getInt();
                if (keyLen < 0 || keyLen > RecordCodec.MAX_KEY_BYTES || keyLen > buf.remaining()) {
                    return null;
                }
                byte[] kb = new byte[keyLen];
                buf.get(kb);
                K key = keys.fromBytes(kb);
                int segId = buf.getInt();
                long offset = buf.getLong();
                int recBytes = buf.getInt();
                Long segSize = segmentSizes.get(segId);
                if (segSize == null || offset < 0 || recBytes <= 0 || offset + recBytes > segSize) {
                    return null;                          // references a replaced/short segment
                }
                if (prev != null && keyOrder.compare(prev, key) >= 0) {
                    return null;                          // not strictly ascending: corrupt
                }
                prev = key;
                entries.add(new IndexEntry<>(key, segId, offset, recBytes));
            }
            int garbageCount = buf.getInt();
            if (garbageCount < 0) {
                return null;
            }
            Map<Integer, Long> garbage = new HashMap<>();
            for (int i = 0; i < garbageCount; i++) {
                garbage.put(buf.getInt(), buf.getLong());
            }
            return new Hint<>(entries, maxCovered, garbage);
        } catch (Exception anyDoubt) {
            return null;                                  // a hint is never worth an exception
        }
    }
}
