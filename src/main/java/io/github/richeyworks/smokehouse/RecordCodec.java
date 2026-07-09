package io.github.richeyworks.smokehouse;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * The on-disk record format — the only truth in a SmokeHouse (ADR: every index is a cache of it).
 *
 * <pre>
 *   [crc32:4][flags:1][keyLen:4][valLen:4][key bytes][value bytes]
 * </pre>
 *
 * CRC32 covers everything after itself ({@code flags..value}). {@code flags} bit 0 = tombstone
 * (tombstones carry {@code valLen = 0} and no value bytes). A record torn by a crash mid-append
 * fails its CRC (or ends the file early) and decodes as {@link #torn()}; everything before it in
 * the segment is intact by construction, so recovery truncates the tail and loses nothing that
 * was ever durably written.
 */
final class RecordCodec {

    /** Header bytes before the key: crc(4) + flags(1) + keyLen(4) + valLen(4). */
    static final int HEADER_BYTES = 13;
    static final int MAX_KEY_BYTES = 1 << 20;      // 1 MB keys: generous, and a corruption guard
    static final int MAX_VALUE_BYTES = 1 << 28;    // 256 MB values: ditto

    private static final byte FLAG_TOMBSTONE = 1;

    private RecordCodec() {
    }

    /** One decoded record; {@code value} is {@code null} for tombstones. */
    record Rec(byte[] key, byte[] value, boolean tombstone, int totalBytes) {

        static Rec torn() {
            return new Rec(null, null, false, -1);
        }

        boolean isTorn() {
            return totalBytes < 0;
        }
    }

    /** Encode a live record or (with {@code value == null}) a tombstone. */
    static byte[] encode(byte[] key, byte[] value, boolean tombstone) {
        int valLen = (value == null) ? 0 : value.length;
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + key.length + valLen);
        buf.position(4);                                   // CRC written last
        buf.put(tombstone ? FLAG_TOMBSTONE : 0);
        buf.putInt(key.length);
        buf.putInt(valLen);
        buf.put(key);
        if (valLen > 0) {
            buf.put(value);
        }
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 4, buf.capacity() - 4);
        buf.putInt(0, (int) crc.getValue());
        return buf.array();
    }

    /**
     * Decode the next record from a stream (sequential scan path). Returns {@code null} at a
     * clean end-of-file (the previous record was the last), or {@link Rec#torn()} for a torn or
     * corrupt tail — the caller stops scanning that segment and treats everything before as good.
     */
    static Rec decode(DataInputStream in) throws IOException {
        int storedCrc;
        try {
            storedCrc = in.readInt();
        } catch (EOFException cleanEnd) {
            return null;
        }
        try {
            byte flags = in.readByte();
            int keyLen = in.readInt();
            int valLen = in.readInt();
            if (keyLen < 0 || keyLen > MAX_KEY_BYTES || valLen < 0 || valLen > MAX_VALUE_BYTES) {
                return Rec.torn();                          // insane lengths = corrupt header
            }
            byte[] key = new byte[keyLen];
            in.readFully(key);
            byte[] value = new byte[valLen];
            in.readFully(value);

            CRC32 crc = new CRC32();
            crc.update(flags);
            crc.update(intBytes(keyLen));
            crc.update(intBytes(valLen));
            crc.update(key);
            crc.update(value);
            if ((int) crc.getValue() != storedCrc) {
                return Rec.torn();
            }
            boolean tombstone = (flags & FLAG_TOMBSTONE) != 0;
            return new Rec(key, tombstone ? null : value, tombstone,
                    HEADER_BYTES + keyLen + valLen);
        } catch (EOFException torn) {
            return Rec.torn();                              // file ended mid-record
        }
    }

    private static byte[] intBytes(int v) {
        return new byte[]{(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }
}
