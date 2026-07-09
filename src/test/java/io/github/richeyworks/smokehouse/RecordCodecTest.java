package io.github.richeyworks.smokehouse;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The truth format: round-trips exactly, and every kind of damage decodes as torn, never as data. */
class RecordCodecTest {

    private static DataInputStream in(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    @Test
    void roundTripsLiveRecordsAndTombstones() throws IOException {
        byte[] key = "beef-42".getBytes(StandardCharsets.UTF_8);
        byte[] value = "well done".getBytes(StandardCharsets.UTF_8);

        RecordCodec.Rec live = RecordCodec.decode(in(RecordCodec.encode(key, value, false)));
        assertNotTorn(live);
        assertArrayEquals(key, live.key());
        assertArrayEquals(value, live.value());
        assertFalse(live.tombstone());
        assertEquals(RecordCodec.HEADER_BYTES + key.length + value.length, live.totalBytes());

        RecordCodec.Rec dead = RecordCodec.decode(in(RecordCodec.encode(key, null, true)));
        assertNotTorn(dead);
        assertTrue(dead.tombstone());
        assertNull(dead.value());
    }

    @Test
    void emptyValueIsLegal() throws IOException {
        RecordCodec.Rec rec = RecordCodec.decode(in(RecordCodec.encode(new byte[]{1}, new byte[0], false)));
        assertNotTorn(rec);
        assertEquals(0, rec.value().length);
    }

    @Test
    void anySingleFlippedByteDecodesAsTorn() throws IOException {
        byte[] record = RecordCodec.encode("k".getBytes(StandardCharsets.UTF_8),
                "v".getBytes(StandardCharsets.UTF_8), false);
        Random rnd = new Random(3);
        for (int trial = 0; trial < 50; trial++) {
            byte[] damaged = Arrays.copyOf(record, record.length);
            damaged[rnd.nextInt(damaged.length)] ^= (byte) (1 + rnd.nextInt(255));
            RecordCodec.Rec rec = RecordCodec.decode(in(damaged));
            // Either the CRC catches it, or an insane length does — never a silent wrong record.
            assertTrue(rec.isTorn(), "flipped byte must never decode as valid data (trial " + trial + ")");
        }
    }

    @Test
    void truncatedTailDecodesAsTornAndCleanEofAsNull() throws IOException {
        byte[] record = RecordCodec.encode("key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8), false);
        for (int cut = 5; cut < record.length; cut += 3) {
            RecordCodec.Rec rec = RecordCodec.decode(in(Arrays.copyOf(record, cut)));
            assertTrue(rec.isTorn(), "cut at " + cut + " must read as torn");
        }
        assertNull(RecordCodec.decode(in(new byte[0])), "clean EOF is null, not torn");
    }

    private static void assertNotTorn(RecordCodec.Rec rec) {
        assertTrue(rec != null && !rec.isTorn());
    }
}
