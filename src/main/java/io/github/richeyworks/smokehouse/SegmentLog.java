package io.github.richeyworks.smokehouse;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The append-only segment log — SmokeHouse's body, and its only truth. Records append to the
 * active segment; at the size threshold the segment closes (immutable forever after) and a new
 * one opens. Reads are positional over cached read-only channels, safe from any thread against
 * concurrent appends (bytes at an offset never change once written). Each {@code open} starts a
 * fresh active segment: cheap, and it means a previously-torn tail can never be appended past.
 */
final class SegmentLog implements Closeable {

    private static final Pattern SEGMENT_NAME = Pattern.compile("seg-(\\d{8})\\.log");

    private final Path dir;
    private final long segmentBytes;
    private final Fsync fsync;
    private final ConcurrentHashMap<Integer, FileChannel> readers = new ConcurrentHashMap<>();

    private FileChannel active;
    private int activeId;
    private long activeSize;

    /** A record's durable address. */
    record Location(int segmentId, long offset) { }

    private SegmentLog(Path dir, long segmentBytes, Fsync fsync) {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.fsync = fsync;
    }

    static SegmentLog open(Path dir, long segmentBytes, Fsync fsync) throws IOException {
        Files.createDirectories(dir);
        SegmentLog log = new SegmentLog(dir, segmentBytes, fsync);
        int maxExisting = -1;
        for (int id : log.segmentIds()) {
            maxExisting = Math.max(maxExisting, id);
        }
        log.roll(maxExisting + 1);
        return log;
    }

    /** Existing segment ids on disk, ascending (includes the active one once created). */
    List<Integer> segmentIds() throws IOException {
        List<Integer> ids = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return ids;
        }
        try (var stream = Files.list(dir)) {
            stream.forEach(p -> {
                Matcher m = SEGMENT_NAME.matcher(p.getFileName().toString());
                if (m.matches()) {
                    ids.add(Integer.parseInt(m.group(1)));
                }
            });
        }
        ids.sort(null);
        return ids;
    }

    private Path segmentPath(int id) {
        return dir.resolve(String.format("seg-%08d.log", id));
    }

    private void roll(int newId) throws IOException {
        if (active != null) {
            active.force(false);
            active.close();
        }
        activeId = newId;
        activeSize = 0;
        active = FileChannel.open(segmentPath(newId),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    /** Append one encoded record (single-writer: callers hold the store lock). */
    synchronized Location append(byte[] record) throws IOException {
        if (activeSize > 0 && activeSize + record.length > segmentBytes) {
            roll(activeId + 1);
        }
        long offset = activeSize;
        ByteBuffer buf = ByteBuffer.wrap(record);
        while (buf.hasRemaining()) {
            active.write(buf);
        }
        activeSize += record.length;
        if (fsync == Fsync.ALWAYS) {
            active.force(false);
        }
        return new Location(activeId, offset);
    }

    /** Positional read of the record at {@code loc} — thread-safe, works on active and closed segments. */
    RecordCodec.Rec read(Location loc) throws IOException {
        FileChannel ch = readers.computeIfAbsent(loc.segmentId(), id -> {
            try {
                return FileChannel.open(segmentPath(id), StandardOpenOption.READ);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
        // Stream a bounded window from the offset through the codec (header first, then body).
        long remaining = ch.size() - loc.offset();
        if (remaining < RecordCodec.HEADER_BYTES) {
            return RecordCodec.Rec.torn();
        }
        InputStream in = new InputStream() {
            private long pos = loc.offset();
            @Override public int read() throws IOException {
                byte[] one = new byte[1];
                int n = read(one, 0, 1);
                return n < 0 ? -1 : one[0] & 0xFF;
            }
            @Override public int read(byte[] b, int off, int len) throws IOException {
                int n = ch.read(ByteBuffer.wrap(b, off, len), pos);
                if (n > 0) {
                    pos += n;
                }
                return n;
            }
        };
        return RecordCodec.decode(new DataInputStream(new BufferedInputStream(in, 8192)));
    }

    /** Sequential visitor for recovery: every intact record in every segment, oldest first. */
    interface RecordVisitor {
        void visit(int segmentId, long offset, RecordCodec.Rec rec);
    }

    /**
     * Scan all segments in id order, stopping a segment at its first torn record (crash tail) —
     * everything before a tear is intact by the format's construction.
     */
    void scan(RecordVisitor visitor) throws IOException {
        for (int id : segmentIds()) {
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(segmentPath(id)), 1 << 16))) {
                long offset = 0;
                while (true) {
                    RecordCodec.Rec rec = RecordCodec.decode(in);
                    if (rec == null) {
                        break;                       // clean end of segment
                    }
                    if (rec.isTorn()) {
                        break;                       // crash tail: truncate here, keep the rest
                    }
                    visitor.visit(id, offset, rec);
                    offset += rec.totalBytes();
                }
            }
        }
    }

    int activeSegmentId() {
        return activeId;
    }

    int segmentCount() throws IOException {
        return segmentIds().size();
    }

    /** Force pending appends to disk (used by close and by future fsync-interval policies). */
    synchronized void force() throws IOException {
        active.force(false);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            active.force(false);
            active.close();
        }
        for (FileChannel ch : readers.values()) {
            ch.close();
        }
        readers.clear();
    }
}
