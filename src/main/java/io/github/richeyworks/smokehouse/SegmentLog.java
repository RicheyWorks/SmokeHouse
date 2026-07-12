package io.github.richeyworks.smokehouse;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The append-only segment log — SmokeHouse's body, and its only truth. Records append to the
 * active segment; at the size threshold the segment closes (immutable forever after) and a new
 * one opens. Reads are positional over cached read-only channels, safe from any thread against
 * concurrent appends (bytes at an offset never change once written). Each {@code open} starts a
 * fresh active segment: cheap, and it means a previously-torn tail can never be appended past.
 *
 * <p><b>Compaction commit protocol (Phase 2, crash-safe by ordering):</b> the compacted
 * replacement is fully written and forced to {@code compact.tmp}; then {@code compact.ready}
 * (naming the covered id range) is forced into existence — the point of no return; then originals
 * are deleted, the tmp renamed to the highest covered id, and the marker removed. Every open runs
 * {@link #finishPendingCompaction} first: tmp without marker is discarded (never committed);
 * marker present means the tmp is a complete superset of the covered range, so the replay is
 * idempotent. The marker step also deletes the index hint — its locations reference the old
 * files and must never be trusted again.</p>
 */
final class SegmentLog implements Closeable {

    private static final Pattern SEGMENT_NAME = Pattern.compile("seg-(\\d{8})\\.log");
    static final String COMPACT_TMP = "compact.tmp";
    static final String COMPACT_READY = "compact.ready";
    static final String HINT_FILE = "index.hint";

    private final Path dir;
    private final long segmentBytes;
    private final Fsync fsync;
    private final ConcurrentHashMap<Integer, FileChannel> readers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService fsyncDaemon;   // non-null only for Fsync.INTERVAL

    private FileChannel active;
    private int activeId;
    private long activeSize;

    /** A record's durable address. */
    record Location(int segmentId, long offset) { }

    private SegmentLog(Path dir, long segmentBytes, Fsync fsync, long fsyncIntervalMillis) {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.fsync = fsync;
        if (fsync == Fsync.INTERVAL) {
            this.fsyncDaemon = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "smokehouse-fsync");
                t.setDaemon(true);
                return t;
            });
            fsyncDaemon.scheduleAtFixedRate(() -> {
                try {
                    force();
                } catch (IOException ignored) {
                    // Next interval retries; close() forces unconditionally.
                }
            }, fsyncIntervalMillis, fsyncIntervalMillis, TimeUnit.MILLISECONDS);
        } else {
            this.fsyncDaemon = null;
        }
    }

    static SegmentLog open(Path dir, long segmentBytes, Fsync fsync, long fsyncIntervalMillis)
            throws IOException {
        Files.createDirectories(dir);
        finishPendingCompaction(dir);
        SegmentLog log = new SegmentLog(dir, segmentBytes, fsync, fsyncIntervalMillis);
        int maxExisting = -1;
        for (int id : log.segmentIds()) {
            maxExisting = Math.max(maxExisting, id);
        }
        log.roll(maxExisting + 1);
        return log;
    }

    /**
     * Idempotent replay/rollback of an interrupted compaction commit (see class javadoc).
     * Runs before any segment listing on every open. Case analysis, by what the crash left:
     * no marker → the attempt never committed, discard the scratch; a torn marker → ditto (the
     * marker is forced before any original is deleted, so a torn one proves no delete happened);
     * marker + scratch → the scratch is a complete superset of the covered range, finish the
     * deletes and the rename; marker but <b>no</b> scratch → the rename itself already happened,
     * so {@code seg-maxId} IS the merged replacement and must be kept — only the lower originals
     * (already gone or safe to drop) are cleared. Deleting {@code seg-maxId} in that last case
     * would destroy every live record the compaction preserved.
     */
    static void finishPendingCompaction(Path dir) throws IOException {
        Path tmp = dir.resolve(COMPACT_TMP);
        Path ready = dir.resolve(COMPACT_READY);
        if (!Files.exists(ready)) {
            Files.deleteIfExists(tmp);                    // never committed: discard
            return;
        }
        int minId;
        int maxId;
        try {
            String[] range = Files.readString(ready, StandardCharsets.UTF_8).trim().split(" ");
            minId = Integer.parseInt(range[0]);
            maxId = Integer.parseInt(range[1]);
        } catch (RuntimeException tornMarker) {
            // Crash inside the marker write, before its force returned. Deletes only begin
            // after that force, so every original is intact: the attempt never committed.
            Files.deleteIfExists(tmp);
            Files.deleteIfExists(ready);
            return;
        }
        for (int id = minId; id < maxId; id++) {          // maxId handled below — it may already
            Files.deleteIfExists(dir.resolve(segmentName(id)));   // BE the merged replacement
        }
        if (Files.exists(tmp)) {                          // crash before the rename: finish it
            Files.deleteIfExists(dir.resolve(segmentName(maxId)));   // the pre-compaction original
            Files.move(tmp, dir.resolve(segmentName(maxId)), StandardCopyOption.ATOMIC_MOVE);
        }
        // No tmp: the rename already committed, seg-maxId is the merged segment — keep it.
        Files.deleteIfExists(dir.resolve(HINT_FILE));     // hint locations reference the old files
        Files.deleteIfExists(ready);
    }

    static String segmentName(int id) {
        return String.format("seg-%08d.log", id);
    }

    /**
     * Whether {@code dir} already holds any segment file. The {@code importInto} guard reads this
     * <em>before</em> opening a log (an {@link #open} always creates a fresh active segment, so it
     * could not tell a fresh directory from a populated one). A non-existent directory has none.
     */
    static boolean hasSegments(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        try (var stream = Files.list(dir)) {
            return stream.anyMatch(p -> SEGMENT_NAME.matcher(p.getFileName().toString()).matches());
        }
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

    /** Closed (immutable) segment ids, ascending — everything but the active segment. */
    List<Integer> closedSegmentIds() throws IOException {
        List<Integer> ids = segmentIds();
        ids.remove(Integer.valueOf(activeId));
        return ids;
    }

    Path segmentPath(int id) {
        return dir.resolve(segmentName(id));
    }

    long segmentSize(int id) throws IOException {
        return Files.size(segmentPath(id));
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

    /** Sequential visitor for recovery: every intact record, oldest segment first. */
    interface RecordVisitor {
        void visit(int segmentId, long offset, RecordCodec.Rec rec);
    }

    /** Scan all segments in id order (see {@link #scanAbove}). */
    void scan(RecordVisitor visitor) throws IOException {
        scanAbove(Integer.MIN_VALUE, visitor);
    }

    /**
     * Scan segments with {@code id > minExclusive} in id order — the hint-checkpoint delta path.
     * Each segment stops at its first torn record (crash tail); everything before a tear is
     * intact by the format's construction.
     */
    void scanAbove(int minExclusive, RecordVisitor visitor) throws IOException {
        for (int id : segmentIds()) {
            if (id <= minExclusive) {
                continue;
            }
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(segmentPath(id)), 1 << 16))) {
                long offset = 0;
                while (true) {
                    RecordCodec.Rec rec = RecordCodec.decode(in);
                    if (rec == null || rec.isTorn()) {
                        break;                            // clean end, or crash tail: truncate here
                    }
                    visitor.visit(id, offset, rec);
                    offset += rec.totalBytes();
                }
            }
        }
    }

    // ── Compaction plumbing (the store orchestrates; the log owns the files) ─────────────────

    /** Open the compaction scratch file (any stale one is from an uncommitted attempt). */
    FileChannel openCompactionTmp() throws IOException {
        Files.deleteIfExists(dir.resolve(COMPACT_TMP));
        return FileChannel.open(dir.resolve(COMPACT_TMP),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    /**
     * Commit a fully-written, forced compaction tmp over closed segments {@code [minId..maxId]}.
     * Marker first (the point of no return), then close victim readers (Windows cannot delete
     * open files), delete originals, rename, invalidate the hint, drop the marker.
     */
    synchronized void commitCompaction(int minId, int maxId) throws IOException {
        Path ready = dir.resolve(COMPACT_READY);
        Files.writeString(ready, minId + " " + maxId, StandardCharsets.UTF_8);
        try (FileChannel ch = FileChannel.open(ready, StandardOpenOption.WRITE)) {
            ch.force(true);   // the marker must be durable BEFORE any original is deleted
        }
        for (int id = minId; id <= maxId; id++) {
            FileChannel cached = readers.remove(id);
            if (cached != null) {
                cached.close();
            }
            Files.deleteIfExists(segmentPath(id));
        }
        Files.move(dir.resolve(COMPACT_TMP), segmentPath(maxId), StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(dir.resolve(HINT_FILE));
        Files.deleteIfExists(ready);
    }

    Path hintPath() {
        return dir.resolve(HINT_FILE);
    }

    int activeSegmentId() {
        return activeId;
    }

    int segmentCount() throws IOException {
        return segmentIds().size();
    }

    /** Force pending appends to disk (interval daemon, close, and hint-write all route here). */
    synchronized void force() throws IOException {
        active.force(false);
    }

    @Override
    public void close() throws IOException {
        if (fsyncDaemon != null) {
            fsyncDaemon.shutdownNow();
        }
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
