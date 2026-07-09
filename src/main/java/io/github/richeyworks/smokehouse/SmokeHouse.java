package io.github.richeyworks.smokehouse;

import io.github.richeyworks.csrbt.OrderedSet;
import io.github.richeyworks.csrbt.adapter.NavigableOrderedSet;
import io.github.richeyworks.csrbt.control.MorphPolicy;
import io.github.richeyworks.csrbt.event.TreeEvent;
import io.github.richeyworks.csrbt.strategy.RedBlackStrategy;
import io.github.richeyworks.superbeefsort.BeefSort;
import io.github.richeyworks.superbeefsort.csrbt.WorkloadAdaptation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * SmokeHouse — the third engine: a log-structured record store whose primary index is an adaptive
 * CSRBT set and whose recovery engine is SuperBeefSort. The append-only {@link SegmentLog} is the
 * only truth; every index is a cache of it. Cold recovery is scan → last-writer-wins →
 * <b>SuperBeefSort sort → {@code OrderedSet.fromSorted}</b> (the O(n) zero-rotation build); warm
 * recovery loads the {@linkplain HintFile hint checkpoint} a clean shutdown wrote and scans only
 * the delta. See {@code SuperBeefSort/docs/adr-smokehouse-ecosystem-ring.md}.
 *
 * <p><b>Phase 2 surfaces:</b> {@link Fsync#INTERVAL} group durability (the default, 50 ms);
 * per-segment garbage accounting funded by {@link IndexEntry#recordBytes()};
 * {@link #compact()} — merges all closed segments into one key-ordered segment via a crash-safe
 * marker protocol, reclaiming dead bytes while reads continue (an overlapping read retries once
 * against the repointed index); and the retention tier
 * ({@link SmokeHouseOptions#retainNewest}) — keep only the newest-written N keys, evictions
 * becoming compactable garbage with no tombstones (recovery re-derives newest-N from log order,
 * which <em>is</em> write order).</p>
 *
 * <p><b>Concurrency:</b> single writer (all mutation, the pilot, and compaction's commit under
 * one store lock); reads resolve the index under the lock and read the log outside it.
 * <b>Bounds, honestly:</b> all keys in memory (the Bitcask trade); key {@code equals} must agree
 * with the comparator; after an index morph the retention window's FIFO order falls back to
 * ascending until writes re-establish it (CSRBT's documented safety net).</p>
 */
public final class SmokeHouse<K, V> implements Closeable {

    private final Object lock = new Object();
    private final SmokeHouseOptions<K, V> opts;
    private final SegmentLog log;
    private final OrderedSet<IndexEntry<K>> index;
    private final NavigableOrderedSet<IndexEntry<K>> navigable;
    private final WorkloadAdaptation<IndexEntry<K>> adaptation;   // null in STATIC tier
    private final ScheduledExecutorService pilot;                 // null in STATIC tier
    private final Map<Integer, Long> garbage = new HashMap<>();   // segmentId -> dead bytes; store-lock guarded
    private volatile String pilotVerdict = "not yet evaluated";
    private long puts, gets, deletes;
    private boolean closed;

    private SmokeHouse(SmokeHouseOptions<K, V> opts, SegmentLog log, OrderedSet<IndexEntry<K>> index,
                       WorkloadAdaptation<IndexEntry<K>> adaptation, Map<Integer, Long> recoveredGarbage) {
        this.opts = opts;
        this.log = log;
        this.index = index;
        this.navigable = new NavigableOrderedSet<>(index);
        this.adaptation = adaptation;
        this.garbage.putAll(recoveredGarbage);

        if (opts.retention() > 0) {
            // Retention tier: CSRBT's FIFO window IS the mechanism. Upserts re-enter at the tail
            // (put = remove+add), so eviction order is newest-write-wins. Evicted entries fund
            // the garbage ledger via the event seam — no tombstones needed (see class javadoc).
            index.setEventListener(e -> {
                if (e instanceof TreeEvent.Evict<IndexEntry<K>> ev) {
                    addGarbage(ev.key().segmentId(), ev.key().recordBytes());
                }
            });
            index.setMaxSize(opts.retention());
        }
        if (adaptation != null) {
            // The pilot shares the STORE lock (not Autopilot's own): store ops are compound
            // (log append + index update), so single-threaded-equivalence must be enforced here.
            this.pilot = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "smokehouse-pilot");
                t.setDaemon(true);
                return t;
            });
            long ms = opts.cadence().toMillis();
            pilot.scheduleAtFixedRate(() -> {
                synchronized (lock) {
                    var r = adaptation.maybeAdapt();
                    pilotVerdict = r.morphed() ? "morph " + r.from() + " -> " + r.to()
                                               : "hold (" + r.reason() + ")";
                }
            }, ms, ms, TimeUnit.MILLISECONDS);
        } else {
            this.pilot = null;
        }
    }

    /** Open (or create) a store at {@code dir}, recovering the index from the log (hint-accelerated). */
    public static <K, V> SmokeHouse<K, V> open(Path dir, SmokeHouseOptions<K, V> opts) throws IOException {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(opts, "opts");
        SegmentLog log = SegmentLog.open(dir, opts.segmentBytesLimit(), opts.fsyncPolicy(),
                opts.fsyncInterval());

        // ── Recovery: (hint checkpoint +) delta scan → last-writer-wins → sort → O(n) build ──
        HintFile.KeyCodec<K> codec = keyCodec(opts);
        HintFile.Hint<K> hint = HintFile.load(log.hintPath(), codec, opts.comparator(), log);
        Map<K, IndexEntry<K>> live = new HashMap<>();
        Map<Integer, Long> recoveredGarbage = new HashMap<>();
        boolean[] delta = {false};
        int scanFloor = Integer.MIN_VALUE;
        if (hint != null) {
            for (IndexEntry<K> e : hint.entries()) {
                live.put(e.key(), e);
            }
            recoveredGarbage.putAll(hint.garbage());
            scanFloor = hint.maxCoveredSegmentId();
        }
        log.scanAbove(scanFloor, (segId, offset, rec) -> {
            delta[0] = true;
            K key = readKey(opts, rec.key());
            IndexEntry<K> prev;
            if (rec.tombstone()) {
                prev = live.remove(key);
                recoveredGarbage.merge(segId, (long) rec.totalBytes(), Long::sum);
            } else {
                prev = live.put(key, new IndexEntry<>(key, segId, offset, rec.totalBytes()));
            }
            if (prev != null) {
                recoveredGarbage.merge(prev.segmentId(), (long) prev.recordBytes(), Long::sum);
            }
        });

        List<IndexEntry<K>> entries = new ArrayList<>(live.values());
        if (opts.retention() > 0 && entries.size() > opts.retention()) {
            // Newest-written N survive: log order is write order, so (segmentId, offset) is age.
            entries.sort(Comparator.<IndexEntry<K>>comparingInt(IndexEntry::segmentId)
                    .thenComparingLong(IndexEntry::offset));
            int cut = entries.size() - opts.retention();
            for (int i = 0; i < cut; i++) {
                IndexEntry<K> evicted = entries.get(i);
                recoveredGarbage.merge(evicted.segmentId(), (long) evicted.recordBytes(), Long::sum);
            }
            entries = new ArrayList<>(entries.subList(cut, entries.size()));
            delta[0] = true;
        }
        Comparator<IndexEntry<K>> ordering = IndexEntry.ordering(opts.comparator());
        if ((hint == null || delta[0]) && entries.size() > 1) {
            entries = BeefSort.with(ordering).source(entries).run().sorted();
        }
        OrderedSet<IndexEntry<K>> index =
                OrderedSet.fromSorted(entries, new RedBlackStrategy<>(), ordering);
        WorkloadAdaptation<IndexEntry<K>> adaptation =
                (opts.tier() == SmokeHouseOptions.IndexTier.ADAPTIVE)
                        ? WorkloadAdaptation.attach(index, MorphPolicy.defaults())
                        : null;
        return new SmokeHouse<>(opts, log, index, adaptation, recoveredGarbage);
    }

    // ── Data plane ────────────────────────────────────────────────────────────────────────────

    /** Insert or overwrite. Durability per the {@link Fsync} policy; the log append IS the WAL write. */
    public void put(K key, V value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] record = RecordCodec.encode(keyBytes(key), valueBytes(value), false);
        synchronized (lock) {
            IndexEntry<K> prev = floorHit(key);
            SegmentLog.Location loc = log.append(record);            // truth before cache
            IndexEntry<K> entry = new IndexEntry<>(key, loc.segmentId(), loc.offset(), record.length);
            if (adaptation != null) {
                adaptation.remove(IndexEntry.probe(key));             // upsert = remove + add (ADR D2)
                adaptation.add(entry);
            } else {
                index.remove(IndexEntry.probe(key));
                index.add(entry);
            }
            if (prev != null) {
                addGarbage(prev.segmentId(), prev.recordBytes());     // the old version just died
            }
            puts++;
        }
    }

    /**
     * The newest value for {@code key}, or {@code null}. Index under the lock; log read outside
     * it — with one retry, because a read can overlap a compaction commit that just repointed
     * its entry into the merged segment.
     */
    public V get(K key) throws IOException {
        Objects.requireNonNull(key, "key");
        for (int attempt = 0; ; attempt++) {
            IndexEntry<K> entry;
            synchronized (lock) {
                entry = floorHit(key);
                if (adaptation != null) {
                    adaptation.recordSearch(key.hashCode());          // read mix + skew for the scorer
                }
                gets++;
            }
            if (entry == null) {
                return null;
            }
            try {
                RecordCodec.Rec rec = log.read(entry.location());
                if (rec == null || rec.isTorn() || rec.tombstone()) {
                    throw new IOException("unreadable record at " + entry.location());
                }
                return readValue(rec.value());
            } catch (IOException | UncheckedIOException raced) {
                if (attempt >= 1) {
                    throw new IOException("log/index divergence for key " + key
                            + " at " + entry.location(), raced);
                }
                // fall through: re-resolve against the (possibly just-compacted) index
            }
        }
    }

    /** Delete: a durable tombstone in the log, then the index entry goes. Returns whether the key existed. */
    public boolean delete(K key) throws IOException {
        Objects.requireNonNull(key, "key");
        byte[] record = RecordCodec.encode(keyBytes(key), null, true);
        synchronized (lock) {
            IndexEntry<K> prev = floorHit(key);
            SegmentLog.Location loc = log.append(record);             // truth before cache
            addGarbage(loc.segmentId(), record.length);               // a tombstone is born dead weight
            if (adaptation != null) {
                adaptation.remove(IndexEntry.probe(key));
            } else {
                index.remove(IndexEntry.probe(key));
            }
            if (prev != null) {
                addGarbage(prev.segmentId(), prev.recordBytes());
            }
            deletes++;
            return prev != null;
        }
    }

    /**
     * Visit every record with {@code lo <= key <= hi} in key order. The entry list snapshots
     * under the lock (CSRBT's range walk); values then stream from the log outside it.
     */
    public void range(K lo, K hi, BiConsumer<K, V> consumer) throws IOException {
        List<IndexEntry<K>> entries;
        synchronized (lock) {
            entries = index.rangeQuery(IndexEntry.probe(lo), IndexEntry.probe(hi));
        }
        for (IndexEntry<K> e : entries) {
            RecordCodec.Rec rec = log.read(e.location());
            if (rec == null || rec.isTorn() || rec.tombstone()) {
                throw new IOException("index pointed at an unreadable record at " + e.location());
            }
            consumer.accept(e.key(), readValue(rec.value()));
        }
    }

    /**
     * Compaction (Phase 2): merge every closed segment into one key-ordered segment containing
     * exactly the live records, reclaiming all dead bytes in them. The copy runs outside the
     * store lock (closed segments are immutable); the commit + index repoint run under it, with
     * the repoint skipping any entry the workload updated mid-copy (its fresh copy is counted as
     * garbage immediately). Crash-safe via the marker protocol (see {@link SegmentLog}); any
     * compaction invalidates the hint. Returns the net bytes reclaimed (may be 0).
     */
    public long compact() throws IOException {
        List<IndexEntry<K>> victims = new ArrayList<>();
        int minId;
        int maxId;
        synchronized (lock) {
            List<Integer> closed = log.closedSegmentIds();
            if (closed.isEmpty()) {
                return 0;
            }
            minId = closed.get(0);
            maxId = closed.get(closed.size() - 1);
            for (IndexEntry<K> e : index.inOrder()) {                 // already key-sorted
                if (e.segmentId() <= maxId) {
                    victims.add(e);
                }
            }
        }

        // Copy phase, lock-free: rewrite the live records, key-ordered, into the scratch file.
        List<IndexEntry<K>> replacements = new ArrayList<>(victims.size());
        long newOffset = 0;
        try (FileChannel tmp = log.openCompactionTmp()) {
            for (IndexEntry<K> v : victims) {
                RecordCodec.Rec rec = log.read(v.location());
                if (rec == null || rec.isTorn() || rec.tombstone()) {
                    throw new IOException("compaction could not read live record at " + v.location());
                }
                byte[] encoded = RecordCodec.encode(rec.key(), rec.value(), false);
                ByteBuffer buf = ByteBuffer.wrap(encoded);
                while (buf.hasRemaining()) {
                    tmp.write(buf);
                }
                replacements.add(new IndexEntry<>(v.key(), maxId, newOffset, encoded.length));
                newOffset += encoded.length;
            }
            tmp.force(true);
        }

        synchronized (lock) {
            long beforeBytes = 0;
            for (int id = minId; id <= maxId; id++) {
                beforeBytes += log.segmentSize(id);
            }
            log.commitCompaction(minId, maxId);                       // marker → delete → rename
            for (int id = minId; id <= maxId; id++) {
                garbage.remove(id);                                   // those files are gone
            }
            for (int i = 0; i < victims.size(); i++) {
                IndexEntry<K> victim = victims.get(i);
                IndexEntry<K> current = floorHit(victim.key());
                if (current != null && current.sameLocation(victim)) {
                    index.remove(victim);                             // repoint: same key, new address
                    index.add(replacements.get(i));
                } else {
                    // The workload overwrote/deleted/evicted this key mid-copy: our fresh copy
                    // is dead on arrival — honest garbage in the merged segment.
                    addGarbage(maxId, replacements.get(i).recordBytes());
                }
            }
            return beforeBytes - newOffset;
        }
    }

    /** Number of live keys. */
    public int size() {
        return index.size();
    }

    /** Total dead bytes across all segments — what a {@link #compact()} could reclaim from closed ones. */
    public long garbageBytes() {
        synchronized (lock) {
            long total = 0;
            for (long g : garbage.values()) {
                total += g;
            }
            return total;
        }
    }

    /** A one-line health/shape summary. */
    public String stats() {
        try {
            return "SmokeHouse{keys=" + index.size()
                    + ", segments=" + log.segmentCount()
                    + ", garbageBytes=" + garbageBytes()
                    + ", strategy=" + index.getStrategy().getClass().getSimpleName()
                    + ", tier=" + opts.tier()
                    + (opts.retention() > 0 ? ", retain=" + opts.retention() : "")
                    + ", fsync=" + opts.fsyncPolicy()
                    + ", pilot=\"" + pilotVerdict + "\""
                    + ", puts=" + puts + ", gets=" + gets + ", deletes=" + deletes + "}";
        } catch (IOException e) {
            return "SmokeHouse{stats unavailable: " + e.getMessage() + "}";
        }
    }

    /** The live index — every CSRBT read on it is torn-read-free from any thread. */
    public OrderedSet<IndexEntry<K>> index() {
        return index;
    }

    /** Clean shutdown: force the log, write the hint checkpoint (next open is a warm start), close. Idempotent. */
    @Override
    public void close() throws IOException {
        if (pilot != null) {
            pilot.shutdownNow();
        }
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            log.force();
            try {
                HintFile.write(log.hintPath(), index.inOrder(), log.activeSegmentId(),
                        garbage, keyCodec(opts));
            } catch (IOException hintFailed) {
                // A hint is an optimization; a failed one must never fail the close. Next open
                // simply cold-scans.
            }
            log.close();
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────────────────────

    private void addGarbage(int segmentId, long bytes) {
        garbage.merge(segmentId, bytes, Long::sum);
    }

    private IndexEntry<K> floorHit(K key) {
        IndexEntry<K> f = navigable.floor(IndexEntry.probe(key));
        return (f != null && opts.comparator().compare(f.key(), key) == 0) ? f : null;
    }

    private static <K, V> HintFile.KeyCodec<K> keyCodec(SmokeHouseOptions<K, V> opts) {
        return new HintFile.KeyCodec<>() {
            @Override public byte[] toBytes(K key) throws IOException {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
                try (DataOutputStream out = new DataOutputStream(bytes)) {
                    opts.keySerializer().write(key, out);
                }
                return bytes.toByteArray();
            }
            @Override public K fromBytes(byte[] bytes) throws IOException {
                try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                    return opts.keySerializer().read(in);
                }
            }
        };
    }

    private byte[] keyBytes(K key) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            opts.keySerializer().write(key, out);
        }
        return bytes.toByteArray();
    }

    private byte[] valueBytes(V value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            opts.valueSerializer().write(value, out);
        }
        return bytes.toByteArray();
    }

    private static <K, V> K readKey(SmokeHouseOptions<K, V> opts, byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return opts.keySerializer().read(in);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt key bytes survived CRC — serializer mismatch?", e);
        }
    }

    private V readValue(byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return opts.valueSerializer().read(in);
        }
    }
}
