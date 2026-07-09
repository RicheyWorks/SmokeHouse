package io.github.richeyworks.smokehouse;

import io.github.richeyworks.csrbt.OrderedSet;
import io.github.richeyworks.csrbt.adapter.NavigableOrderedSet;
import io.github.richeyworks.csrbt.control.MorphPolicy;
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
 * only truth; the index is a cache of it, rebuilt on every open (scan → last-writer-wins →
 * <b>SuperBeefSort sort → {@code OrderedSet.fromSorted}</b>, the O(n) zero-rotation build — the
 * feeding pipeline as the restart path). See {@code SuperBeefSort/docs/adr-smokehouse-ecosystem-ring.md}.
 *
 * <p><b>Concurrency:</b> single writer (all mutation under one store lock, which also serializes
 * the internal pilot — CSRBT's control plane is single-threaded by contract); reads resolve the
 * index under the same lock (the workload monitor records them) and then read the log
 * <em>outside</em> it (record bytes at an address never change once written).</p>
 *
 * <p><b>Requirements, honestly:</b> all keys live in memory (the Bitcask trade — this is an
 * embedded store for key-indexed records, not a general database; the hard ceiling is CSRBT's
 * 2³¹-key index). Key {@code equals}/{@code hashCode} must agree with the comparator (standard
 * {@code TreeMap}+{@code HashMap} interop discipline; recovery and skew detection rely on it).
 * Phase 1 has no compaction — deleted/overwritten bytes stay on disk until Phase 2.</p>
 */
public final class SmokeHouse<K, V> implements Closeable {

    private final Object lock = new Object();
    private final SmokeHouseOptions<K, V> opts;
    private final SegmentLog log;
    private final OrderedSet<IndexEntry<K>> index;
    private final NavigableOrderedSet<IndexEntry<K>> navigable;
    private final WorkloadAdaptation<IndexEntry<K>> adaptation;   // null in STATIC tier
    private final ScheduledExecutorService pilot;                 // null in STATIC tier
    private volatile String pilotVerdict = "not yet evaluated";
    private long puts, gets, deletes;

    private SmokeHouse(SmokeHouseOptions<K, V> opts, SegmentLog log, OrderedSet<IndexEntry<K>> index,
                       WorkloadAdaptation<IndexEntry<K>> adaptation) {
        this.opts = opts;
        this.log = log;
        this.index = index;
        this.navigable = new NavigableOrderedSet<>(index);
        this.adaptation = adaptation;
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

    /** Open (or create) a store at {@code dir}, recovering the index from the log. */
    public static <K, V> SmokeHouse<K, V> open(Path dir, SmokeHouseOptions<K, V> opts) throws IOException {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(opts, "opts");
        SegmentLog log = SegmentLog.open(dir, opts.segmentBytesLimit(), opts.fsyncPolicy());

        // ── Recovery: scan → last-writer-wins → SuperBeefSort sort → O(n) index build ──
        Map<K, IndexEntry<K>> live = new HashMap<>();
        log.scan((segId, offset, rec) -> {
            K key = readKey(opts, rec.key());
            if (rec.tombstone()) {
                live.remove(key);
            } else {
                live.put(key, new IndexEntry<>(key, segId, offset));
            }
        });
        Comparator<IndexEntry<K>> ordering = IndexEntry.ordering(opts.comparator());
        List<IndexEntry<K>> entries = new ArrayList<>(live.values());
        if (entries.size() > 1) {
            entries = BeefSort.with(ordering).source(entries).run().sorted();
        }
        OrderedSet<IndexEntry<K>> index =
                OrderedSet.fromSorted(entries, new RedBlackStrategy<>(), ordering);

        WorkloadAdaptation<IndexEntry<K>> adaptation =
                (opts.tier() == SmokeHouseOptions.IndexTier.ADAPTIVE)
                        ? WorkloadAdaptation.attach(index, MorphPolicy.defaults())
                        : null;
        return new SmokeHouse<>(opts, log, index, adaptation);
    }

    // ── Data plane ────────────────────────────────────────────────────────────────────────────

    /** Insert or overwrite. Durability per the {@link Fsync} policy; the log append IS the WAL write. */
    public void put(K key, V value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] record = RecordCodec.encode(keyBytes(key), valueBytes(value), false);
        synchronized (lock) {
            SegmentLog.Location loc = log.append(record);
            IndexEntry<K> probe = IndexEntry.probe(key);
            IndexEntry<K> entry = new IndexEntry<>(key, loc.segmentId(), loc.offset());
            if (adaptation != null) {
                adaptation.remove(probe);              // upsert = remove + add (ADR D2)
                adaptation.add(entry);
            } else {
                index.remove(probe);
                index.add(entry);
            }
            puts++;
        }
    }

    /** The newest value for {@code key}, or {@code null}. Index under the lock; log read outside it. */
    public V get(K key) throws IOException {
        Objects.requireNonNull(key, "key");
        IndexEntry<K> entry;
        synchronized (lock) {
            entry = floorHit(key);
            if (adaptation != null) {
                adaptation.recordSearch(key.hashCode());   // read mix + skew for the scorer
            }
            gets++;
        }
        if (entry == null) {
            return null;
        }
        RecordCodec.Rec rec = log.read(entry.location());
        if (rec == null || rec.isTorn() || rec.tombstone()) {
            throw new IOException("index pointed at an unreadable record for key " + key
                    + " at " + entry.location() + " — log/index divergence");
        }
        return readValue(rec.value());
    }

    /** Delete: a durable tombstone in the log, then the index entry goes. Returns whether the key existed. */
    public boolean delete(K key) throws IOException {
        Objects.requireNonNull(key, "key");
        byte[] record = RecordCodec.encode(keyBytes(key), null, true);
        synchronized (lock) {
            IndexEntry<K> probe = IndexEntry.probe(key);
            boolean existed = floorHit(key) != null;
            log.append(record);                        // tombstone first: truth before cache
            if (adaptation != null) {
                adaptation.remove(probe);
            } else {
                index.remove(probe);
            }
            deletes++;
            return existed;
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

    /** Number of live keys. */
    public int size() {
        return index.size();
    }

    /** A one-line health/shape summary. */
    public String stats() {
        try {
            return "SmokeHouse{keys=" + index.size()
                    + ", segments=" + log.segmentCount()
                    + ", strategy=" + index.getStrategy().getClass().getSimpleName()
                    + ", tier=" + opts.tier()
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

    @Override
    public void close() throws IOException {
        if (pilot != null) {
            pilot.shutdownNow();
        }
        synchronized (lock) {
            log.close();
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────────────────────

    private IndexEntry<K> floorHit(K key) {
        IndexEntry<K> f = navigable.floor(IndexEntry.probe(key));
        return (f != null && opts.comparator().compare(f.key(), key) == 0) ? f : null;
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
