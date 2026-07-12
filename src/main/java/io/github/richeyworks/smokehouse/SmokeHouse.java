package io.github.richeyworks.smokehouse;

import io.github.richeyworks.csrbt.OrderedSet;
import io.github.richeyworks.csrbt.adapter.NavigableOrderedSet;
import io.github.richeyworks.csrbt.control.MorphPolicy;
import io.github.richeyworks.csrbt.ensemble.EnsembleOrderedSet;
import io.github.richeyworks.csrbt.event.TreeEvent;
import io.github.richeyworks.csrbt.strategy.RedBlackStrategy;
import io.github.richeyworks.csrbt.strategy.TreeStrategy;
import io.github.richeyworks.csrbt.util.StrategyHealthCheck;
import io.github.richeyworks.superbeefsort.BeefSort;
import io.github.richeyworks.superbeefsort.core.SortResult;
import io.github.richeyworks.superbeefsort.csrbt.AccessPolicy;
import io.github.richeyworks.superbeefsort.csrbt.EnsembleAdaptation;
import io.github.richeyworks.superbeefsort.csrbt.EnsembleSpec;
import io.github.richeyworks.superbeefsort.csrbt.EnsembleTargetFactory;
import io.github.richeyworks.superbeefsort.csrbt.EvolutionAdaptation;
import io.github.richeyworks.superbeefsort.csrbt.StrategyAdvisor;
import io.github.richeyworks.superbeefsort.csrbt.WorkloadAdaptation;
import io.github.richeyworks.superbeefsort.engine.SortRunResult;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;
import io.github.richeyworks.superbeefsort.profile.DataProfile;
import io.github.richeyworks.superbeefsort.source.RecordSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.NoSuchElementException;
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
 * <p><b>Phase 3 surfaces — CSRBT at full extent:</b></p>
 * <ul>
 *   <li><b>Born optimal:</b> recovery no longer hardcodes Red-Black. The
 *       {@link SmokeHouseOptions#accessPolicy declared access pattern} + the recovery sort's
 *       {@link DataProfile} pick the index's balancing strategy via SuperBeefSort's
 *       {@code StrategyAdvisor}; non-Red-Black bulk builds are health-gated
 *       ({@link StrategyHealthCheck}) with a Red-Black fallback, mirroring CSRBT's morph ethos.</li>
 *   <li><b>Profile-guided adaptation:</b> the ADAPTIVE tier hands the sort's profile and realized
 *       run metrics to {@code WorkloadAdaptation.attachProfileGuided} — the recovery sort primes
 *       the tree's control plane instead of being thrown away — and the recovery feed itself is
 *       folded into the workload monitor ({@code recordFeed}), so the first pilot cycle never
 *       evaluates an empty workload. Range reads are observed too, and every 64th lookup measures
 *       its <em>realized</em> search depth.</li>
 *   <li><b>{@link SmokeHouseOptions.IndexTier#ENSEMBLE ENSEMBLE} tier:</b> the index is CSRBT's
 *       mirrored member trio and the pilot promotes the read path (O(1) swap) and runs the
 *       failover/quarantine/heal health cadence.</li>
 *   <li><b>{@link SmokeHouseOptions.IndexTier#EVOLUTION EVOLUTION} tier:</b> CSRBT's evolution
 *       machine as an index — a laboratory member trials parameterized balancing policies
 *       (UCB1 bandit) against live traffic, with health-gate deaths and policy-gated
 *       promotions on the record in {@link #stats()}.</li>
 *   <li><b>Order statistics:</b> the index maintains subtree sizes intrinsically, so
 *       {@link #countRange}, {@link #nthKey}, {@link #rankOf}, {@link #medianKey},
 *       {@link #percentileKey}, {@link #firstKey} and {@link #lastKey} are all O(log n) — free
 *       power the log itself could never answer without a scan.</li>
 * </ul>
 *
 * <p><b>Concurrency:</b> single writer (all mutation, the pilot, and compaction's commit under
 * one store lock); reads resolve the index under the lock and read the log outside it.
 * <b>Bounds, honestly:</b> all keys in memory (the Bitcask trade); key {@code equals} must agree
 * with the comparator; after an index morph the retention window's FIFO order falls back to
 * ascending until writes re-establish it (CSRBT's documented safety net).</p>
 */
public final class SmokeHouse<K, V> implements Closeable {

    /** Prime the monitor with at most this many trailing feed keys — the rolling window's own size. */
    private static final int FEED_PRIME_CAP = 4096;
    /** Every Nth {@link #get} pays one extra O(log n) walk to record the realized search depth. */
    private static final int DEPTH_SAMPLE_STRIDE = 64;
    /** Bound on per-range-query monitor updates (bounds the lock hold; the window saturates anyway). */
    private static final int RANGE_OBSERVE_CAP = 1024;
    /** Every Nth pilot cycle in the ENSEMBLE tier also runs the failover/quarantine/heal cadence. */
    private static final int HEALTH_EVERY = 4;

    /** Laboratory members the EVOLUTION tier hosts (the bandit trials on the first). */
    private static final int EVOLUTION_SLOTS = 1;

    /** Recent committed mutations the tail keeps for late subscribers to replay. */
    private static final int TAIL_RING_CAPACITY = 1 << 12;

    private final Object lock = new Object();
    private final SmokeHouseOptions<K, V> opts;
    private final SegmentLog log;
    private final OrderedSet<IndexEntry<K>> index;                 // single-tree tiers; null in ENSEMBLE
    private final NavigableOrderedSet<IndexEntry<K>> navigable;    // null in ENSEMBLE
    private final WorkloadAdaptation<IndexEntry<K>> adaptation;    // ADAPTIVE tier only
    private final EnsembleOrderedSet<IndexEntry<K>> ensemble;      // ENSEMBLE tier only
    private final EnsembleAdaptation<IndexEntry<K>> promotion;     // ENSEMBLE tier only
    private final EvolutionAdaptation<IndexEntry<K>> evolution;    // EVOLUTION tier only
    private final ScheduledExecutorService pilot;                  // null in STATIC tier
    private final Map<Integer, Long> garbage = new HashMap<>();    // segmentId -> dead bytes; store-lock guarded
    private final Tail<K, V> tailStream = new Tail<>(TAIL_RING_CAPACITY);   // Phase 7: committed-mutation stream
    private volatile String pilotVerdict = "not yet evaluated";
    private volatile boolean compacting;                           // single-compaction guard: serializes compact()
    private long puts, gets, deletes;
    private int pilotCycles;
    private boolean evolutionCycleOpen;                            // pilot-only (store lock)
    private boolean closed;

    private SmokeHouse(SmokeHouseOptions<K, V> opts, SegmentLog log, OrderedSet<IndexEntry<K>> index,
                       WorkloadAdaptation<IndexEntry<K>> adaptation,
                       EnsembleOrderedSet<IndexEntry<K>> ensemble,
                       EnsembleAdaptation<IndexEntry<K>> promotion,
                       EvolutionAdaptation<IndexEntry<K>> evolution,
                       Map<Integer, Long> recoveredGarbage) {
        this.opts = opts;
        this.log = log;
        this.index = index;
        this.navigable = (index != null) ? new NavigableOrderedSet<>(index) : null;
        this.adaptation = adaptation;
        this.ensemble = ensemble;
        this.promotion = promotion;
        this.evolution = evolution;
        this.garbage.putAll(recoveredGarbage);

        if (opts.retention() > 0) {
            // Retention tier: CSRBT's FIFO window IS the mechanism. Upserts re-enter at the tail
            // (put = remove+add), so eviction order is newest-write-wins. Evicted entries fund
            // the garbage ledger via the event seam — no tombstones needed (see class javadoc).
            // Single-tree tiers only: open() rejects ENSEMBLE + retention (no Evict events there).
            index.setEventListener(e -> {
                if (e instanceof TreeEvent.Evict<IndexEntry<K>> ev) {
                    addGarbage(ev.key().segmentId(), ev.key().recordBytes());
                }
            });
            index.setMaxSize(opts.retention());
        }
        if (adaptation != null || promotion != null || evolution != null) {
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
                    if (adaptation != null) {
                        var r = adaptation.maybeAdapt();
                        pilotVerdict = r.morphed() ? "morph " + r.from() + " -> " + r.to()
                                                   : "hold (" + r.reason() + ")";
                    } else if (promotion != null) {
                        var p = promotion.maybePromote();
                        String verdict = p.promoted() ? "promote " + p.from() + " -> " + p.to()
                                                      : "hold (" + p.reason() + ")";
                        if (++pilotCycles % HEALTH_EVERY == 0) {
                            var h = promotion.checkHealth();
                            if (h.failedOver()) {
                                verdict += "; failover " + h.from() + " -> " + h.to();
                            }
                        }
                        pilotVerdict = verdict;
                    } else {
                        pilotVerdict = evolutionCycle();
                    }
                }
                maybeAutoCompact();   // outside the lock: compaction's copy phase must not hold it
            }, ms, ms, TimeUnit.MILLISECONDS);
        } else {
            this.pilot = null;
        }
    }

    /**
     * One EVOLUTION pilot tick: alternate open/close so every trial scores a real traffic window
     * (open the arm, let ops flow until the next tick, then judge). All throws are folded into
     * the verdict — an uncaught exception would silently cancel the scheduled pilot task.
     */
    private String evolutionCycle() {
        try {
            if (!evolutionCycleOpen) {
                var arms = evolution.beginCycle();
                evolutionCycleOpen = true;
                return "trial open: " + arms;
            }
            var r = evolution.endCycle();
            evolutionCycleOpen = false;
            return r.promoted()
                    ? String.format("promote genome (cost %.4f beats %.4f)",
                            r.challengerCost(), r.incumbentCost())
                    : "hold (" + r.reason() + ")";
        } catch (RuntimeException halted) {
            evolutionCycleOpen = false;
            return "evolution halted (" + halted.getMessage() + ")";
        }
    }

    /**
     * Auto-compaction (Phase 4.3), ridden by the pilot: if the closed segments' garbage ratio
     * exceeds {@link SmokeHouseOptions#compactWhenGarbageAbove}, run {@link #compact()} right
     * here on the pilot thread — its copy phase already runs outside the store lock, so the
     * pilot doing it is safe. Re-entry guarded; failures land in the verdict, never propagate.
     */
    private void maybeAutoCompact() {
        double threshold = opts.compactAbove();
        if (threshold <= 0.0 || compacting) {
            return;
        }
        synchronized (lock) {
            if (closed) {
                return;
            }
            try {
                List<Integer> closedIds = log.closedSegmentIds();
                if (closedIds.isEmpty()) {
                    return;
                }
                long total = 0;
                long dead = 0;
                for (int id : closedIds) {
                    total += log.segmentSize(id);
                    dead += garbage.getOrDefault(id, 0L);
                }
                if (total <= 0 || (double) dead / total <= threshold) {
                    return;
                }
            } catch (IOException unreadable) {
                pilotVerdict = pilotVerdict + "; auto-compaction skipped (" + unreadable.getMessage() + ")";
                return;
            }
        }
        try {
            long reclaimed = compact();                              // compact() self-guards against concurrency
            pilotVerdict = pilotVerdict + "; auto-compacted " + reclaimed + " bytes";
        } catch (IOException failed) {
            pilotVerdict = pilotVerdict + "; auto-compaction failed (" + failed.getMessage() + ")";
        }
    }

    /** Open (or create) a store at {@code dir}, recovering the index from the log (hint-accelerated). */
    public static <K, V> SmokeHouse<K, V> open(Path dir, SmokeHouseOptions<K, V> opts) throws IOException {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(opts, "opts");
        boolean ensembleBacked = opts.tier() == SmokeHouseOptions.IndexTier.ENSEMBLE
                || opts.tier() == SmokeHouseOptions.IndexTier.EVOLUTION;
        if (ensembleBacked && opts.retention() > 0) {
            throw new IllegalArgumentException("retainNewest requires a single-tree index tier: the "
                    + "ensemble surfaces no per-member Evict events, so the retention garbage ledger "
                    + "cannot be funded. Use STATIC or ADAPTIVE with retention, or drop retention.");
        }
        SegmentLog log = SegmentLog.open(dir, opts.segmentBytesLimit(), opts.fsyncPolicy(),
                opts.fsyncInterval());

        // ── Recovery: (hint checkpoint +) delta scan → last-writer-wins → sort → O(n) build ──
        HintFile.KeyCodec<K> codec = keyCodec(opts);
        HintFile.Hint<K> hint = HintFile.load(log.hintPath(), codec, opts.comparator(), log);
        // TreeMap, not HashMap: the warm-start-no-delta path below skips the SuperBeefSort re-sort
        // and builds the index straight from live.values(), so that iteration MUST be key-sorted.
        // HashMap order is only accidentally ascending below 2^16 keys — the h ^ (h >>> 16) spread
        // perturbs bucket order above it, so fromSorted saw an unsorted list and threw.
        Map<K, IndexEntry<K>> live = new TreeMap<>(opts.comparator());
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

        // The recovery sort is not just a sort: it is SuperBeefSort MEASURING the data. Keep what
        // it learned — the DataProfile and the realized run metrics prime the control plane below
        // (the "two engines talking" seam of the ecosystem ADR). Warm starts with no delta skip
        // the sort and hence carry no profile; adaptation then attaches unprimed, as before.
        DataProfile profile = null;
        SortResult sortMetrics = null;
        if ((hint == null || delta[0]) && entries.size() > 1) {
            SortRunResult<IndexEntry<K>> run = BeefSort.with(ordering).source(entries).run();
            entries = run.sorted();
            profile = run.profile();
            sortMetrics = run.sortMetrics();
        }

        if (opts.tier() == SmokeHouseOptions.IndexTier.ENSEMBLE) {
            // The mirrored morph-family trio (RB + AVL + Splay): adaptation is an O(1) primary
            // promotion instead of an O(n) morph. Bulk-loaded per member in O(n) each.
            EnsembleOrderedSet<IndexEntry<K>> ens = EnsembleTargetFactory.forProfile(
                    profile, opts.access(), ordering, EnsembleSpec.adaptive());
            ens.buildAllFromSorted(entries);
            EnsembleAdaptation<IndexEntry<K>> promotion = (profile != null)
                    ? EnsembleAdaptation.attachProfileGuided(ens, profile, opts.access(),
                            sortMetrics, MorphPolicy.defaults())
                    : EnsembleAdaptation.attach(ens, MorphPolicy.defaults());
            promotion.recordFeed(feedTail(entries));
            return new SmokeHouse<>(opts, log, null, null, ens, promotion, null, recoveredGarbage);
        }

        if (opts.tier() == SmokeHouseOptions.IndexTier.EVOLUTION) {
            // The evolution machine (CSRBT ADR-011): the access-advised primary holds the throne
            // while a laboratory member trials parameterized policies against live traffic —
            // births, deaths, and promotions on the record. Observability tier by CSRBT's own
            // pre-registered verdict; the performance story stays with ADAPTIVE/ENSEMBLE.
            EnsembleOrderedSet<IndexEntry<K>> host = EnsembleTargetFactory.evolutionHost(
                    profile, opts.access(), ordering, EVOLUTION_SLOTS, false);
            host.buildAllFromSorted(entries);
            EvolutionAdaptation<IndexEntry<K>> evolution =
                    EvolutionAdaptation.banditSearch(host, MorphPolicy.defaults());
            evolution.recordFeed(feedTail(entries));
            return new SmokeHouse<>(opts, log, null, null, host, null, evolution, recoveredGarbage);
        }

        // Born optimal (single-tree tiers): the declared access pattern + the measured profile
        // pick the strategy the index is BUILT with, so the control plane starts from the shape
        // it would otherwise have to morph toward — construction beats correction.
        TreeStrategy<IndexEntry<K>> born = bornStrategy(opts, profile);
        OrderedSet<IndexEntry<K>> index = buildHealthGated(entries, born, ordering);
        WorkloadAdaptation<IndexEntry<K>> adaptation = null;
        if (opts.tier() == SmokeHouseOptions.IndexTier.ADAPTIVE) {
            adaptation = (profile != null)
                    ? WorkloadAdaptation.attachProfileGuided(index, profile, opts.access(),
                            sortMetrics, MorphPolicy.defaults())
                    : WorkloadAdaptation.attach(index, MorphPolicy.defaults());
            adaptation.recordFeed(feedTail(entries));   // the feed WAS the recent workload
        }
        return new SmokeHouse<>(opts, log, index, adaptation, null, null, null, recoveredGarbage);
    }

    /**
     * The strategy the index is born with: SuperBeefSort's {@code StrategyAdvisor} over the
     * declared access pattern + the recovery sort's profile. One clamp (documented on
     * {@link SmokeHouseOptions#accessPolicy}): WRITE_HEAVY advises WeightBalanced, which has no
     * {@code StrategyId} and cannot be morph-managed — in the ADAPTIVE tier it becomes Red-Black,
     * the morph family's rotation-thrifty member.
     */
    private static <K> TreeStrategy<IndexEntry<K>> bornStrategy(SmokeHouseOptions<K, ?> opts,
                                                                DataProfile profile) {
        if (opts.tier() == SmokeHouseOptions.IndexTier.ADAPTIVE
                && opts.access() == AccessPolicy.WRITE_HEAVY) {
            return new RedBlackStrategy<>();
        }
        return StrategyAdvisor.advise(profile, opts.access());
    }

    /**
     * O(n) bulk build under CSRBT's own morph ethos: a non-Red-Black build must pass
     * {@link StrategyHealthCheck} before it serves reads; a failure falls back to the
     * always-valid Red-Black build rather than publishing a tree in a dubious shape.
     */
    private static <K> OrderedSet<IndexEntry<K>> buildHealthGated(List<IndexEntry<K>> entries,
            TreeStrategy<IndexEntry<K>> born, Comparator<IndexEntry<K>> ordering) {
        OrderedSet<IndexEntry<K>> set = OrderedSet.fromSorted(entries, born, ordering);
        if (!(born instanceof RedBlackStrategy)
                && !StrategyHealthCheck.validate(set.getEngine(), set.getStrategy(), entries).isEmpty()) {
            return OrderedSet.fromSorted(entries, new RedBlackStrategy<>(), ordering);
        }
        return set;
    }

    /** The trailing slice of the feed that still fits the monitor's rolling window. */
    private static <T> List<T> feedTail(List<T> entries) {
        int n = entries.size();
        return entries.subList(Math.max(0, n - FEED_PRIME_CAP), n);
    }

    /**
     * Import a whole {@link RecordSource} into a <b>fresh</b> store at {@code dir} — <em>ingestion
     * as recovery</em> (ADR Phase 3). Instead of {@link #put}-ing each record (two O(log n) index
     * ops apiece), this appends every record straight to a bare {@link SegmentLog} with <b>no index
     * maintenance at all</b> — that is the whole trick — then returns
     * {@link #open(Path, SmokeHouseOptions)}, whose recovery already does the elegant part: scan the
     * log, resolve duplicate keys <em>last-writer-wins</em> (a later source record overwrites an
     * earlier one with the same key), sort the survivors with SuperBeefSort, and build the index in
     * O(n) via {@code OrderedSet.fromSorted}. An import is thus a pre-compacted store.
     *
     * <p>v1 targets an empty directory: if {@code dir} already contains segments this fails loudly
     * (importing into a populated store would mean upsert-against-existing, which is what
     * {@link #put} is for). The {@code source} is consumed once and closed. The bulk append's
     * durability follows {@code opts}' {@link Fsync} policy; the log is forced before recovery
     * regardless.
     *
     * @throws IllegalStateException if {@code dir} already contains segment files
     */
    public static <K, V> SmokeHouse<K, V> importInto(Path dir, SmokeHouseOptions<K, V> opts,
                                                     RecordSource<K, V> source) throws IOException {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(opts, "opts");
        Objects.requireNonNull(source, "source");
        try (source) {                                   // closed on every path, incl. the guard throw
            if (SegmentLog.hasSegments(dir)) {
                throw new IllegalStateException("importInto targets a fresh store, but " + dir
                        + " already contains segments — open it and put(...) instead, or import into "
                        + "an empty directory.");
            }
            SegmentLog log = SegmentLog.open(dir, opts.segmentBytesLimit(), opts.fsyncPolicy(),
                    opts.fsyncInterval());
            try {
                RecordSource.Record<K, V> rec;
                while ((rec = source.next()) != null) {
                    byte[] key = encodeField(opts.keySerializer(),
                            Objects.requireNonNull(rec.key(), "record key"));
                    byte[] value = encodeField(opts.valueSerializer(),
                            Objects.requireNonNull(rec.value(), "record value"));
                    log.append(RecordCodec.encode(key, value, false));   // append only — no index
                }
                log.force();
            } finally {
                log.close();
            }
        }
        return open(dir, opts);
    }

    private static <T> byte[] encodeField(SpillSerializer<T> serializer, T value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            serializer.write(value, out);
        }
        return bytes.toByteArray();
    }

    // ── Data plane ────────────────────────────────────────────────────────────────────────────

    /** Insert or overwrite. Durability per the {@link Fsync} policy; the log append IS the WAL write. */
    public void put(K key, V value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] record = RecordCodec.encode(keyBytes(key), valueBytes(value), false);
        synchronized (lock) {
            IndexEntry<K> prev = liveEntry(key);
            SegmentLog.Location loc = log.append(record);            // truth before cache
            IndexEntry<K> entry = new IndexEntry<>(key, loc.segmentId(), loc.offset(), record.length);
            if (evolution != null) {
                evolution.remove(IndexEntry.probe(key));              // upsert = remove + add (ADR D2)
                evolution.add(entry);
            } else if (promotion != null) {
                promotion.remove(IndexEntry.probe(key));
                promotion.add(entry);
            } else if (adaptation != null) {
                adaptation.remove(IndexEntry.probe(key));
                adaptation.add(entry);
            } else {
                index.remove(IndexEntry.probe(key));
                index.add(entry);
            }
            if (prev != null) {
                addGarbage(prev.segmentId(), prev.recordBytes());     // the old version just died
            }
            puts++;
            tailStream.publish(key, value, false, loc.segmentId(), loc.offset());
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
                entry = liveEntry(key);
                observeGet(key);                                      // read mix + skew (+ sampled depth)
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

    /**
     * Feed the lookup into the workload monitor. Most reads record hash + depth-0 (the walk
     * already happened in {@link #liveEntry}); every {@link #DEPTH_SAMPLE_STRIDE}-th read pays
     * one extra O(log n) walk to record the REALIZED search depth — CSRBT's primary signal for
     * how good the current tree shape is, sampled so honesty stays cheap.
     */
    private void observeGet(K key) {
        if (adaptation == null && promotion == null && evolution == null) {
            return;
        }
        int hash = key.hashCode();
        int depth = 0;
        if (gets % DEPTH_SAMPLE_STRIDE == 0) {
            int d = (ensemble != null) ? ensemble.searchDepth(IndexEntry.probe(key))
                                       : index.searchDepth(IndexEntry.probe(key));
            depth = (d >= 0) ? d : ~d;
        }
        if (adaptation != null) {
            adaptation.recordSearch(hash, depth);
        } else if (promotion != null) {
            promotion.recordSearch(hash, depth);
        } else {
            evolution.recordSearch(hash, depth);
        }
    }

    /** Delete: a durable tombstone in the log, then the index entry goes. Returns whether the key existed. */
    public boolean delete(K key) throws IOException {
        Objects.requireNonNull(key, "key");
        byte[] record = RecordCodec.encode(keyBytes(key), null, true);
        synchronized (lock) {
            IndexEntry<K> prev = liveEntry(key);
            SegmentLog.Location loc = log.append(record);             // truth before cache
            addGarbage(loc.segmentId(), record.length);               // a tombstone is born dead weight
            if (evolution != null) {
                evolution.remove(IndexEntry.probe(key));
            } else if (promotion != null) {
                promotion.remove(IndexEntry.probe(key));
            } else if (adaptation != null) {
                adaptation.remove(IndexEntry.probe(key));
            } else {
                index.remove(IndexEntry.probe(key));
            }
            if (prev != null) {
                addGarbage(prev.segmentId(), prev.recordBytes());
            }
            deletes++;
            tailStream.publish(key, null, true, loc.segmentId(), loc.offset());
            return prev != null;
        }
    }

    /**
     * Visit every record with {@code lo <= key <= hi} in key order. The entry list snapshots
     * under the lock (CSRBT's range walk); values then stream from the log outside it. Range
     * traffic is folded into the workload monitor (capped per call), so range-heavy workloads
     * are visible to the pilot instead of invisible.
     */
    public void range(K lo, K hi, BiConsumer<K, V> consumer) throws IOException {
        List<IndexEntry<K>> entries;
        synchronized (lock) {
            entries = indexRange(IndexEntry.probe(lo), IndexEntry.probe(hi));
            observeRange(entries);
        }
        for (IndexEntry<K> e : entries) {
            RecordCodec.Rec rec = log.read(e.location());
            if (rec == null || rec.isTorn() || rec.tombstone()) {
                throw new IOException("index pointed at an unreadable record at " + e.location());
            }
            consumer.accept(e.key(), readValue(rec.value()));
        }
    }

    /** Each ranged key was a read; tell the monitor so (bounded — the rolling window saturates anyway). */
    private void observeRange(List<IndexEntry<K>> hits) {
        if (adaptation == null && promotion == null && evolution == null) {
            return;
        }
        int n = Math.min(hits.size(), RANGE_OBSERVE_CAP);
        for (int i = 0; i < n; i++) {
            int hash = Objects.hashCode(hits.get(i).key());
            if (adaptation != null) {
                adaptation.recordSearch(hash);
            } else if (promotion != null) {
                promotion.recordSearch(hash, 0);
            } else {
                evolution.recordSearch(hash, 0);
            }
        }
    }

    // ── Order statistics (CSRBT's RankedSet face, O(log n) each — no log I/O involved) ─────────

    /** Number of live keys with {@code lo <= key <= hi} — two rank walks, no scan. */
    public int countRange(K lo, K hi) {
        synchronized (lock) {
            return (ensemble != null)
                    ? ensemble.countInRange(IndexEntry.probe(lo), IndexEntry.probe(hi))
                    : index.countInRange(IndexEntry.probe(lo), IndexEntry.probe(hi));
        }
    }

    /**
     * The {@code rank}-th smallest live key, <b>1-indexed</b> (1 = minimum, {@link #size()} =
     * maximum) — CLRS OS-SELECT.
     *
     * @throws IndexOutOfBoundsException if {@code rank < 1} or {@code rank > size()}
     */
    public K nthKey(int rank) {
        synchronized (lock) {
            IndexEntry<K> e = (ensemble != null) ? ensemble.select(rank) : index.select(rank);
            return (e == null) ? null : e.key();
        }
    }

    /** The <b>1-indexed</b> rank of {@code key} among live keys, or {@code 0} if absent — CLRS OS-RANK. */
    public int rankOf(K key) {
        synchronized (lock) {
            try {
                return (ensemble != null) ? ensemble.rank(IndexEntry.probe(key))
                                          : index.rank(IndexEntry.probe(key));
            } catch (NoSuchElementException absent) {
                return 0;
            }
        }
    }

    /** The lower-median live key, or {@code null} if the store is empty. */
    public K medianKey() {
        synchronized (lock) {
            IndexEntry<K> e = (ensemble != null) ? ensemble.median() : index.median();
            return (e == null) ? null : e.key();
        }
    }

    /** The {@code pct}-th percentile live key (0–100; 50 = median), or {@code null} if empty. */
    public K percentileKey(int pct) {
        synchronized (lock) {
            IndexEntry<K> e = (ensemble != null) ? ensemble.percentile(pct) : index.percentile(pct);
            return (e == null) ? null : e.key();
        }
    }

    /** The smallest live key, or {@code null} if the store is empty. */
    public K firstKey() {
        synchronized (lock) {
            IndexEntry<K> e = (ensemble != null) ? ensemble.minimum() : index.minimum();
            return (e == null) ? null : e.key();
        }
    }

    /** The largest live key, or {@code null} if the store is empty. */
    public K lastKey() {
        synchronized (lock) {
            IndexEntry<K> e = (ensemble != null) ? ensemble.maximum() : index.maximum();
            return (e == null) ? null : e.key();
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
            if (compacting) {
                return 0;                                            // a compaction is already in flight
            }
            List<Integer> closed = log.closedSegmentIds();
            if (closed.isEmpty()) {
                return 0;
            }
            minId = closed.get(0);
            maxId = closed.get(closed.size() - 1);
            for (IndexEntry<K> e : indexInOrder()) {                  // already key-sorted
                if (e.segmentId() <= maxId) {
                    victims.add(e);
                }
            }
            compacting = true;                                       // serialize compactions across the off-lock copy
        }
        try {
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
                    IndexEntry<K> current = liveEntry(victim.key());
                    if (current != null && current.sameLocation(victim)) {
                        // Repoint: same key, new address. Deliberately NOT recorded in the workload
                        // monitor — compaction is maintenance, not traffic; recording it would skew
                        // the pilot's read/write mix with our own housekeeping.
                        maintenanceRemove(victim);
                        maintenanceAdd(replacements.get(i));
                    } else {
                        // The workload overwrote/deleted/evicted this key mid-copy: our fresh copy
                        // is dead on arrival — honest garbage in the merged segment.
                        addGarbage(maxId, replacements.get(i).recordBytes());
                    }
                }
                return beforeBytes - newOffset;
            }
        } finally {
            compacting = false;
        }
    }

    /** Number of live keys. */
    public int size() {
        return (ensemble != null) ? ensemble.size() : index.size();
    }

    /** One segment's live picture: total bytes on disk, dead bytes in the ledger, and whether it's the active tail. */
    public record SegmentStat(int segmentId, long bytes, long garbageBytes, boolean active) { }

    /**
     * The per-segment map (Phase 4.4 observability): every closed segment plus the active tail,
     * each with its size and its share of the garbage ledger — the raw material of the
     * dashboard's segment bars, and of any external monitoring.
     */
    public List<SegmentStat> segmentStats() throws IOException {
        synchronized (lock) {
            List<SegmentStat> out = new ArrayList<>();
            for (int id : log.closedSegmentIds()) {
                out.add(new SegmentStat(id, log.segmentSize(id), garbage.getOrDefault(id, 0L), false));
            }
            int active = log.activeSegmentId();
            out.add(new SegmentStat(active, log.segmentSize(active), garbage.getOrDefault(active, 0L), true));
            return out;
        }
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

    /**
     * Snapshot the current segment set into a fresh, generation-numbered {@link ManifestFile}
     * (Phase 6 — advisory: backup and replication consume it; recovery never does). Runs under the
     * store lock so the named set and its byte lengths are a consistent instant, and each CRC covers
     * an immutable prefix. The next generation is one past the highest manifest already present, so
     * numbering is monotonic even across a corrupt one. Returns the generation written.
     *
     * <p>Cost note: this CRCs every live segment under the lock. That is fine for occasional
     * checkpoints; backup (Phase 6) rolls the active segment first and copies off-lock.</p>
     */
    public long writeManifest() throws IOException {
        synchronized (lock) {
            Path dir = log.directory();
            List<ManifestFile.SegmentEntry> entries = new ArrayList<>();
            for (int id : log.segmentIds()) {
                long size = log.segmentSize(id);
                entries.add(new ManifestFile.SegmentEntry(id, size, ManifestFile.crcOf(log.segmentPath(id), size)));
            }
            long generation = ManifestFile.nextGeneration(dir);
            ManifestFile.write(dir, new ManifestFile.Manifest(generation, entries));
            return generation;
        }
    }

    /**
     * Back the store up into {@code targetDir}: a self-contained, restorable copy of the current
     * state. Runs under the store lock, so the segment set and its byte lengths are one consistent
     * instant and no compaction can reclaim a segment mid-copy — it forces the log durable, copies
     * each segment's immutable prefix (CRCing as it goes), and writes the same manifest generation
     * into both the source (the point-in-time record) and the target (so the backup is
     * self-describing and {@link ManifestFile#verify verifiable}). Mutations after this call cannot
     * leak into the copy: only bytes already written are captured.
     *
     * <p>Restore is just {@link #restore}/{@link #open(Path, SmokeHouseOptions) open} on
     * {@code targetDir} — recovery rebuilds everything from the segments, so a backup is only
     * recovery's input, relocated.</p>
     *
     * <p>Phase 6 copies under the lock for correctness; the off-lock copy the ADR envisions needs
     * Phase 7's snapshot pinning to keep compaction from reclaiming a segment out from under it.</p>
     *
     * @return the manifest generation written
     */
    public long backup(Path targetDir) throws IOException {
        synchronized (lock) {
            log.force();
            Path sourceDir = log.directory();
            Files.createDirectories(targetDir);
            long generation = ManifestFile.nextGeneration(sourceDir);
            List<ManifestFile.SegmentEntry> entries = new ArrayList<>();
            for (int id : log.segmentIds()) {
                long len = log.segmentSize(id);
                long crc = ManifestFile.copyPrefix(log.segmentPath(id),
                        targetDir.resolve(SegmentLog.segmentName(id)), len);
                entries.add(new ManifestFile.SegmentEntry(id, len, crc));
            }
            ManifestFile.Manifest m = new ManifestFile.Manifest(generation, entries);
            ManifestFile.write(sourceDir, m);
            ManifestFile.write(targetDir, m);
            return generation;
        }
    }

    /**
     * Restore a {@link #backup(Path)} — literally {@link #open(Path, SmokeHouseOptions) open} on the
     * backup directory, named for the round-trip's sake. Recovery does all the work.
     */
    public static <K, V> SmokeHouse<K, V> restore(Path backupDir, SmokeHouseOptions<K, V> opts)
            throws IOException {
        return open(backupDir, opts);
    }

    /**
     * Subscribe to the tail (Phase 7): an ordered, gap-free stream of committed mutations from
     * {@code fromSequence} onward. History still in the ring replays first, then live events stream
     * as they commit — delivered on a tail thread, off the store lock, so a slow listener never
     * stalls the writer (it drops oldest and is told via {@link TailListener#onGap()}). Close the
     * returned handle to unsubscribe.
     */
    public AutoCloseable tail(long fromSequence, TailListener<K, V> listener) {
        Objects.requireNonNull(listener, "listener");
        return tailStream.subscribe(fromSequence, listener);
    }

    /** The next sequence the tail will assign — the number of mutations committed since this open. */
    public long tailSequence() {
        return tailStream.nextSequence();
    }

    /**
     * Watch a single key (Phase 7): the returned handle streams every committed mutation of
     * {@code key} — each put and the delete — as {@link TailEvent}s from now on, delivered off the
     * store lock. It is a filtered view of the {@link #tail}; close the handle to stop. Future
     * changes only — call {@link #get} first if you need the current value.
     */
    public AutoCloseable watch(K key, TailListener<K, V> listener) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(listener, "listener");
        var cmp = opts.comparator();
        return tailStream.subscribe(tailStream.nextSequence(), new TailListener<K, V>() {
            @Override
            public void onEvent(TailEvent<K, V> event) {
                if (cmp.compare(key, event.key()) == 0) {
                    listener.onEvent(event);
                }
            }

            @Override
            public void onGap() {
                listener.onGap();
            }
        });
    }

    /**
     * Watch a key range {@code [lo, hi]} (both inclusive, by the store's comparator): every
     * committed mutation of an in-range key streams to {@code listener} from now on, off the store
     * lock. A filtered view of the {@link #tail}; close the handle to stop.
     */
    public AutoCloseable watchRange(K lo, K hi, TailListener<K, V> listener) {
        Objects.requireNonNull(lo, "lo");
        Objects.requireNonNull(hi, "hi");
        Objects.requireNonNull(listener, "listener");
        var cmp = opts.comparator();
        return tailStream.subscribe(tailStream.nextSequence(), new TailListener<K, V>() {
            @Override
            public void onEvent(TailEvent<K, V> event) {
                if (cmp.compare(lo, event.key()) <= 0 && cmp.compare(event.key(), hi) <= 0) {
                    listener.onEvent(event);
                }
            }

            @Override
            public void onGap() {
                listener.onGap();
            }
        });
    }

    /** A one-line health/shape summary, including the control plane's own report. */
    public String stats() {
        try {
            synchronized (lock) {
                String shape = (ensemble != null)
                        ? "ensemble(primary=" + (promotion != null
                                ? String.valueOf(promotion.currentPrimary())
                                : ensemble.primary().strategyName()) + ")"
                        : index.getStrategy().getClass().getSimpleName();
                return "SmokeHouse{keys=" + size()
                        + ", segments=" + log.segmentCount()
                        + ", garbageBytes=" + garbageBytes()
                        + ", strategy=" + shape
                        + ", tier=" + opts.tier()
                        + ", access=" + opts.access()
                        + (opts.retention() > 0 ? ", retain=" + opts.retention() : "")
                        + ", fsync=" + opts.fsyncPolicy()
                        + ", pilot=\"" + pilotVerdict + "\""
                        + (adaptation != null
                                ? ", adaptation=" + adaptation.adaptationReport()
                                        + ", rotations=" + index.rotationCount()
                                : "")
                        + (promotion != null ? ", promotion=" + promotion.report() : "")
                        + (evolution != null ? ", evolution=" + evolution.mode()
                                + "[cycles=" + evolution.cycles()
                                + ", promotions=" + evolution.promotions() + "]" : "")
                        + ", puts=" + puts + ", gets=" + gets + ", deletes=" + deletes + "}";
            }
        } catch (IOException e) {
            return "SmokeHouse{stats unavailable: " + e.getMessage() + "}";
        }
    }

    /**
     * The live single-tree index — every CSRBT read on it is torn-read-free from any thread.
     *
     * @throws IllegalStateException in the ensemble-backed tiers (use {@link #ensembleIndex()})
     */
    public OrderedSet<IndexEntry<K>> index() {
        if (index == null) {
            throw new IllegalStateException(
                    "this store runs an ensemble-backed tier — use ensembleIndex()");
        }
        return index;
    }

    /** The live ensemble index, or {@code null} unless this store runs an ensemble-backed tier. */
    public EnsembleOrderedSet<IndexEntry<K>> ensembleIndex() {
        return ensemble;
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
            tailStream.close();
            log.force();
            try {
                HintFile.write(log.hintPath(), indexInOrder(), log.activeSegmentId(),
                        garbage, keyCodec(opts));
            } catch (IOException hintFailed) {
                // A hint is an optimization; a failed one must never fail the close. Next open
                // simply cold-scans.
            }
            log.close();
            if (ensemble != null) {
                ensemble.close();                                     // member executor shutdown
            }
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────────────────────

    private void addGarbage(int segmentId, long bytes) {
        garbage.merge(segmentId, bytes, Long::sum);
    }

    /** The stored entry for {@code key}, or {@code null} — exact match on the live index. */
    private IndexEntry<K> liveEntry(K key) {
        IndexEntry<K> probe = IndexEntry.probe(key);
        if (ensemble != null) {
            List<IndexEntry<K>> hit = ensemble.rangeQuery(probe, probe);
            return hit.isEmpty() ? null : hit.get(0);
        }
        IndexEntry<K> f = navigable.floor(probe);
        return (f != null && opts.comparator().compare(f.key(), key) == 0) ? f : null;
    }

    private List<IndexEntry<K>> indexRange(IndexEntry<K> lo, IndexEntry<K> hi) {
        return (ensemble != null) ? ensemble.rangeQuery(lo, hi) : index.rangeQuery(lo, hi);
    }

    private List<IndexEntry<K>> indexInOrder() {
        return (ensemble != null) ? ensemble.inOrder() : index.inOrder();
    }

    /** Index maintenance (compaction repoint): apply without feeding the workload monitor. */
    private void maintenanceRemove(IndexEntry<K> e) {
        if (ensemble != null) {
            ensemble.remove(e);
        } else {
            index.remove(e);
        }
    }

    private void maintenanceAdd(IndexEntry<K> e) {
        if (ensemble != null) {
            ensemble.add(e);
        } else {
            index.add(e);
        }
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
