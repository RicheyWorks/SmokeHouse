package io.github.richeyworks.smokehouse;

import io.github.richeyworks.csrbt.OrderedSet;
import io.github.richeyworks.csrbt.TreeContext;
import io.github.richeyworks.csrbt.augment.IntervalAugmentor;
import io.github.richeyworks.csrbt.strategy.RedBlackStrategy;
import io.github.richeyworks.superbeefsort.BeefSort;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Secondary and interval indexes over a {@link SmokeHouse} (Phase 4.1 + 4.2) — composition, not
 * modification: an {@code IndexedStore} <b>owns</b> a primary store plus named indexes, holds its
 * own outer lock, and is the only thing callers touch.
 *
 * <p><b>Plain secondaries</b> ({@link Builder#secondary}): each is a CSRBT {@link OrderedSet} of
 * composite {@code (attribute, primaryKey)} entries ordered by attribute <em>then</em> key —
 * composites keep equal attributes distinct — so {@link #byAttribute} is one range walk.</p>
 *
 * <p><b>Interval indexes</b> ({@link Builder#interval}): for records exposing an
 * {@code (int start, int end)} span (epoch minutes/seconds and the like — CSRBT's
 * {@link IntervalAugmentor} is deliberately {@code Integer}-bound in v1). Each is a CSRBT
 * interval tree (CLRS 14.3): distinct start points are the keys, the node tag carries the
 * <em>maximum</em> end among live spans sharing that start, and the augmentor maintains
 * subtree max-end for pruned search. An exact sidecar (start → end → keys) resolves the
 * candidate starts the tree reports into precisely the matching primary keys — so duplicate
 * starts and duplicate spans are fully supported. {@link #stab} answers "which records cover
 * this point", {@link #overlapping} "which records overlap this span", both in
 * candidate-pruned walks, results ordered by (start, end, key).</p>
 *
 * <p><b>Consistency stance:</b> all indexes update in the same critical section as the primary;
 * there is no cross-index transaction log; a crash loses nothing because <b>all</b> indexes
 * rebuild from the log — {@link Builder#build()} rebuilds every index by scanning the primary
 * (full range sweep → extract → SuperBeefSort sort → {@code OrderedSet.fromSorted} for
 * secondaries; per-span inserts for interval trees). Indexes are pure memory; nothing about
 * them is ever persisted.</p>
 *
 * <p><b>Costs and bounds, honestly:</b> an indexed {@link #put}/{@link #delete} does one extra
 * primary read first — the old value must be known to retract its stale index entries — and all
 * extractors run <em>before</em> the primary write, so a bad value (null attribute, inverted
 * span) rejects the put with the store untouched. Attribute types must have {@code equals}
 * agreeing with their comparator (the discipline the primary key already obeys); extractors
 * must be pure and never return {@code null}. v1 refuses {@code retainNewest > 0} with any
 * index declared: retention evictions happen inside the primary's index and bypass this class,
 * so derived indexes would silently go stale (future work: an eviction callback seam). Mutating
 * the {@link #primary()} directly bypasses every index — don't.</p>
 */
public final class IndexedStore<K, V> implements Closeable {

    private final Object lock = new Object();
    private final SmokeHouse<K, V> store;
    private final Map<String, Secondary<K, V>> secondaries;            // insertion-ordered
    private final Map<String, IntervalSecondary<K, V>> intervals;      // insertion-ordered

    private IndexedStore(SmokeHouse<K, V> store,
                         LinkedHashMap<String, Secondary<K, V>> secondaries,
                         LinkedHashMap<String, IntervalSecondary<K, V>> intervals) {
        this.store = store;
        this.secondaries = secondaries;
        this.intervals = intervals;
    }

    /** Start building an indexed store over {@code dir}; declare indexes, then {@link Builder#build()}. */
    public static <K, V> Builder<K, V> open(Path dir, SmokeHouseOptions<K, V> opts) {
        return new Builder<>(Objects.requireNonNull(dir, "dir"), Objects.requireNonNull(opts, "opts"));
    }

    /** Declares the indexes, then opens the primary and rebuilds them from it. */
    public static final class Builder<K, V> {
        private final Path dir;
        private final SmokeHouseOptions<K, V> opts;
        private final LinkedHashMap<String, Secondary<K, V>> secondaries = new LinkedHashMap<>();
        private final LinkedHashMap<String, IntervalSecondary<K, V>> intervals = new LinkedHashMap<>();

        private Builder(Path dir, SmokeHouseOptions<K, V> opts) {
            this.dir = dir;
            this.opts = opts;
        }

        /**
         * Declare a plain secondary index: {@code extractor} pulls the attribute out of the
         * value, {@code order} ranks attributes. The attribute type's {@code equals} must agree
         * with {@code order}, and the extractor must be a pure function of the value.
         */
        @SuppressWarnings("unchecked")
        public <A> Builder<K, V> secondary(String name, Comparator<A> order, Function<V, A> extractor) {
            Objects.requireNonNull(order, "order");
            Objects.requireNonNull(extractor, "extractor");
            reserve(name);
            secondaries.put(name, new Secondary<>(name,
                    (Comparator<Object>) (Comparator<?>) order, (Function<V, Object>) extractor));
            return this;
        }

        /**
         * Declare an interval index over the {@code [start(v), end(v)]} span of each value
         * (closed, {@code int} endpoints, {@code start <= end} — enforced on every write).
         * Query it with {@link #stab} and {@link #overlapping}.
         */
        public Builder<K, V> interval(String name, ToIntFunction<V> start, ToIntFunction<V> end) {
            Objects.requireNonNull(start, "start");
            Objects.requireNonNull(end, "end");
            reserve(name);
            intervals.put(name, new IntervalSecondary<>(name, start, end, opts.comparator()));
            return this;
        }

        private void reserve(String name) {
            Objects.requireNonNull(name, "name");
            if (secondaries.containsKey(name) || intervals.containsKey(name)) {
                throw new IllegalArgumentException("duplicate index name: " + name);
            }
        }

        /**
         * Open the primary and rebuild every declared index from it.
         *
         * @throws IllegalArgumentException if the options carry {@code retainNewest > 0} and
         *         indexes are declared (evictions would bypass this class — see class javadoc)
         */
        public IndexedStore<K, V> build() throws IOException {
            if (opts.retention() > 0 && !(secondaries.isEmpty() && intervals.isEmpty())) {
                throw new IllegalArgumentException("retainNewest > 0 is incompatible with "
                        + "IndexedStore indexes in v1: retention evictions happen inside the "
                        + "primary index and bypass IndexedStore, so derived indexes would "
                        + "silently go stale. Drop retention or the indexes (future work: an "
                        + "eviction callback seam).");
            }
            SmokeHouse<K, V> store = SmokeHouse.open(dir, opts);
            try {
                for (Secondary<K, V> s : secondaries.values()) {
                    s.rebuild(store, opts.comparator());
                }
                for (IntervalSecondary<K, V> s : intervals.values()) {
                    s.rebuild(store);
                }
            } catch (IOException | RuntimeException rebuildFailed) {
                store.close();
                throw rebuildFailed;
            }
            return new IndexedStore<>(store, secondaries, intervals);
        }
    }

    // ── Data plane ────────────────────────────────────────────────────────────────────────────

    /**
     * Insert or overwrite, fanning out to every index. Does one extra primary read first — the
     * previous value's index entries must be retracted (the documented indexed-put cost) — and
     * runs every extractor <em>before</em> the primary write, so a rejected value leaves the
     * store untouched.
     */
    public void put(K key, V value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        synchronized (lock) {
            boolean indexed = !(secondaries.isEmpty() && intervals.isEmpty());
            V old = indexed ? store.get(key) : null;

            // Validate-first: compute the whole new fan-out before touching the primary.
            List<AttrEntry<K>> attrAdds = new ArrayList<>(secondaries.size());
            for (Secondary<K, V> s : secondaries.values()) {
                attrAdds.add(s.entry(value, key));
            }
            List<int[]> spanAdds = new ArrayList<>(intervals.size());
            for (IntervalSecondary<K, V> s : intervals.values()) {
                spanAdds.add(s.span(value, key));
            }

            store.put(key, value);                                     // truth before every cache

            int i = 0;
            for (Secondary<K, V> s : secondaries.values()) {
                if (old != null) {
                    s.set.remove(s.entry(old, key));
                }
                s.set.add(attrAdds.get(i++));
            }
            i = 0;
            for (IntervalSecondary<K, V> s : intervals.values()) {
                if (old != null) {
                    s.remove(old, key);
                }
                int[] span = spanAdds.get(i++);
                s.add(span[0], span[1], key);
            }
        }
    }

    /** Delete from the primary and retract the key's entries from every index. */
    public boolean delete(K key) throws IOException {
        Objects.requireNonNull(key, "key");
        synchronized (lock) {
            boolean indexed = !(secondaries.isEmpty() && intervals.isEmpty());
            V old = indexed ? store.get(key) : null;
            boolean existed = store.delete(key);
            if (old != null) {
                for (Secondary<K, V> s : secondaries.values()) {
                    s.set.remove(s.entry(old, key));
                }
                for (IntervalSecondary<K, V> s : intervals.values()) {
                    s.remove(old, key);
                }
            }
            return existed;
        }
    }

    /** The newest value for {@code key}, or {@code null} — a plain primary read. */
    public V get(K key) throws IOException {
        return store.get(key);
    }

    /**
     * Every primary key whose {@code name} attribute lies in {@code [lo, hi]} (both closed),
     * ordered by attribute then key — one range walk on the secondary via sentinel probes
     * ({@code (lo, -∞)} to {@code (hi, +∞)}), no post-filtering.
     *
     * @throws IllegalArgumentException if no secondary named {@code name} was declared
     */
    public List<K> byAttribute(String name, Object lo, Object hi) {
        Secondary<K, V> s = secondaries.get(name);
        if (s == null) {
            throw new IllegalArgumentException("no secondary named '" + name
                    + "'; declared: " + secondaries.keySet());
        }
        Objects.requireNonNull(lo, "lo");
        Objects.requireNonNull(hi, "hi");
        synchronized (lock) {
            List<AttrEntry<K>> hits = s.set.rangeQuery(AttrEntry.loProbe(lo), AttrEntry.hiProbe(hi));
            List<K> keys = new ArrayList<>(hits.size());
            for (AttrEntry<K> e : hits) {
                keys.add(e.key());
            }
            return keys;
        }
    }

    /**
     * Every primary key whose {@code name} span contains {@code point}
     * ({@code start <= point <= end}), ordered by (start, end, key) — CLRS interval stabbing
     * over the max-end-augmented tree, candidates resolved exactly through the sidecar.
     *
     * @throws IllegalArgumentException if no interval index named {@code name} was declared
     */
    public List<K> stab(String name, int point) {
        IntervalSecondary<K, V> s = intervalIndex(name);
        synchronized (lock) {
            return s.stab(point);
        }
    }

    /**
     * Every primary key whose {@code name} span overlaps {@code [lo, hi]} (closed; overlap =
     * {@code start <= hi && end >= lo}), ordered by (start, end, key).
     *
     * @throws IllegalArgumentException if no interval index named {@code name} was declared,
     *         or {@code lo > hi}
     */
    public List<K> overlapping(String name, int lo, int hi) {
        if (lo > hi) {
            throw new IllegalArgumentException("lo " + lo + " > hi " + hi);
        }
        IntervalSecondary<K, V> s = intervalIndex(name);
        synchronized (lock) {
            return s.overlapping(lo, hi);
        }
    }

    private IntervalSecondary<K, V> intervalIndex(String name) {
        IntervalSecondary<K, V> s = intervals.get(name);
        if (s == null) {
            throw new IllegalArgumentException("no interval index named '" + name
                    + "'; declared: " + intervals.keySet());
        }
        return s;
    }

    /**
     * The primary store — for reads, stats, and compaction. Mutating it directly bypasses the
     * indexes; route writes through this class.
     */
    public SmokeHouse<K, V> primary() {
        return store;
    }

    /** Close the primary (indexes are memory-only; the next {@code build()} rebuilds them). */
    @Override
    public void close() throws IOException {
        synchronized (lock) {
            store.close();
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────────────────────

    /** One named plain secondary: the composite-entry set plus how to derive entries from values. */
    private static final class Secondary<K, V> {
        final String name;
        final Comparator<Object> attrOrder;
        final Function<V, Object> extractor;
        OrderedSet<AttrEntry<K>> set;

        Secondary(String name, Comparator<Object> attrOrder, Function<V, Object> extractor) {
            this.name = name;
            this.attrOrder = attrOrder;
            this.extractor = extractor;
        }

        /** Full primary sweep → extract → sort (SuperBeefSort) → O(n) bulk build. */
        void rebuild(SmokeHouse<K, V> store, Comparator<? super K> keyOrder) throws IOException {
            Comparator<AttrEntry<K>> ordering = AttrEntry.ordering(attrOrder, keyOrder);
            List<AttrEntry<K>> entries = new ArrayList<>(store.size());
            if (store.size() > 0) {
                store.range(store.firstKey(), store.lastKey(), (k, v) -> entries.add(entry(v, k)));
            }
            List<AttrEntry<K>> sorted = (entries.size() > 1)
                    ? BeefSort.with(ordering).source(entries).run().sorted()
                    : entries;
            this.set = OrderedSet.fromSorted(sorted, new RedBlackStrategy<>(), ordering);
        }

        AttrEntry<K> entry(V value, K key) {
            Object attribute = extractor.apply(value);
            if (attribute == null) {
                throw new IllegalArgumentException("secondary '" + name
                        + "': extractor returned null for key " + key
                        + " — attributes must be non-null");
            }
            return AttrEntry.of(attribute, key);
        }
    }

    /**
     * One named interval index: CSRBT's {@link IntervalAugmentor} tree over distinct start
     * points (tag = max end at that start, augment = subtree max end — the CLRS 14.3 encoding)
     * plus the exact sidecar {@code start → end → keys}. The tree prunes the candidate walk;
     * the sidecar turns candidate starts into exactly the matching keys, which is what makes
     * duplicate starts and duplicate spans safe under the tag encoding (the tag can only hold
     * one end per start, so it holds the <em>maximum</em> — an upper bound that never prunes
     * away a real match).
     */
    private static final class IntervalSecondary<K, V> {
        final String name;
        final ToIntFunction<V> startOf;
        final ToIntFunction<V> endOf;
        final Comparator<? super K> keyOrder;
        final TreeContext ctx = new TreeContext(new RedBlackStrategy<>());
        final Map<Integer, TreeMap<Integer, TreeSet<K>>> byStart = new HashMap<>();

        IntervalSecondary(String name, ToIntFunction<V> startOf, ToIntFunction<V> endOf,
                          Comparator<? super K> keyOrder) {
            this.name = name;
            this.startOf = startOf;
            this.endOf = endOf;
            this.keyOrder = keyOrder;
        }

        /** Extract + validate the span BEFORE the primary write. */
        int[] span(V value, K key) {
            int lo = startOf.applyAsInt(value);
            int hi = endOf.applyAsInt(value);
            if (lo > hi) {
                throw new IllegalArgumentException("interval '" + name + "': start " + lo
                        + " > end " + hi + " for key " + key);
            }
            return new int[]{lo, hi};
        }

        void add(int lo, int hi, K key) {
            TreeMap<Integer, TreeSet<K>> ends = byStart.computeIfAbsent(lo, x -> new TreeMap<>());
            ends.computeIfAbsent(hi, x -> new TreeSet<>(keyOrder)).add(key);
            // (Re-)stamp this start's tag with its max end; insertInterval is add-or-restamp
            // and re-augments the root path either way.
            IntervalAugmentor.insertInterval(ctx, lo, ends.lastKey());
        }

        void remove(V oldValue, K key) {
            int lo = startOf.applyAsInt(oldValue);
            int hi = endOf.applyAsInt(oldValue);
            TreeMap<Integer, TreeSet<K>> ends = byStart.get(lo);
            if (ends == null) {
                return;
            }
            TreeSet<K> keys = ends.get(hi);
            if (keys == null || !keys.remove(key)) {
                return;
            }
            if (keys.isEmpty()) {
                ends.remove(hi);
            }
            if (ends.isEmpty()) {
                byStart.remove(lo);
                ctx.remove(lo);                                        // last span at this start
            } else {
                IntervalAugmentor.insertInterval(ctx, lo, ends.lastKey());   // max end may have dropped
            }
        }

        List<K> stab(int point) {
            return resolve(IntervalAugmentor.stabQuery(ctx, point), point);
        }

        List<K> overlapping(int qlo, int qhi) {
            return resolve(IntervalAugmentor.intervalSearchAll(ctx, qlo, qhi), qlo);
        }

        /** Candidate starts (each with end >= minEnd guaranteed only at max) → exact keys. */
        private List<K> resolve(List<int[]> candidates, int minEnd) {
            candidates.sort(Comparator.comparingInt(c -> c[0]));       // (start, end, key) ordering
            List<K> out = new ArrayList<>();
            for (int[] c : candidates) {
                TreeMap<Integer, TreeSet<K>> ends = byStart.get(c[0]);
                if (ends == null) {
                    continue;
                }
                for (TreeSet<K> keys : ends.tailMap(minEnd, true).values()) {
                    out.addAll(keys);
                }
            }
            return out;
        }

        /** Full primary sweep → per-span insert (distinct starts only cost tree nodes once). */
        void rebuild(SmokeHouse<K, V> store) throws IOException {
            if (store.size() == 0) {
                return;
            }
            store.range(store.firstKey(), store.lastKey(), (k, v) -> {
                int[] s = span(v, k);
                add(s[0], s[1], k);
            });
        }
    }

    /**
     * The composite secondary entry: {@code (attribute, primaryKey)} plus a sentinel rank so a
     * probe can stand for {@code (attr, -∞)} / {@code (attr, +∞)} — the two-sided version of
     * {@link IndexEntry#probe}'s trick, giving {@code byAttribute} an exact closed range with
     * no filtering. Sentinels are probe-only and never stored, so the record's default
     * {@code equals} (attribute + key + rank) agrees with the comparator for everything that
     * actually lives in the set.
     */
    record AttrEntry<K>(Object attribute, K key, int rank) {

        static <K> AttrEntry<K> of(Object attribute, K key) {
            return new AttrEntry<>(attribute, key, 0);
        }

        static <K> AttrEntry<K> loProbe(Object attribute) {
            return new AttrEntry<>(attribute, null, -1);               // before every real key
        }

        static <K> AttrEntry<K> hiProbe(Object attribute) {
            return new AttrEntry<>(attribute, null, 1);                // after every real key
        }

        static <K> Comparator<AttrEntry<K>> ordering(Comparator<Object> attrOrder,
                                                     Comparator<? super K> keyOrder) {
            return (a, b) -> {
                int c = attrOrder.compare(a.attribute(), b.attribute());
                if (c != 0) {
                    return c;
                }
                c = Integer.compare(a.rank(), b.rank());
                if (c != 0) {
                    return c;
                }
                return (a.rank() != 0) ? 0 : keyOrder.compare(a.key(), b.key());
            };
        }
    }
}
