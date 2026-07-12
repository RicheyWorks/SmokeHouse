package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.csrbt.AccessPolicy;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;

/**
 * Configuration for a {@link SmokeHouse}. Serializers reuse SuperBeefSort's
 * {@link SpillSerializer} contract (the same one its external sort spills with), so the common
 * key/value types come ready-made: {@code forLongs()}, {@code forIntegers()}, {@code forStrings()}.
 */
public final class SmokeHouseOptions<K, V> {

    /** How much index adaptivity the store runs with. */
    public enum IndexTier {
        /**
         * A single profile-advised tree; no control plane. The baseline. The tree is still
         * <em>born optimal</em>: recovery picks the balancing strategy from
         * {@link #accessPolicy(AccessPolicy)} + the recovery sort's data profile
         * (SuperBeefSort's {@code StrategyAdvisor}), it just never re-shapes afterwards.
         */
        STATIC,
        /**
         * CSRBT's control plane + an internal pilot: the index is born optimal AND re-shapes
         * itself to your traffic (health-gated O(n) morphs across the Red-Black / AVL / Splay /
         * Hybrid family, primed by the recovery sort's profile).
         */
        ADAPTIVE,
        /**
         * CSRBT's ensemble: the index is a mirrored Red-Black + AVL + Splay member trio and the
         * pilot <em>promotes</em> the read path to whichever member matches live traffic — an
         * O(1) pointer swap instead of an O(n) morph — with failover/quarantine/heal health
         * cadences. Costs ~3× index memory (every member mirrors all keys). Incompatible with
         * {@link #retainNewest(int)}: the ensemble surfaces no per-member eviction events, so
         * the retention garbage ledger cannot be funded (open rejects the combination).
         */
        ENSEMBLE,
        /**
         * CSRBT's evolution machine (its ADR-011): an access-advised primary holds the throne
         * while a laboratory member trials parameterized balancing policies against <em>live</em>
         * traffic — a UCB1 bandit over the verified policy grid, with health-gate deaths and
         * policy-gated promotions, all on the record in {@code stats()}. Per CSRBT's own
         * pre-registered verdict this is an <b>observability tier, not a promised speedup</b>;
         * the performance story stays with ADAPTIVE/ENSEMBLE. Same constraints as ENSEMBLE
         * (mirrored members; incompatible with {@link #retainNewest(int)}).
         */
        EVOLUTION
    }

    private final SpillSerializer<K> keySerializer;
    private final SpillSerializer<V> valueSerializer;
    private final Comparator<? super K> comparator;
    private final Fsync fsync;
    private final long fsyncIntervalMillis;
    private final long segmentBytes;
    private final IndexTier indexTier;
    private final Duration pilotCadence;
    private final int retainNewest;
    private final AccessPolicy accessPolicy;
    private final double compactAboveRatio;

    private SmokeHouseOptions(SpillSerializer<K> keySerializer, SpillSerializer<V> valueSerializer,
                              Comparator<? super K> comparator, Fsync fsync, long fsyncIntervalMillis,
                              long segmentBytes, IndexTier indexTier, Duration pilotCadence,
                              int retainNewest, AccessPolicy accessPolicy, double compactAboveRatio) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.comparator = comparator;
        this.fsync = fsync;
        this.fsyncIntervalMillis = fsyncIntervalMillis;
        this.segmentBytes = segmentBytes;
        this.indexTier = indexTier;
        this.pilotCadence = pilotCadence;
        this.retainNewest = retainNewest;
        this.accessPolicy = accessPolicy;
        this.compactAboveRatio = compactAboveRatio;
    }

    /** Options for naturally-ordered keys. Serializers are the only thing you must supply. */
    public static <K extends Comparable<? super K>, V> SmokeHouseOptions<K, V> of(
            SpillSerializer<K> keySerializer, SpillSerializer<V> valueSerializer) {
        return of(keySerializer, valueSerializer, Comparator.<K>naturalOrder());
    }

    /** Options with an explicit key comparator (key {@code equals} must agree with it — see {@link IndexEntry}). */
    public static <K, V> SmokeHouseOptions<K, V> of(SpillSerializer<K> keySerializer,
                                                    SpillSerializer<V> valueSerializer,
                                                    Comparator<? super K> comparator) {
        return new SmokeHouseOptions<>(
                Objects.requireNonNull(keySerializer, "keySerializer"),
                Objects.requireNonNull(valueSerializer, "valueSerializer"),
                Objects.requireNonNull(comparator, "comparator"),
                Fsync.INTERVAL, 50L, 64L << 20, IndexTier.ADAPTIVE, Duration.ofSeconds(5), 0,
                AccessPolicy.BALANCED, 0.5);
    }

    public SmokeHouseOptions<K, V> fsync(Fsync policy) {
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                Objects.requireNonNull(policy), fsyncIntervalMillis, segmentBytes, indexTier,
                pilotCadence, retainNewest, accessPolicy, compactAboveRatio);
    }

    /** Group-fsync period for {@link Fsync#INTERVAL} (default 50 ms) — the bounded loss window. */
    public SmokeHouseOptions<K, V> fsyncIntervalMillis(long millis) {
        if (millis < 1) {
            throw new IllegalArgumentException("fsyncIntervalMillis must be >= 1: " + millis);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, millis, segmentBytes, indexTier, pilotCadence, retainNewest, accessPolicy,
                compactAboveRatio);
    }

    /** Segment roll threshold in bytes (default 64 MB). Small values are useful in tests. */
    public SmokeHouseOptions<K, V> segmentBytes(long bytes) {
        if (bytes < 1024) {
            throw new IllegalArgumentException("segmentBytes must be >= 1024: " + bytes);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, bytes, indexTier, pilotCadence, retainNewest,
                accessPolicy, compactAboveRatio);
    }

    public SmokeHouseOptions<K, V> indexTier(IndexTier tier) {
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, Objects.requireNonNull(tier),
                pilotCadence, retainNewest, accessPolicy, compactAboveRatio);
    }

    /**
     * The access pattern the index should be shaped for (default {@link AccessPolicy#BALANCED}).
     * This is the "born optimal" input: recovery hands it, together with the recovery sort's
     * {@code DataProfile}, to SuperBeefSort's {@code StrategyAdvisor} to pick the balancing
     * strategy the index is built with — and, in the ADAPTIVE/ENSEMBLE tiers, to the
     * profile-guided scorer that primes CSRBT's control plane toward that shape.
     *
     * <p>One clamp: {@link AccessPolicy#WRITE_HEAVY} advises a WeightBalanced tree, which is a
     * deliberately static target outside CSRBT's morph family — so in the ADAPTIVE tier it is
     * clamped to Red-Black (the family's rotation-thrifty member). Declare WRITE_HEAVY with the
     * STATIC tier to get the true WeightBalanced index.</p>
     */
    public SmokeHouseOptions<K, V> accessPolicy(AccessPolicy policy) {
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, indexTier, pilotCadence, retainNewest,
                Objects.requireNonNull(policy, "policy"), compactAboveRatio);
    }

    /** How often the internal pilot runs one policy-gated evaluation (every tier except STATIC). */
    public SmokeHouseOptions<K, V> pilotCadence(Duration cadence) {
        if (cadence.isZero() || cadence.isNegative()) {
            throw new IllegalArgumentException("pilotCadence must be positive: " + cadence);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, indexTier, cadence, retainNewest,
                accessPolicy, compactAboveRatio);
    }

    /**
     * Retention tier (Phase 2): keep only the {@code n} most-recently-written keys; older ones
     * are evicted from the index (their log bytes become compactable garbage — no tombstones
     * needed, recovery re-derives the newest-n from log order). {@code 0} = unbounded (default).
     * Incompatible with the ensemble-backed tiers ({@link IndexTier#ENSEMBLE},
     * {@link IndexTier#EVOLUTION} — see their javadoc).
     */
    public SmokeHouseOptions<K, V> retainNewest(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("retainNewest must be >= 0: " + n);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, indexTier, pilotCadence, n, accessPolicy,
                compactAboveRatio);
    }

    /**
     * Auto-compaction (Phase 4.3): when the internal pilot sees
     * {@code garbageBytes(closed) / totalBytes(closed) > ratio} it runs {@link SmokeHouse#compact()}
     * itself — the copy phase off the store lock, re-entry guarded, the verdict on the record in
     * {@code stats()}. {@code 0} disables; default {@code 0.5}. Rides the pilot thread, so the
     * STATIC tier (which has no pilot) never auto-compacts — call {@code compact()} manually there,
     * with the garbage ledger telling you when it's worth it.
     */
    public SmokeHouseOptions<K, V> compactWhenGarbageAbove(double ratio) {
        if (ratio < 0.0 || ratio >= 1.0) {
            throw new IllegalArgumentException("ratio must be in [0, 1): " + ratio);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, indexTier, pilotCadence, retainNewest,
                accessPolicy, ratio);
    }

    SpillSerializer<K> keySerializer() { return keySerializer; }
    SpillSerializer<V> valueSerializer() { return valueSerializer; }
    Comparator<? super K> comparator() { return comparator; }
    Fsync fsyncPolicy() { return fsync; }
    long fsyncInterval() { return fsyncIntervalMillis; }
    long segmentBytesLimit() { return segmentBytes; }
    IndexTier tier() { return indexTier; }
    Duration cadence() { return pilotCadence; }
    int retention() { return retainNewest; }
    AccessPolicy access() { return accessPolicy; }
    double compactAbove() { return compactAboveRatio; }
}
