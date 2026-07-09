package io.github.richeyworks.smokehouse;

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
        /** A plain red-black index; no control plane. The baseline. */
        STATIC,
        /** CSRBT's control plane + an internal pilot: the index re-shapes itself to your traffic. */
        ADAPTIVE
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

    private SmokeHouseOptions(SpillSerializer<K> keySerializer, SpillSerializer<V> valueSerializer,
                              Comparator<? super K> comparator, Fsync fsync, long fsyncIntervalMillis,
                              long segmentBytes, IndexTier indexTier, Duration pilotCadence,
                              int retainNewest) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.comparator = comparator;
        this.fsync = fsync;
        this.fsyncIntervalMillis = fsyncIntervalMillis;
        this.segmentBytes = segmentBytes;
        this.indexTier = indexTier;
        this.pilotCadence = pilotCadence;
        this.retainNewest = retainNewest;
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
                Fsync.INTERVAL, 50L, 64L << 20, IndexTier.ADAPTIVE, Duration.ofSeconds(5), 0);
    }

    public SmokeHouseOptions<K, V> fsync(Fsync policy) {
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                Objects.requireNonNull(policy), fsyncIntervalMillis, segmentBytes, indexTier,
                pilotCadence, retainNewest);
    }

    /** Group-fsync period for {@link Fsync#INTERVAL} (default 50 ms) — the bounded loss window. */
    public SmokeHouseOptions<K, V> fsyncIntervalMillis(long millis) {
        if (millis < 1) {
            throw new IllegalArgumentException("fsyncIntervalMillis must be >= 1: " + millis);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, millis, segmentBytes, indexTier, pilotCadence, retainNewest);
    }

    /** Segment roll threshold in bytes (default 64 MB). Small values are useful in tests. */
    public SmokeHouseOptions<K, V> segmentBytes(long bytes) {
        if (bytes < 1024) {
            throw new IllegalArgumentException("segmentBytes must be >= 1024: " + bytes);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, bytes, indexTier, pilotCadence, retainNewest);
    }

    public SmokeHouseOptions<K, V> indexTier(IndexTier tier) {
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, Objects.requireNonNull(tier),
                pilotCadence, retainNewest);
    }

    /** How often the internal pilot runs one policy-gated morph evaluation (ADAPTIVE tier only). */
    public SmokeHouseOptions<K, V> pilotCadence(Duration cadence) {
        if (cadence.isZero() || cadence.isNegative()) {
            throw new IllegalArgumentException("pilotCadence must be positive: " + cadence);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, indexTier, cadence, retainNewest);
    }

    /**
     * Retention tier (Phase 2): keep only the {@code n} most-recently-written keys; older ones
     * are evicted from the index (their log bytes become compactable garbage — no tombstones
     * needed, recovery re-derives the newest-n from log order). {@code 0} = unbounded (default).
     */
    public SmokeHouseOptions<K, V> retainNewest(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("retainNewest must be >= 0: " + n);
        }
        return new SmokeHouseOptions<>(keySerializer, valueSerializer, comparator,
                fsync, fsyncIntervalMillis, segmentBytes, indexTier, pilotCadence, n);
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
}
