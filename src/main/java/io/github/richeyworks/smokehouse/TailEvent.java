package io.github.richeyworks.smokehouse;

/**
 * One committed mutation on the {@linkplain SmokeHouse#tail tail} (Phase 7): its monotonic
 * {@code sequence}, the {@code key}, the new {@code value} ({@code null} for a delete), whether it
 * was a {@code deleted} (tombstone), and the log location ({@code segmentId}, {@code offset}) of the
 * record — enough for a watcher to react and for replication to copy the raw bytes.
 */
public record TailEvent<K, V>(long sequence, K key, V value, boolean deleted, int segmentId, long offset) { }
