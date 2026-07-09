package io.github.richeyworks.smokehouse;

import java.util.Comparator;
import java.util.Objects;

/**
 * What the CSRBT index actually holds: a key plus the durable address and size of its newest
 * record. Ordered <em>by key alone</em> (locations never affect ordering), so
 * {@code NavigableOrderedSet.floor(probe(key))} retrieves the stored entry — a set becomes a map
 * through CSRBT's public API, no engine changes (ADR D2). {@code recordBytes} funds the garbage
 * accounting: when this entry is overwritten, deleted, or evicted, exactly that many log bytes
 * die, and compaction knows what it can reclaim.
 *
 * <p>{@code equals}/{@code hashCode} also delegate to the key: the index's workload monitor then
 * sees real key hashes (skew detection works on your keys, not on entry identities), and the
 * standard requirement follows — <b>key {@code equals} must agree with the store's
 * comparator</b>, exactly as {@code TreeMap} + {@code HashMap} interop already demands.</p>
 */
public final class IndexEntry<K> {

    private final K key;
    private final int segmentId;
    private final long offset;
    private final int recordBytes;

    IndexEntry(K key, int segmentId, long offset, int recordBytes) {
        this.key = Objects.requireNonNull(key, "key");
        this.segmentId = segmentId;
        this.offset = offset;
        this.recordBytes = recordBytes;
    }

    /** A location-less probe for lookups/removals — compares equal to any entry with this key. */
    static <K> IndexEntry<K> probe(K key) {
        return new IndexEntry<>(key, -1, -1L, -1);
    }

    /** Key-only ordering over entries, derived from the store's key comparator. */
    static <K> Comparator<IndexEntry<K>> ordering(Comparator<? super K> keyOrder) {
        return (a, b) -> keyOrder.compare(a.key, b.key);
    }

    public K key() {
        return key;
    }

    public int segmentId() {
        return segmentId;
    }

    public long offset() {
        return offset;
    }

    /** Total on-disk bytes of the record this entry points at (header + key + value). */
    public int recordBytes() {
        return recordBytes;
    }

    SegmentLog.Location location() {
        return new SegmentLog.Location(segmentId, offset);
    }

    /** Same durable address (used by compaction's repoint-skip: "is the index still pointing here?"). */
    boolean sameLocation(IndexEntry<K> other) {
        return other != null && segmentId == other.segmentId && offset == other.offset;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof IndexEntry<?> e && key.equals(e.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return key + "@" + segmentId + ":" + offset + "(" + recordBytes + "B)";
    }
}
