package io.github.richeyworks.smokehouse;

/**
 * A consumer of the {@linkplain SmokeHouse#tail tail}. Events arrive in sequence order on a tail
 * thread, off the store lock — so a listener may do slow work without stalling the writer, at the
 * cost of dropping oldest (and being told via {@link #onGap()}) if it falls too far behind.
 */
public interface TailListener<K, V> {

    /** A committed mutation, in sequence order. */
    void onEvent(TailEvent<K, V> event);

    /**
     * Signalled just before the next event when this consumer fell behind and older events were
     * dropped, or when it subscribed from history already evicted from the ring. The stream is no
     * longer gap-free from here; re-bootstrap from a snapshot or backup if that matters.
     */
    default void onGap() {
    }
}
