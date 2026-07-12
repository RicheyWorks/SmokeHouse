package io.github.richeyworks.smokehouse;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The tail engine (Phase 7): an ordered, gap-free stream of committed mutations. The single-writer
 * contract makes it cheap — the writer assigns each committed put/delete a monotonic sequence and
 * {@link #publish}es it here under the store lock (enqueue only, no delivery). A bounded ring keeps
 * recent events so a late subscriber can replay them; each subscriber then gets its own bounded
 * queue drained by a daemon thread, so delivery runs <em>off</em> the store lock and one slow
 * consumer never stalls the writer or the others — it drops oldest and is told it gapped (the
 * aquarium treatment SuperBeefSort's demo pioneered). Built once, consumed by watchers and, in
 * Phase 8, replication.
 *
 * <p>v1 replays only what the ring still holds; a subscriber asking for history older than the ring
 * floor is served from the floor and flagged gapped. Log-backed catch-up for arbitrarily old
 * sequences is the documented next increment, shaped by Phase 8's bootstrap needs.</p>
 */
final class Tail<K, V> {

    private final int ringCapacity;
    private final Deque<TailEvent<K, V>> ring = new ArrayDeque<>();     // recent events, guarded by `this`
    private final AtomicLong sequence = new AtomicLong();              // next sequence to assign
    private final CopyOnWriteArrayList<Sub> subs = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    Tail(int ringCapacity) {
        this.ringCapacity = ringCapacity;
    }

    /** The next sequence that will be assigned — i.e. how many events have been published so far. */
    long nextSequence() {
        return sequence.get();
    }

    /** Publish a committed mutation. Called by the writer under the store lock; returns fast (enqueue only). */
    synchronized void publish(K key, V value, boolean deleted, int segmentId, long offset) {
        if (closed) {
            return;
        }
        long seq = sequence.getAndIncrement();
        TailEvent<K, V> event = new TailEvent<>(seq, key, value, deleted, segmentId, offset);
        ring.addLast(event);
        while (ring.size() > ringCapacity) {
            ring.removeFirst();
        }
        for (Sub sub : subs) {
            sub.offer(event);
        }
    }

    /**
     * Subscribe from {@code fromSequence}. Whatever the ring still holds at or after it is replayed
     * first (in order), then live events stream as they publish. Returns a handle; closing it
     * unsubscribes and stops the tail thread.
     */
    AutoCloseable subscribe(long fromSequence, TailListener<K, V> listener) {
        Sub sub = new Sub(listener);
        synchronized (this) {
            long floor = ring.isEmpty() ? sequence.get() : ring.peekFirst().sequence();
            if (fromSequence < floor && floor > 0) {
                sub.markGap();                                         // requested history already evicted
            }
            for (TailEvent<K, V> e : ring) {
                if (e.sequence() >= fromSequence) {
                    sub.offer(e);
                }
            }
            subs.add(sub);                                            // now live: publishes fan out to it too
        }
        sub.start();
        return sub;
    }

    /** Stop the tail: cancel every subscriber's thread. Idempotent; publishes become no-ops. */
    void close() {
        closed = true;
        for (Sub sub : subs) {
            sub.cancel();
        }
        subs.clear();
    }

    /** One subscriber: a bounded queue drained by a daemon thread, drop-oldest when the consumer lags. */
    private final class Sub implements AutoCloseable {
        private final TailListener<K, V> listener;
        private final LinkedBlockingQueue<TailEvent<K, V>> queue = new LinkedBlockingQueue<>(1 << 12);
        private volatile boolean gapped;
        private volatile boolean running = true;
        private Thread pump;

        Sub(TailListener<K, V> listener) {
            this.listener = listener;
        }

        void start() {
            pump = new Thread(this::run, "smokehouse-tail");
            pump.setDaemon(true);
            pump.start();
        }

        void offer(TailEvent<K, V> event) {
            if (!queue.offer(event)) {
                queue.poll();                                        // drop-oldest: the slow-consumer contract
                queue.offer(event);
                gapped = true;
            }
        }

        void markGap() {
            gapped = true;
        }

        private void run() {
            while (running) {
                TailEvent<K, V> event;
                try {
                    event = queue.take();
                } catch (InterruptedException stop) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (gapped) {
                    gapped = false;
                    fire(listener::onGap);
                }
                fire(() -> listener.onEvent(event));
            }
        }

        private void fire(Runnable delivery) {
            try {
                delivery.run();
            } catch (RuntimeException ignored) {
                // a listener must never kill the tail thread
            }
        }

        void cancel() {
            running = false;
            if (pump != null) {
                pump.interrupt();
            }
        }

        @Override
        public void close() {
            cancel();
            subs.remove(this);
        }
    }
}
