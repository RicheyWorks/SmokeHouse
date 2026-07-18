package io.github.richeyworks.smokehouse;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Phase 8 (D3 option A): a read replica — <b>a SmokeHouse whose writer happens to live
 * elsewhere</b>. It bootstraps from the primary's shipped backup (restore = open; recovery
 * does the work), then applies the primary's tail frames through its own {@code put}/
 * {@code delete} — append to its own log, update its own CSRBT, exactly as recovery would,
 * incrementally. Every index tier works unchanged; every read surface (gets, ranges, order
 * statistics, its own tail and watchers) is live; {@link #lagSequence()} says how far behind
 * the truth it is.
 *
 * <p><b>Non-goals, stated loudly:</b> no automatic failover, no write forwarding, no
 * consensus. A dead primary means a manually promoted replica, and split-brain prevention is
 * the operator's problem in v1. Writing to a replica's store directly is undefined behavior —
 * the apply thread is the single writer, same contract as everywhere in the ring.</p>
 *
 * <p>A replica that falls too far behind is told it {@linkplain #gapped() gapped} and stops
 * applying — its data is a consistent-but-stale prefix; re-connect a fresh replica to
 * re-bootstrap. Honest, never wrong.</p>
 */
public final class Replica<K, V> implements Closeable {

    private final SmokeHouse<K, V> store;
    private final Socket socket;
    private final DataInputStream in;
    private final SmokeHouseOptions<K, V> opts;
    private final Thread apply;

    private volatile long appliedSequence = -1;
    private volatile long primarySequence;
    private volatile boolean gapped;

    private Replica(SmokeHouse<K, V> store, Socket socket, DataInputStream in,
                    SmokeHouseOptions<K, V> opts) {
        this.store = store;
        this.socket = socket;
        this.in = in;
        this.opts = opts;
        this.apply = new Thread(this::applyLoop, "smokehouse-replica-apply");
        this.apply.setDaemon(true);
    }

    /**
     * Bootstrap a replica into {@code dir} (must be empty) from the primary serving on the
     * loopback {@code port}: receive the shipped backup, open it (recovery rebuilds the
     * index), then follow the tail.
     */
    public static <K, V> Replica<K, V> connect(Path dir, SmokeHouseOptions<K, V> opts, int port)
            throws IOException {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(opts, "opts");
        Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
        try {
            DataInputStream in = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream(), 1 << 16));
            Files.createDirectories(dir);
            int files = in.readInt();
            byte[] buf = new byte[1 << 16];
            for (int i = 0; i < files; i++) {
                String name = in.readUTF();
                if (name.contains("/") || name.contains("\\") || name.contains("..")) {
                    throw new IOException("unsafe shipped file name: " + name);
                }
                long size = in.readLong();
                try (OutputStream fileOut = Files.newOutputStream(dir.resolve(name))) {
                    long left = size;
                    while (left > 0) {
                        int n = in.read(buf, 0, (int) Math.min(buf.length, left));
                        if (n < 0) {
                            throw new EOFException("stream ended mid-file: " + name);
                        }
                        fileOut.write(buf, 0, n);
                        left -= n;
                    }
                }
            }
            SmokeHouse<K, V> store = SmokeHouse.restore(dir, opts);
            Replica<K, V> replica = new Replica<>(store, socket, in, opts);
            replica.apply.start();
            return replica;
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // bootstrap failed; the original exception is the story
            }
            throw e;
        }
    }

    private void applyLoop() {
        try {
            while (true) {
                byte type = in.readByte();
                if (type == ReplicationServer.FRAME_GAP) {
                    gapped = true;
                    return;
                }
                long sequence = in.readLong();
                primarySequence = in.readLong();
                K key = opts.keySerializer().read(in);
                if (type == ReplicationServer.FRAME_PUT) {
                    V value = opts.valueSerializer().read(in);
                    store.put(key, value);
                } else if (type == ReplicationServer.FRAME_DELETE) {
                    store.delete(key);
                } else {
                    throw new IOException("unknown frame type " + type);
                }
                appliedSequence = sequence;
            }
        } catch (IOException endOfStream) {
            // primary closed, we closed, or the wire broke — the replica keeps serving reads
        }
    }

    /** The replica's own store — every read surface, live. Do not write to it. */
    public SmokeHouse<K, V> store() {
        return store;
    }

    /** How many committed primary mutations this replica has not yet applied (0 = caught up). */
    public long lagSequence() {
        return Math.max(0, (primarySequence - 1) - appliedSequence);
    }

    /** The last primary tail sequence applied here ({@code -1} before the first frame). */
    public long appliedSequence() {
        return appliedSequence;
    }

    /** True once the primary dropped frames for this replica; applying has stopped for good. */
    public boolean gapped() {
        return gapped;
    }

    /**
     * Block until every primary mutation before {@code primaryTailSequence} is applied (or
     * the timeout passes). Pass the primary's {@code tailSequence()} sampled after the writes
     * you care about.
     */
    public boolean awaitCaughtUp(long primaryTailSequence, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (appliedSequence < primaryTailSequence - 1 && !gapped) {
            if (System.currentTimeMillis() > deadline) {
                return false;
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !gapped;
    }

    /** Disconnect and close the replica's store. Its directory remains a valid SmokeHouse. */
    @Override
    public void close() throws IOException {
        try {
            socket.close();
        } catch (IOException ignored) {
            // wire teardown; the store close below is what matters
        }
        try {
            apply.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        store.close();
    }
}
