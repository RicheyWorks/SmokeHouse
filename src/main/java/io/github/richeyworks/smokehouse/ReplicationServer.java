package io.github.richeyworks.smokehouse;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Phase 8 (D3 option A): the primary's side of the replication ring. Serves any number of
 * {@linkplain Replica read replicas} over a loopback JDK socket, zero dependencies. Per
 * client, the protocol is <b>subscribe → buffer → backup → ship → go live</b>:
 *
 * <ol>
 *   <li>Subscribe to the {@linkplain SmokeHouse#tail tail} first, buffering events;</li>
 *   <li>{@linkplain SmokeHouse#backup backup} to a temp dir (segments + manifest, under the
 *       store lock, CRC'd) and ship the files;</li>
 *   <li>flush the buffered events and stream live from then on.</li>
 * </ol>
 *
 * Events that committed between the subscription and the backup are shipped twice — once in
 * the segments, once as frames — and that is deliberately fine: a replica applies frames
 * through its own {@code put}/{@code delete}, which are last-writer-wins upserts, so replays
 * are harmless. The same replace-idempotency argument that makes Renderer's bootstrap
 * race-free makes this one exact.
 *
 * <p>Frames ride the tail subscriber's own thread (the sanctioned slow-consumer path: a
 * replica that can't keep up is dropped-oldest and told it gapped — it must re-bootstrap).
 * Keys and values cross the wire through the store's own {@code SpillSerializer}s — the
 * framing the outer-ring ADR promised to reuse.</p>
 *
 * <p>Loopback-only, as tradition demands.</p>
 */
public final class ReplicationServer<K, V> implements Closeable {

    static final byte FRAME_PUT = 1;
    static final byte FRAME_DELETE = 2;
    static final byte FRAME_GAP = 3;

    private final SmokeHouse<K, V> store;
    private final SmokeHouseOptions<K, V> opts;
    private final ServerSocket server;
    private final Thread acceptor;
    private final List<AutoCloseable> perClient = new ArrayList<>();
    private volatile boolean closed;

    private ReplicationServer(SmokeHouse<K, V> store, SmokeHouseOptions<K, V> opts,
                              ServerSocket server) {
        this.store = store;
        this.opts = opts;
        this.server = server;
        this.acceptor = new Thread(this::acceptLoop, "smokehouse-repl-acceptor");
        this.acceptor.setDaemon(true);
    }

    /** Serve {@code store} to replicas on an ephemeral loopback port (see {@link #port()}). */
    public static <K, V> ReplicationServer<K, V> serve(SmokeHouse<K, V> store,
                                                       SmokeHouseOptions<K, V> opts)
            throws IOException {
        ServerSocket server = new ServerSocket(0, 8, InetAddress.getLoopbackAddress());
        ReplicationServer<K, V> rs = new ReplicationServer<>(store, opts, server);
        rs.acceptor.start();
        return rs;
    }

    /** The loopback port replicas connect to. */
    public int port() {
        return server.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket socket = server.accept();
                Thread t = new Thread(() -> bootstrap(socket), "smokehouse-repl-client");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;                                        // server closed
            }
        }
    }

    /** Subscribe → buffer → backup → ship → go live, then the tail thread owns the socket. */
    private void bootstrap(Socket socket) {
        try {
            DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), 1 << 16));
            FrameWriter writer = new FrameWriter(out);
            long seq0 = store.tailSequence();                  // frames stream from here on
            AutoCloseable sub = store.tail(seq0, writer);
            synchronized (perClient) {
                perClient.add(sub);
                perClient.add(socket);
            }
            Path tmp = Files.createTempDirectory("smokehouse-repl");
            try {
                store.backup(tmp);
                List<Path> files;
                try (var listing = Files.list(tmp)) {
                    files = listing.filter(Files::isRegularFile).sorted().toList();
                }
                out.writeInt(files.size());
                for (Path f : files) {
                    out.writeUTF(f.getFileName().toString());
                    out.writeLong(Files.size(f));
                    Files.copy(f, out);
                }
            } finally {
                try (var listing = Files.list(tmp)) {
                    for (Path f : listing.toList()) {
                        Files.deleteIfExists(f);
                    }
                }
                Files.deleteIfExists(tmp);
            }
            // The bootstrap baseline: everything at or before this sequence arrived inside
            // the shipped segments, so the replica is ALREADY caught up to here even if no
            // frame ever arrives (all-history-before-connect is the common case).
            out.writeLong(seq0 - 1);
            writer.flushAndGoLive();
        } catch (IOException e) {
            closeQuietly(socket);                              // replica gone mid-bootstrap
        }
    }

    /** The per-client tail listener: buffers during bootstrap, then writes frames directly. */
    private final class FrameWriter implements TailListener<K, V> {

        private final DataOutputStream out;
        private final List<TailEvent<K, V>> buffered = new ArrayList<>();
        private boolean live;
        private boolean dead;
        private boolean pendingGap;

        FrameWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public synchronized void onEvent(TailEvent<K, V> event) {
            if (dead) {
                return;
            }
            if (!live) {
                buffered.add(event);
                return;
            }
            try {
                frame(event);
                out.flush();
            } catch (IOException e) {
                dead = true;                                   // replica disconnected
            }
        }

        @Override
        public synchronized void onGap() {
            if (dead) {
                return;
            }
            dead = true;
            if (!live) {
                pendingGap = true;                             // bootstrap owns the wire; defer
                return;
            }
            try {
                out.writeByte(FRAME_GAP);
                out.flush();
            } catch (IOException ignored) {
                // the replica is gone either way
            }
        }

        synchronized void flushAndGoLive() throws IOException {
            if (pendingGap) {
                out.writeByte(FRAME_GAP);                      // gapped during bootstrap
                out.flush();
                return;
            }
            for (TailEvent<K, V> e : buffered) {
                frame(e);
            }
            buffered.clear();
            live = true;
            out.flush();
        }

        private void frame(TailEvent<K, V> e) throws IOException {
            out.writeByte(e.deleted() ? FRAME_DELETE : FRAME_PUT);
            out.writeLong(e.sequence());
            out.writeLong(store.tailSequence());               // the replica's lag numerator
            opts.keySerializer().write(e.key(), out);
            if (!e.deleted()) {
                opts.valueSerializer().write(e.value(), out);
            }
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ignored) {
            // shutdown path
        }
    }

    /** Stop accepting and drop every connected replica. The store itself is not closed. */
    @Override
    public void close() {
        closed = true;
        closeQuietly(server);
        synchronized (perClient) {
            for (AutoCloseable c : perClient) {
                closeQuietly(c);
            }
            perClient.clear();
        }
    }
}
