package io.github.richeyworks.smokehouse;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 8 against the required oracle: a replica must converge to byte-for-byte read
 * equality with the primary — full range scans, sizes, and order statistics — after
 * bootstrap, after live churn (puts, overwrites, deletes), and across multiple replicas.
 * Seeded and deterministic; the only timing concession is {@code awaitCaughtUp}, because
 * frames ride the tail thread and the wire.
 */
class ReplicationTest {

    private static final long AWAIT = 10_000;

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(2048)                            // several segments → real shipping
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    private static TreeMap<Long, String> scan(SmokeHouse<Long, String> store) throws IOException {
        TreeMap<Long, String> out = new TreeMap<>();
        if (store.size() > 0) {
            store.range(store.firstKey(), store.lastKey(), out::put);
        }
        return out;
    }

    private static void churn(SmokeHouse<Long, String> primary, TreeMap<Long, String> oracle,
                              Random rnd, int ops) throws IOException {
        for (int i = 0; i < ops; i++) {
            long key = rnd.nextInt(150);
            if (rnd.nextInt(6) == 0) {
                primary.delete(key);
                oracle.remove(key);
            } else {
                String v = "v" + key + ":" + i + ":" + rnd.nextInt(1_000);
                primary.put(key, v);
                oracle.put(key, v);
            }
        }
    }

    @Test
    void replicaBootstrapsThenFollowsTheChurn(@TempDir Path primaryDir, @TempDir Path replicaDir)
            throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> primary = SmokeHouse.open(primaryDir, opts())) {
            churn(primary, oracle, rnd, 400);                  // history before any replica
            try (ReplicationServer<Long, String> server =
                         ReplicationServer.serve(primary, opts());
                 Replica<Long, String> replica =
                         Replica.connect(replicaDir, opts(), server.port())) {

                assertTrue(replica.awaitCaughtUp(primary.tailSequence(), AWAIT),
                        "bootstrap must converge");
                assertEquals(oracle, scan(replica.store()), "post-bootstrap state");
                assertEquals(oracle, scan(primary), "oracle sanity");

                churn(primary, oracle, rnd, 400);              // live churn on top
                assertTrue(replica.awaitCaughtUp(primary.tailSequence(), AWAIT),
                        "live follow must converge");
                assertEquals(oracle, scan(replica.store()), "post-churn state");
                assertEquals(0, replica.lagSequence(), "caught up means zero lag");
            }
        }
    }

    @Test
    void aReplicaIsAFullSmokeHouse(@TempDir Path primaryDir, @TempDir Path replicaDir)
            throws IOException {
        Random rnd = new Random(7);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> primary = SmokeHouse.open(primaryDir, opts())) {
            churn(primary, oracle, rnd, 500);
            try (ReplicationServer<Long, String> server =
                         ReplicationServer.serve(primary, opts());
                 Replica<Long, String> replica =
                         Replica.connect(replicaDir, opts(), server.port())) {
                assertTrue(replica.awaitCaughtUp(primary.tailSequence(), AWAIT));

                SmokeHouse<Long, String> r = replica.store();
                assertEquals(primary.size(), r.size());
                assertEquals(primary.firstKey(), r.firstKey());
                assertEquals(primary.lastKey(), r.lastKey());
                assertEquals(primary.medianKey(), r.medianKey(), "order statistics work");
                assertEquals(primary.countRange(20L, 90L), r.countRange(20L, 90L));
                assertEquals(primary.nthKey(5), r.nthKey(5));
            }
        }
    }

    @Test
    void twoReplicasConvergeIndependently(@TempDir Path primaryDir, @TempDir Path dirA,
                                          @TempDir Path dirB) throws IOException {
        Random rnd = new Random(11);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> primary = SmokeHouse.open(primaryDir, opts());
             ReplicationServer<Long, String> server = ReplicationServer.serve(primary, opts())) {
            churn(primary, oracle, rnd, 200);
            try (Replica<Long, String> a = Replica.connect(dirA, opts(), server.port())) {
                churn(primary, oracle, rnd, 200);              // B bootstraps mid-churn
                try (Replica<Long, String> b = Replica.connect(dirB, opts(), server.port())) {
                    churn(primary, oracle, rnd, 200);
                    long target = primary.tailSequence();
                    assertTrue(a.awaitCaughtUp(target, AWAIT), "replica A converges");
                    assertTrue(b.awaitCaughtUp(target, AWAIT), "replica B converges");
                    assertEquals(oracle, scan(a.store()));
                    assertEquals(oracle, scan(b.store()));
                }
            }
        }
    }

    @Test
    void aClosedReplicaDirectoryReopensAsAPlainStore(@TempDir Path primaryDir,
                                                     @TempDir Path replicaDir)
            throws IOException {
        Random rnd = new Random(3);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> primary = SmokeHouse.open(primaryDir, opts())) {
            churn(primary, oracle, rnd, 300);
            try (ReplicationServer<Long, String> server =
                         ReplicationServer.serve(primary, opts());
                 Replica<Long, String> replica =
                         Replica.connect(replicaDir, opts(), server.port())) {
                assertTrue(replica.awaitCaughtUp(primary.tailSequence(), AWAIT));
            }
        }
        // The log is the only truth — the replica's dir must recover as a normal store.
        try (SmokeHouse<Long, String> reopened = SmokeHouse.open(replicaDir, opts())) {
            assertEquals(oracle, scan(reopened), "manual promotion is just open()");
        }
    }

    @Test
    void aReplicaGapsOnANonContiguousFrameInsteadOfApplyingAHole(@TempDir Path dir) throws Exception {
        // Twelfth pass: the replica applies only CONTIGUOUS frames. A fake primary ships an empty
        // backup (baseSequence 0, so the first expected frame is sequence 1), one good frame
        // (seq 1), then a frame that SKIPS seq 2 (seq 3) — as if the tail dropped an event but its
        // FRAME_GAP arrived a frame late. Applying seq 3 would leave a state that never existed on
        // the primary; the replica must gap and keep the clean prefix it has.
        SmokeHouseOptions<Long, String> opts = SmokeHouseOptions.of(
                SpillSerializer.forLongs(), SpillSerializer.forStrings());
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Thread primary = new Thread(() -> {
                try (Socket s = server.accept();
                     DataOutputStream out = new DataOutputStream(
                             new BufferedOutputStream(s.getOutputStream()))) {
                    out.writeInt(0);                              // empty backup: zero shipped files
                    out.writeLong(0L);                            // baseSequence → expect frame seq 1
                    writeFrame(out, 1L, 2L, 10L, "ten");          // contiguous: applied
                    writeFrame(out, 3L, 4L, 30L, "thirty");       // SKIPS seq 2: must gap, not apply
                    out.flush();
                    Thread.sleep(3_000);                          // hold the socket open while it reads
                } catch (Exception ignored) {
                    // the replica closing the socket ends the thread
                }
            }, "fake-primary");
            primary.setDaemon(true);
            primary.start();

            try (Replica<Long, String> replica = Replica.connect(dir, opts, server.getLocalPort())) {
                for (int i = 0; i < 300 && !replica.gapped(); i++) {
                    Thread.sleep(10);
                }
                assertTrue(replica.gapped(), "a non-contiguous frame must gap the replica");
                assertEquals("ten", replica.store().get(10L), "the contiguous frame applied");
                assertNull(replica.store().get(30L), "the frame past the hole must NOT apply");
            }
        }
    }

    private static void writeFrame(DataOutputStream out, long sequence, long primarySequence,
                                   long key, String value) throws IOException {
        out.writeByte(ReplicationServer.FRAME_PUT);
        out.writeLong(sequence);
        out.writeLong(primarySequence);
        SpillSerializer.forLongs().write(key, out);
        SpillSerializer.forStrings().write(value, out);
    }
}
