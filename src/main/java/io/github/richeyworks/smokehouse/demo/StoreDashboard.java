package io.github.richeyworks.smokehouse.demo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.richeyworks.smokehouse.Replica;
import io.github.richeyworks.smokehouse.ReplicationServer;
import io.github.richeyworks.smokehouse.SmokeHouse;
import io.github.richeyworks.smokehouse.SmokeHouseOptions;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

/**
 * The shop window (Phase 4.4): a live SmokeHouse on exhibit. A built-in workload churns the
 * store through flipping regimes while the browser watches over Server-Sent Events: keys /
 * puts / gets / deletes ticking, the pilot's verdicts, and — the point of the exhibit — the
 * <b>segment map</b>: one bar per log segment, its red fill the garbage ledger made visible.
 * Press <i>Compact now</i> and watch the map squash into one clean segment while the workload
 * keeps running (compaction's copy phase runs off the store lock by design).
 *
 * <p>Run: {@code ./gradlew run}, open <b>http://127.0.0.1:8079/</b>. Binds loopback only — an
 * exhibit, not a service (the same posture as SuperBeefSort's aquarium, whose SSE plumbing this
 * deliberately reuses). The store lives in a temp directory; all mutation happens on the single
 * driver thread; the HTTP threads only read ({@code stats()}, {@code segmentStats()}) or call
 * {@code compact()}, whose copy phase is off-lock by contract. Auto-compaction is disabled here
 * so the button owns the money shot.</p>
 */
public final class StoreDashboard {

    private static final int PORT = 8079;
    private static final int CLIENT_QUEUE_CAP = 1_000;
    private static final int VALUE_PAD = 120;              // fat-ish records: segments roll visibly

    private final CopyOnWriteArrayList<LinkedBlockingQueue<String>> clients = new CopyOnWriteArrayList<>();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicBoolean workloadOn = new AtomicBoolean(true);
    private final AtomicReference<String> requestedRegime = new AtomicReference<>();
    private volatile String regimeName = "warming up";

    private SmokeHouse<Long, String> store;

    // ── The replica panel (Phase 8's exhibit): spawn, watch lag, kill, re-bootstrap ─────────
    private ReplicationServer<Long, String> replicationServer;
    private final Object replicaLock = new Object();       // HTTP threads race the buttons
    private volatile Replica<Long, String> replica;        // null = no replica on exhibit
    private Path replicaDir;

    /** One workload regime: a key stream plus a read/delete mix, played for {@code ops} operations. */
    private record Regime(String name, int ops, double readFraction, double deleteFraction,
                          IntSupplier keys) { }

    private final Map<String, Regime> regimes = new LinkedHashMap<>();

    private void stockRegimes() {
        Random r1 = new Random(11);
        Random r2 = new Random(22);
        Random r3 = new Random(33);
        Random r4 = new Random(44);
        regimes.put("steady-churn", new Regime("steady-churn", 30_000, 0.50, 0.15,
                () -> r1.nextInt(50_000)));
        regimes.put("hot-key-overwrite", new Regime("hot-key-overwrite", 30_000, 0.10, 0.05,
                () -> r2.nextInt(300)));                   // the garbage machine
        regimes.put("read-heavy", new Regime("read-heavy", 40_000, 0.85, 0.05,
                () -> r3.nextInt(50_000)));
        regimes.put("delete-wave", new Regime("delete-wave", 15_000, 0.20, 0.60,
                () -> r4.nextInt(20_000)));
    }

    public static void main(String[] args) throws Exception {
        // Tier is selectable so the ensemble-backed tiers are showable live, not just testable:
        //   ./gradlew run --args="ENSEMBLE"     (or STATIC / ADAPTIVE / EVOLUTION;
        //   -Dsmokehouse.tier=... works too; default ADAPTIVE, the classic exhibit)
        String pick = args.length > 0 ? args[0] : System.getProperty("smokehouse.tier", "ADAPTIVE");
        new StoreDashboard(SmokeHouseOptions.IndexTier.valueOf(pick.trim().toUpperCase(java.util.Locale.ROOT)))
                .start();
    }

    private final SmokeHouseOptions.IndexTier tier;

    private StoreDashboard(SmokeHouseOptions.IndexTier tier) {
        this.tier = tier;
    }

    /** Fresh options per store — primary and replica each get their own instance. */
    private SmokeHouseOptions<Long, String> options() {
        return SmokeHouseOptions
                .of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(128 << 10)                   // small segments: a lively map
                .indexTier(tier)
                .pilotCadence(Duration.ofSeconds(2))
                .compactWhenGarbageAbove(0.0);             // manual: the button owns the money shot
    }

    private void start() throws IOException {
        Path dir = Files.createTempDirectory("smokehouse-dashboard");
        store = SmokeHouse.open(dir, options());
        replicationServer = ReplicationServer.serve(store, options());   // the replica's feed
        stockRegimes();

        HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", PORT), 0);
        http.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "dashboard-http");
            t.setDaemon(true);
            return t;
        }));
        http.createContext("/", this::servePage);
        http.createContext("/events", this::serveEvents);
        http.createContext("/compact", this::serveCompact);
        http.createContext("/toggle", this::serveToggle);
        http.createContext("/regime", this::serveRegime);
        http.createContext("/replica", this::serveReplica);
        http.start();

        var ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dashboard-ticker");
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(this::tick, 500, 1_000, TimeUnit.MILLISECONDS);

        System.out.println("SmokeHouse dashboard: http://127.0.0.1:" + PORT + "/   (tier=" + tier
                + ", store in " + dir + ")");
        drive();                                           // blocks forever on the single writer thread
    }

    // ── The driver: one thread owns all mutation ────────────────────────────────────────────

    private void drive() {
        Random mix = new Random(7);
        long stamp = 0;
        List<Regime> playlist = List.copyOf(regimes.values());
        int track = 0;
        String pad = "x".repeat(VALUE_PAD);
        while (true) {
            String pick = requestedRegime.getAndSet(null);
            Regime regime = (pick != null) ? regimes.get(pick) : playlist.get(track++ % playlist.size());
            regimeName = regime.name();
            publish("{\"t\":\"regime\",\"name\":\"" + esc(regime.name()) + "\"}");
            for (int i = 1; i <= regime.ops(); i++) {
                if (!workloadOn.get()) {
                    sleep(150);
                    if (requestedRegime.get() != null) {
                        break;                             // honor a pick even while paused
                    }
                    continue;
                }
                long key = regime.keys().getAsInt();
                try {
                    double roll = mix.nextDouble();
                    if (roll < regime.readFraction()) {
                        store.get(key);
                        gets.incrementAndGet();
                    } else if (roll < regime.readFraction() + regime.deleteFraction()) {
                        store.delete(key);
                        deletes.incrementAndGet();
                    } else {
                        store.put(key, "v" + (stamp++) + ":" + pad);
                        puts.incrementAndGet();
                    }
                } catch (IOException | UncheckedIOException storeTrouble) {
                    publish("{\"t\":\"error\",\"msg\":\"" + esc(storeTrouble.getMessage()) + "\"}");
                    sleep(500);
                }
                if (i % 500 == 0 && requestedRegime.get() != null) {
                    break;                                 // a picked regime cuts this track short
                }
            }
        }
    }

    // ── The ticker: read-only observation, safe from any thread ─────────────────────────────

    private void tick() {
        try {
            StringBuilder segs = new StringBuilder("[");
            boolean first = true;
            for (SmokeHouse.SegmentStat s : store.segmentStats()) {
                if (!first) {
                    segs.append(',');
                }
                segs.append("{\"id\":").append(s.segmentId())
                    .append(",\"bytes\":").append(s.bytes())
                    .append(",\"garbage\":").append(s.garbageBytes())
                    .append(",\"active\":").append(s.active()).append('}');
                first = false;
            }
            segs.append(']');
            // Probe depth of the median live key: CSRBT's searchDepth made visible — watch it
            // drop when the pilot morphs the index under a skewed regime. Read-path, off-thread
            // safe; ensemble tiers may report 0 (unmeasured stride) — the UI shows a dash.
            int depth = 0;
            Long median = store.medianKey();
            if (median != null) {
                int d = store.searchDepth(median);
                depth = (d >= 0) ? d : ~d;
            }
            // The replica's vitals, if one is on exhibit — lag under live churn is the show.
            Replica<Long, String> r = replica;
            String replicaJson = (r == null) ? "null"
                    : "{\"lag\":" + r.lagSequence()
                      + ",\"applied\":" + r.appliedSequence()
                      + ",\"gapped\":" + r.gapped()
                      + ",\"keys\":" + r.store().size() + "}";
            publish("{\"t\":\"stats\",\"keys\":" + store.size()
                    + ",\"garbage\":" + store.garbageBytes()
                    + ",\"puts\":" + puts.get() + ",\"gets\":" + gets.get()
                    + ",\"deletes\":" + deletes.get()
                    + ",\"running\":" + workloadOn.get()
                    + ",\"tier\":\"" + tier + "\""
                    + ",\"depth\":" + depth
                    + ",\"regime\":\"" + esc(regimeName) + "\""
                    + ",\"line\":\"" + esc(store.stats()) + "\""
                    + ",\"replica\":" + replicaJson
                    + ",\"segments\":" + segs + "}");
        } catch (IOException | RuntimeException ignored) {
            // an observation tick must never kill the ticker
        }
    }

    // ── HTTP handlers ────────────────────────────────────────────────────────────────────────

    /** POST/GET /compact: run a real compaction on this HTTP thread (copy phase is off-lock by design). */
    private void serveCompact(HttpExchange ex) throws IOException {
        long reclaimed;
        try {
            reclaimed = store.compact();
        } catch (IOException failed) {
            respond(ex, 500, "compaction failed: " + failed.getMessage());
            return;
        }
        publish("{\"t\":\"compacted\",\"reclaimed\":" + reclaimed + "}");
        respond(ex, 200, String.valueOf(reclaimed));
    }

    /** GET /toggle: pause/resume the built-in workload. */
    private void serveToggle(HttpExchange ex) throws IOException {
        boolean now = !workloadOn.get();
        workloadOn.set(now);
        respond(ex, 200, now ? "running" : "paused");
    }

    /** GET /regime?name=<n>: queue a regime; the driver picks it up within ~500 ops. */
    private void serveRegime(HttpExchange ex) throws IOException {
        String query = ex.getRequestURI().getQuery();
        String name = (query != null && query.startsWith("name="))
                ? URLDecoder.decode(query.substring(5), StandardCharsets.UTF_8) : "";
        if (!regimes.containsKey(name)) {
            respond(ex, 404, "unknown regime; known: " + regimes.keySet());
            return;
        }
        requestedRegime.set(name);
        workloadOn.set(true);
        respond(ex, 204, null);
    }

    /**
     * GET /replica?do=spawn|kill|reboot — Phase 8 on exhibit: spawn an in-process replica
     * (bootstraps from a shipped backup, then follows the tail live), kill it mid-stream,
     * or re-bootstrap it cold. Replica ops run on HTTP threads, serialized by their own
     * lock; the primary's single-writer contract is untouched.
     */
    private void serveReplica(HttpExchange ex) throws IOException {
        String q = ex.getRequestURI().getQuery();
        String action = q == null ? ""
                : URLDecoder.decode(q, StandardCharsets.UTF_8).replace("do=", "");
        try {
            synchronized (replicaLock) {
                switch (action) {
                    case "spawn" -> {
                        if (replica == null) {
                            replicaDir = Files.createTempDirectory("smokehouse-replica");
                            replica = Replica.connect(replicaDir, options(),
                                    replicationServer.port());
                            publish("{\"t\":\"replica\",\"msg\":"
                                    + "\"spawned — bootstrapping from a shipped backup\"}");
                        }
                    }
                    case "kill" -> {
                        if (replica != null) {
                            replica.close();
                            replica = null;
                            publish("{\"t\":\"replica\",\"msg\":"
                                    + "\"killed — its directory remains a valid store\"}");
                        }
                    }
                    case "reboot" -> {
                        if (replica != null) {
                            replica.close();
                            wipe(replicaDir);
                            replica = Replica.connect(replicaDir, options(),
                                    replicationServer.port());
                            publish("{\"t\":\"replica\",\"msg\":"
                                    + "\"re-bootstrapped — a cold start is always acceptable\"}");
                        }
                    }
                    default -> {
                        // unknown action: fall through to ok — the exhibit shrugs
                    }
                }
            }
            respond(ex, 200, "ok");
        } catch (IOException e) {
            respond(ex, 500, "replica op failed: " + e.getMessage());
        }
    }

    private static void wipe(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var walk = Files.walk(dir)) {
            for (Path p : walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
                if (!p.equals(dir)) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        if (body == null) {
            ex.sendResponseHeaders(code, -1);
            ex.close();
            return;
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    // ── SSE plumbing (the aquarium's, deliberately) ──────────────────────────────────────────

    private void publish(String json) {
        for (LinkedBlockingQueue<String> q : clients) {
            if (!q.offer(json)) {                          // slow client: drop oldest, stay live
                q.poll();
                q.offer(json);
            }
        }
    }

    private void serveEvents(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/event-stream; charset=utf-8");
        ex.getResponseHeaders().add("Cache-Control", "no-cache");
        ex.sendResponseHeaders(200, 0);
        LinkedBlockingQueue<String> q = new LinkedBlockingQueue<>(CLIENT_QUEUE_CAP);
        clients.add(q);
        try (OutputStream out = ex.getResponseBody()) {
            out.write("retry: 2000\n\n".getBytes(StandardCharsets.UTF_8));
            StringBuilder menu = new StringBuilder("{\"t\":\"menu\",\"regimes\":[");
            boolean first = true;
            for (String name : regimes.keySet()) {
                if (!first) {
                    menu.append(',');
                }
                menu.append('"').append(name).append('"');
                first = false;
            }
            menu.append("]}");
            out.write(("data: " + menu + "\n\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            while (true) {
                String msg = q.take();
                out.write(("data: " + msg + "\n\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException | InterruptedException gone) {
            if (gone instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            clients.remove(q);
        }
    }

    private void servePage(HttpExchange ex) throws IOException {
        byte[] page;
        try (var in = StoreDashboard.class.getResourceAsStream("/dashboard.html")) {
            page = (in == null)
                    ? "<h1>dashboard.html missing from resources</h1>".getBytes(StandardCharsets.UTF_8)
                    : in.readAllBytes();
        }
        ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
        ex.sendResponseHeaders(200, page.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(page);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
