package benchmarks;

import io.github.richeyworks.smokehouse.SmokeHouse;
import io.github.richeyworks.smokehouse.SmokeHouseOptions;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Steady-state per-operation costs over a warm ADAPTIVE store: a point read, an upsert, and a
 * bounded range scan. Seeded and shape-parameterized (house style, {@code Random} seeds fixed).
 *
 * <p>The {@code upsert} number is D1's headline: today an upsert is a CSRBT {@code remove} + {@code
 * add} (two O(log n) descents). The ADR only cuts the {@code replace} seam if this proves to be
 * more than ~15% of end-to-end upsert cost — the log append and interval fsync are expected to
 * dominate, which is exactly what this measures.</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class StoreOpsBenchmark {

    @Param({"UNIFORM", "SEQUENTIAL", "SKEWED"})
    public Workloads.Shape shape;

    @Param({"100000"})
    public int n;

    /** Half-open width of the range scan (keys). */
    private static final long RANGE_WINDOW = 128;

    private Path dir;
    private SmokeHouse<Long, String> store;
    private long[] present;          // keys guaranteed to be in the store (read hits)
    private int cursor;
    private long stamp;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Workloads.freshDir("ops");
        store = SmokeHouse.open(dir, Workloads.options(SmokeHouseOptions.IndexTier.ADAPTIVE));
        present = shape.keys(n, n, 1L);                 // span == n
        for (long key : present) {
            store.put(key, Workloads.value(stamp++));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        store.close();
        Workloads.deleteRecursively(dir);
    }

    /** Round-robin over the known-present keys — deterministic, allocation-free. */
    private long nextKey() {
        long k = present[cursor];
        cursor = (cursor + 1 == present.length) ? 0 : cursor + 1;
        return k;
    }

    /** Point read of a present key (steady-state hit): index resolve under the lock, log read outside. */
    @Benchmark
    public void get(Blackhole bh) throws IOException {
        bh.consume(store.get(nextKey()));
    }

    /** Upsert: overwrite an existing key. One log append + the index remove+add that D1 eyes. */
    @Benchmark
    public void upsert() throws IOException {
        store.put(nextKey(), Workloads.value(stamp++));
    }

    /** Bounded range scan from a present key over the index (walk under lock, values stream after). */
    @Benchmark
    public void rangeScan(Blackhole bh) throws IOException {
        long lo = nextKey();
        store.range(lo, lo + RANGE_WINDOW, (k, v) -> bh.consume(v));
    }
}
