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

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Full-compaction cost, single-shot: reclaim a garbage-laden store. Invocation setup churns the
 * store ({@code CHURN_FACTOR} × n overwrites over an n-wide key space, so most records are dead)
 * and leaves it open; the timed region is exactly one {@link SmokeHouse#compact()} — the off-lock
 * copy of the live set plus the under-lock commit and index repoint. Auto-compaction is disabled
 * (see {@link Workloads#options}) so the garbage is all still there when the benchmark fires.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class CompactionBenchmark {

    @Param({"UNIFORM", "SEQUENTIAL", "SKEWED"})
    public Workloads.Shape shape;

    @Param({"100000"})
    public int n;

    /** Overwrites per live key: higher → more dead bytes for compaction to reclaim. */
    private static final int CHURN_FACTOR = 4;

    private Path dir;
    private SmokeHouse<Long, String> store;

    @Setup(Level.Invocation)
    public void churn() throws IOException {
        dir = Workloads.freshDir("compact");
        store = SmokeHouse.open(dir, Workloads.options(SmokeHouseOptions.IndexTier.STATIC));
        long stamp = 0;
        for (long key : shape.keys(n * CHURN_FACTOR, n, 4L)) {
            store.put(key, Workloads.value(stamp++));
        }
    }

    @TearDown(Level.Invocation)
    public void clean() throws IOException {
        store.close();
        Workloads.deleteRecursively(dir);
    }

    @Benchmark
    public long compact() throws IOException {
        return store.compact();      // returns net bytes reclaimed
    }
}
