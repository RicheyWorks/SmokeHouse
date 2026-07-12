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
 * Warm-start recovery cost, single-shot: {@code open} a populated directory and rebuild the index.
 * The directory is written once in trial setup and closed cleanly, so a hint checkpoint is present
 * and this measures the warm path (hint-accelerated), not a cold full scan. The paired
 * {@code open}/{@code close} keeps the store from leaking between invocations; the dominant cost is
 * the open-side index rebuild.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class RecoveryBenchmark {

    @Param({"UNIFORM", "SEQUENTIAL", "SKEWED"})
    public Workloads.Shape shape;

    @Param({"100000"})
    public int n;

    private Path dir;

    @Setup(Level.Trial)
    public void populate() throws IOException {
        dir = Workloads.freshDir("recover");
        try (SmokeHouse<Long, String> store =
                     SmokeHouse.open(dir, Workloads.options(SmokeHouseOptions.IndexTier.STATIC))) {
            long stamp = 0;
            for (long key : shape.keys(n, n, 3L)) {
                store.put(key, Workloads.value(stamp++));
            }
        }   // clean close writes the hint → the reopen below is the warm path
    }

    @TearDown(Level.Trial)
    public void clean() {
        Workloads.deleteRecursively(dir);
    }

    @Benchmark
    public int recover() throws IOException {
        try (SmokeHouse<Long, String> store =
                     SmokeHouse.open(dir, Workloads.options(SmokeHouseOptions.IndexTier.STATIC))) {
            return store.size();     // touch the rebuilt index so the open can't be optimized away
        }
    }
}
