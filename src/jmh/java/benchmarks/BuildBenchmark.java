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
 * Cold build cost, single-shot: {@code open} a fresh store and write {@code n} records. This is the
 * bulk-write throughput baseline — every put is a log append plus one index insert, with interval
 * fsync in the mix. Key generation is hoisted to trial setup; a fresh directory per invocation
 * keeps each measurement independent and is not timed.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class BuildBenchmark {

    @Param({"UNIFORM", "SEQUENTIAL", "SKEWED"})
    public Workloads.Shape shape;

    @Param({"100000"})
    public int n;

    private long[] keys;
    private Path dir;

    @Setup(Level.Trial)
    public void keys() {
        keys = shape.keys(n, n, 2L);
    }

    @Setup(Level.Invocation)
    public void freshDir() {
        dir = Workloads.freshDir("build");
    }

    @TearDown(Level.Invocation)
    public void clean() {
        Workloads.deleteRecursively(dir);
    }

    @Benchmark
    public void build() throws IOException {
        try (SmokeHouse<Long, String> store =
                     SmokeHouse.open(dir, Workloads.options(SmokeHouseOptions.IndexTier.STATIC))) {
            long stamp = 0;
            for (long key : keys) {
                store.put(key, Workloads.value(stamp++));
            }
        }
    }
}
