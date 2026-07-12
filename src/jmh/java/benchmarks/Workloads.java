package benchmarks;

import io.github.richeyworks.smokehouse.SmokeHouseOptions;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Shared scaffolding for the SmokeHouse JMH rig (Phase 5): workload shapes, seeded key generation,
 * store options tuned so each benchmark measures one thing, and temp-directory lifecycle helpers.
 *
 * <p>Public so JMH's generated harness (a peer package) can reach {@link Shape} through the
 * {@code @Param} fields; nothing here is API.</p>
 */
public final class Workloads {

    private Workloads() {
    }

    /**
     * Key distributions over a bounded space — the profiler's distribution classes recast as
     * workload knobs. UNIFORM spreads across the space, SEQUENTIAL walks it in order (dense,
     * append-friendly), SKEWED biases hard toward low keys (a hot set, the garbage machine).
     */
    public enum Shape {
        UNIFORM, SEQUENTIAL, SKEWED;

        /** {@code n} keys in {@code [0, span)}, deterministic for {@code seed}. */
        public long[] keys(int n, int span, long seed) {
            Random rnd = new Random(seed);
            long[] out = new long[n];
            for (int i = 0; i < n; i++) {
                switch (this) {
                    case UNIFORM:
                        out[i] = rnd.nextInt(span);
                        break;
                    case SEQUENTIAL:
                        out[i] = i % span;
                        break;
                    case SKEWED:
                        double r = rnd.nextDouble();
                        out[i] = (long) (span * r * r);   // squared → concentrated near 0
                        break;
                    default:
                        throw new AssertionError(this);
                }
            }
            return out;
        }
    }

    /** Fat-ish values so segments roll at a realistic cadence and compaction has bytes to reclaim. */
    private static final String PAD = "x".repeat(96);

    public static String value(long stamp) {
        return "v" + stamp + ":" + PAD;
    }

    /**
     * Options that isolate the operation under test: auto-compaction off and the pilot effectively
     * idle, so no background maintenance bleeds into a measured put/get/compact.
     */
    public static SmokeHouseOptions<Long, String> options(SmokeHouseOptions.IndexTier tier) {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .segmentBytes(1 << 20)
                .indexTier(tier)
                .compactWhenGarbageAbove(0.0)                 // manual only: no background compaction
                .pilotCadence(Duration.ofHours(1));           // the scheduler never fires mid-benchmark
    }

    public static Path freshDir(String label) {
        try {
            return Files.createTempDirectory("smokehouse-bench-" + label + "-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void deleteRecursively(Path dir) {
        if (dir == null) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best effort: a temp dir the OS will reap anyway
                }
            });
        } catch (IOException ignored) {
            // dir already gone
        }
    }
}
