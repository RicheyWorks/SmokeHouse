package benchmarks;

import io.github.richeyworks.csrbt.OrderedSet;
import io.github.richeyworks.csrbt.strategy.AVLStrategy;
import io.github.richeyworks.csrbt.strategy.RedBlackStrategy;
import io.github.richeyworks.csrbt.strategy.TreeStrategy;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * D1 isolation (outer-ring ADR §D1 — the CSRBT {@code replace} seam). A SmokeHouse upsert of an
 * existing key is, in the index, a {@code remove(oldEntry)} + {@code add(newEntry)} at the same key
 * position: two O(log n) descents (the comparator ignores the location fields, so both entries
 * compare-equal and land in the same slot). The proposed seam collapses that to one descent plus an
 * in-place payload swap — no second descent, no rebalance.
 *
 * <p>This measures {@code remove}+{@code add} against a single-descent {@code contains} (the
 * optimistic floor of what {@code replace} would cost), directly on a warm CSRBT set with no log or
 * fsync in the way. Read the result against {@code StoreOpsBenchmark.upsert} (~5 us end-to-end): if
 * {@code removeAdd} is under ~15% of that, the ADR's gate says the seam is not worth cutting and
 * remove+add stands. The seam's actual saving is closer to {@code removeAdd - locate} (the second
 * descent it removes), so that gap is the upper bound on what cutting it could buy.</p>
 *
 * <p>Caveat: elements here are boxed {@code Integer} under natural order, not {@code IndexEntry<Long>}
 * under the store's comparator — the comparison constant differs slightly, but tree depth and the
 * rebalance work (the dominant cost) match at equal n.</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class IndexUpsertBenchmark {

    @Param({"100000"})
    public int n;

    @Param({"RED_BLACK", "AVL"})
    public String strategy;

    private OrderedSet<Integer> set;
    private int[] keys;          // all present in the set: the round-robin upsert / lookup targets
    private int cursor;

    private TreeStrategy<Integer> newStrategy() {
        switch (strategy) {
            case "RED_BLACK": return new RedBlackStrategy<>();
            case "AVL":       return new AVLStrategy<>();
            default:          throw new AssertionError(strategy);
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        Random rnd = new Random(42);                        // deterministic, house style
        keys = rnd.ints().distinct().limit(n).toArray();    // n distinct keys
        set = OrderedSet.withNaturalOrder(newStrategy());
        for (int k : keys) {
            set.add(k);
        }
    }

    /** Round-robin over the present keys — deterministic, allocation-free. */
    private int nextKey() {
        int k = keys[cursor];
        cursor = (cursor + 1 == keys.length) ? 0 : cursor + 1;
        return k;
    }

    /** Current upsert index cost: unlink the incumbent, re-link at the same key. Two descents. */
    @Benchmark
    public void removeAdd() {
        int k = nextKey();
        set.remove(k);
        set.add(k);
    }

    /** Replace's optimistic floor: one descent to locate the key (the payload swap would be O(1)). */
    @Benchmark
    public void locate(Blackhole bh) {
        bh.consume(set.contains(nextKey()));
    }
}
