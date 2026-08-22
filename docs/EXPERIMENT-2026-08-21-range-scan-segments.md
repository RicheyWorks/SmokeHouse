# 2026-08-21 — range-scan read amplification vs. segment count

A pre-registered experiment (ADR-022 methodology: seeded, warmup discarded, median of 3), run by
`RangeScanSegmentCountExperimentTest`. The test asserts only structural truths (every configuration
scanned all n keys; the small-segment run really did fragment the log) so it cannot flake; the
numbers below are the verdict from the canonical run.

## Question

A full-keyspace `range` scan reads every live record positionally from whichever segment holds it,
over per-segment read channels the log caches. Compaction consolidates many closed segments into
one. Is that consolidation a **read-speed** win — does the same data cost more per key to scan when
it is spread across many segments — or only a **space** win (garbage reclaim), with scan cost flat
regardless of how the log is fragmented?

## Method

The same 100,000 records, built into 1, ~10, and ~100 segments by varying `segmentBytes` alone
(STATIC index tier, so no pilot runs and no auto-compaction touches the data — the measurement is of
the read path only). Per-key `range` cost timed over a full-keyspace scan, median of 3, one warmup
discarded.

## Result (per-key nanoseconds, median of 3, 1 warmup discarded)

```
segmentBytes   segments   per-key ns
67108864       1          5097
524288         10         3872
49152          98         3826
many/single per-key ratio = 0.75x  (98 segments vs 1)
```

## Verdict — decision rule resolves: consolidation is a SPACE win, not a read-speed win

The pre-registered rule: consolidation speeds reads if the per-key scan cost at ~100 segments
exceeds **2×** the single-segment cost; otherwise scan cost is segment-count-insensitive and
compaction's read value is negligible.

The ratio is **0.75×** — the 98-segment scan is, if anything, marginally *faster* per key than the
single-segment scan (the 1-segment figure carries the first-configuration JVM warm-up, the same
artifact seen in the indexed-put experiment; the ~10- and ~100-segment runs, measured warm and back
to back, are indistinguishable at 3.9 vs 3.8 µs/key). Fragmenting the log across ~100 segments does
not slow a full scan.

The reason is structural: the log caches one read `FileChannel` per segment in its `readers` map, so
scanning N segments opens N channels once and then every record is a single positional read exactly
as it is in one segment. There is no per-segment scan overhead beyond the one-time channel open, and
that is amortized to nothing over 100k records.

**So compaction earns its keep on space, not on read latency.** Consolidating closed segments
reclaims garbage (its documented purpose) but does not measurably speed range scans — a store left
fragmented across many segments scans just as fast as a freshly compacted one. Practically:
auto-compaction can be tuned purely against the garbage ratio (space) without fear that a high
segment count is silently taxing reads. It also means a deferral of "compact to speed up scans" is
now backed by a number — that motivation does not exist on this read path.

## Honest bounds

Wall-clock magnitudes are disk- and machine-specific (positional reads here are served largely from
the page cache at these sizes); the portable result is the **shape** — flat per-key cost across a
100× spread in segment count. The single-segment baseline carries JVM warm-up jitter as the first
configuration measured; the decision rule's comparison survives it by a wide margin (0.75× vs the 2×
threshold), and the two warm configurations (~10 and ~100 segments) agree to within 2%.
